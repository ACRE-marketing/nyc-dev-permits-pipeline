#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NYC 开发&许可每日抓取器（含 DOB + Google Sheet 追加历史）
依赖：requests beautifulsoup4 feedparser python-dateutil pandas gspread google-auth
环境变量：
  - LOOKBACK_HOURS （默认24）
  - NYC_SODA_APP_TOKEN （可选，提高 Open Data 配额）
  - GOOGLE_SERVICE_ACCOUNT_JSON （必需，Service Account JSON 内容）
  - GSHEET_ID （必需，目标 Google Sheet 的 ID）
  - GSHEET_TAB （可选，工作表名，默认 'Daily'）
"""
from __future__ import annotations
import os, re, time, json, html, logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import requests
from bs4 import BeautifulSoup
import feedparser
from dateutil import tz
import pandas as pd

# === Google Sheets ===
import gspread
from google.oauth2.service_account import Credentials

NY_TZ = tz.gettz("America/New_York")
UTC = tz.gettz("UTC")

LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "24"))
SINCE_DT = datetime.now(NY_TZ) - timedelta(hours=LOOKBACK_HOURS)

HEADERS = {
    "User-Agent": "AcreNY-DevBot/1.0 (+https://acre.example) PythonRequests",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

def parse_iso(dt_str: str):
    if not dt_str:
        return None
    for fmt in ("%a, %d %b %Y %H:%M:%S %z","%Y-%m-%dT%H:%M:%S%z","%Y-%m-%dT%H:%M:%S.%f%z","%Y-%m-%d %H:%M:%S%z","%Y-%m-%d"):
        try:
            return datetime.strptime(dt_str, fmt)
        except Exception:
            pass
    try:
        return datetime.fromisoformat(dt_str.replace("Z","+00:00"))
    except Exception:
        return None

ORG_SUFFIX = r"(?:LLC|LLP|LP|Inc\.|Incorporated|Ltd\.|Ltd|Corp\.|Corporation|Company|Group|Partners|Properties|Holdings|Realty|Development|Builders|Construction|Management)"
DEV_PATTERNS = [
    re.compile(r"(?i)(?:is|are) listed as the (?:owner|developer|applicant|sponsor)[^,.]*?\b([A-Z][\w&'\.\- ]+(?:\s+"+ORG_SUFFIX+r")?)"),
    re.compile(r"(?i)(?:the\s+)?developer(?:s)?\s+(?:is|are)\s+\b([A-Z][\w&'\.\- ]+(?:\s+"+ORG_SUFFIX+r")?)"),
    re.compile(r"(?i)developed\s+by\s+\b([A-Z][\w&'\.\- ]+(?:\s+"+ORG_SUFFIX+r")?)"),
    re.compile(r"(?i)owner\s+(?:is|are)\s+\b([A-Z][\w&'\.\- ]+(?:\s+"+ORG_SUFFIX+r")?)"),
]
ORG_FALLBACK = re.compile(r"\b([A-Z][\w&'\.\- ]+\s(?:"+ORG_SUFFIX+r"))(?!\w)")

BOROUGH_WORDS = {'manhattan':'Manhattan','brooklyn':'Brooklyn','queens':'Queens','bronx':'Bronx','staten island':'Staten Island'}

def extract_developers_from_text(text: str):
    names = []
    for pat in DEV_PATTERNS:
        for m in pat.finditer(text):
            name = m.group(1).strip().rstrip(',.;:')
            if name and name not in names:
                names.append(name)
    if not names:
        for m in ORG_FALLBACK.finditer(text):
            name = m.group(1).strip().rstrip(',.;:')
            if name and name not in names:
                names.append(name)
    return names[:3]

def guess_borough(text: str):
    t = text.lower()
    for k,v in BOROUGH_WORDS.items():
        if k in t: return v
    return ""

def fetch_yimby_recent():
    out = []
    feeds = ["https://newyorkyimby.com/feed"]
    for feed in feeds:
        d = feedparser.parse(feed)
        for e in d.entries:
            published = None
            for k in ("published","updated","created"):
                if hasattr(e,k):
                    published = parse_iso(getattr(e,k)) or published
            if not published and getattr(e,"published_parsed",None):
                published = datetime.fromtimestamp(time.mktime(e.published_parsed))
            if not published:
                published = datetime.utcnow()
            # interpret as NY time
            published = published if published.tzinfo else published.replace(tzinfo=UTC)
            if published.astimezone(NY_TZ) < SINCE_DT: 
                continue
            url = e.link
            try:
                resp = requests.get(url, headers=HEADERS, timeout=25)
                soup = BeautifulSoup(resp.text, "html.parser")
                art = soup.select_one("article") or soup
                text = " ".join([p.get_text(" ", strip=True) for p in art.select("p")])
                devs = extract_developers_from_text(text)
                title = html.unescape(e.title)
                borough = guess_borough(title+" "+text)
                address = title.split(" in ")[0].replace("Permits Filed for","").strip()
                out.append({
                    "date": published.astimezone(NY_TZ).strftime("%Y-%m-%d"),
                    "source": "YIMBY",
                    "title": title, "address": address, "borough": borough,
                    "developers": "; ".join(devs), "url": url
                })
            except Exception as ex:
                logging.warning(f"YIMBY parse failed: {url} -> {ex}")
    return out

def fetch_trd_recent(max_links=40):
    out = []; seen = set()
    list_pages = ["https://therealdeal.com/new-york/","https://therealdeal.com/tag/new-development/"]
    for lp in list_pages:
        try:
            r = requests.get(lp, headers=HEADERS, timeout=25)
            soup = BeautifulSoup(r.text, "html.parser")
            for a in soup.select("a[href]"):
                href = a["href"]
                if not href.startswith("https://therealdeal.com/"): continue
                if any(x in href for x in ("/tag/","/category/","/author/","/video","/shop","/events")): continue
                if href in seen: continue
                seen.add(href)
                if len(seen) >= max_links: break
        except Exception as ex:
            logging.warning(f"TRD list fetch failed: {lp} -> {ex}")
    for url in list(seen)[:max_links]:
        try:
            r = requests.get(url, headers=HEADERS, timeout=25)
            soup = BeautifulSoup(r.text, "html.parser")
            dt_el = soup.select_one("time[datetime]")
            dt = parse_iso(dt_el["datetime"]) if dt_el and dt_el.has_attr("datetime") else None
            if not dt:
                dt = datetime.utcnow()
            dt = dt if dt.tzinfo else dt.replace(tzinfo=UTC)
            if dt.astimezone(NY_TZ) < SINCE_DT: 
                continue
            art = soup.select_one("article") or soup
            title_el = art.select_one("h1")
            title = title_el.get_text(strip=True) if title_el else url
            text = " ".join([p.get_text(" ", strip=True) for p in art.select("p")])
            devs = extract_developers_from_text(text)
            borough = guess_borough(title+" "+text)
            m = re.search(r"(\d{1,5} [A-Za-z0-9'\- ]+ (?:Street|St\.|Avenue|Ave\.|Boulevard|Blvd\.|Road|Rd\.|Place|Pl\.|Court|Ct\.|Drive|Dr\.|Lane|Ln\.)(?:,\s?(?:Brooklyn|Manhattan|Queens|Bronx|Staten Island))?)", title+" "+text)
            address = m.group(1) if m else ""
            out.append({
                "date": dt.astimezone(NY_TZ).strftime("%Y-%m-%d"), "source": "The Real Deal",
                "title": title, "address": address, "borough": borough,
                "developers": "; ".join(devs), "url": url
            })
        except Exception as ex:
            logging.warning(f"TRD parse failed: {url} -> {ex}")
    return out

SOC_DATASETS = {
    "ipu4-2q9a": {
        "name": "DOB Permit Issuance",
        "endpoint": "https://data.cityofnewyork.us/resource/ipu4-2q9a.json",
        "date_fields": [":updated_at","issuance_date","issue_date","job_start_date","filing_date"],
        "owner_fields": ["owner_business_name","owner_business","owner_name","owners_business_name",
                         "permittee_business_name","permittee","applicant_business_name","business_name"],
        "address_fields": ["house__","house","street_name","streetname","job_location_street_name","address","location"],
        "borough_fields": ["borough","borocode","bbl_borough","city"],
        "title_fields": ["job_description","work_description","job_type"],
    },
    "w9ak-ipjd": {
        "name": "DOB NOW: Build – Job Application Filings",
        "endpoint": "https://data.cityofnewyork.us/resource/w9ak-ipjd.json",
        "date_fields": [":updated_at","filing_date","latest_action_date","pre_filing_date"],
        "owner_fields": ["owner_business_name","owner_name","owner_s_business_name","applicant_business_name",
                         "owner_s_first_name","owner_s_last_name","business_name"],
        "address_fields": ["house_number","street_name","bin","bbl","borough_block_lot","job_location_street_name","address"],
        "borough_fields": ["borough","borough_name","city"],
        "title_fields": ["job_type","proposed_occupancy_description","work_type","job_description"],
    },
    "rbx6-tga4": {
        "name": "DOB NOW: Build – Approved Permits",
        "endpoint": "https://data.cityofnewyork.us/resource/rbx6-tga4.json",
        "date_fields": [":updated_at","approval_date","filing_date","latest_action_date"],
        "owner_fields": ["owner_business_name","owner_name","owner_s_business_name","permittee_business_name",
                         "applicant_business_name","business_name"],
        "address_fields": ["house_number","street_name","address","bin","bbl"],
        "borough_fields": ["borough","borough_name","city"],
        "title_fields": ["job_type","work_type","job_description"],
    },
}

def soda_get(url: str, params: Dict[str, Any]):
    headers = dict(HEADERS)
    tok = os.getenv("NYC_SODA_APP_TOKEN")
    if tok:
        headers["X-App-Token"] = tok
    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def pick_first(rec: Dict[str, Any], keys: List[str]) -> str:
    for k in keys:
        v = rec.get(k)
        if not v: 
            continue
        if isinstance(v, dict) and "human_address" in v:
            try:
                addr = json.loads(v["human_address"])
                return f"{addr.get('address','')} {addr.get('city','')}".strip()
            except Exception:
                return str(v)
        if isinstance(v, (list, tuple)):
            return ", ".join(map(str, v))
        return str(v)
    return ""

def fetch_dob_recent():
    out = []
    for dsid, meta in SOC_DATASETS.items():
        url = meta["endpoint"]
        params = {"$order":":updated_at DESC", "$limit": 1000}
        try:
            rows = soda_get(url, params)
        except Exception as ex:
            logging.warning(f"SODA fetch failed: {dsid} -> {ex}")
            continue
        for r in rows:
            updated_str = r.get(":updated_at") or r.get("updated_at") or r.get("approval_date") or r.get("filing_date")
            keep = True
            if updated_str:
                dt = parse_iso(updated_str)
                if dt and (dt.tzinfo or False):
                    if dt.astimezone(UTC) < (datetime.utcnow() - timedelta(hours=LOOKBACK_HOURS)).replace(tzinfo=UTC):
                        keep = False
                elif dt and dt < (datetime.utcnow() - timedelta(hours=LOOKBACK_HOURS)):
                    keep = False
            if not keep: 
                continue
            dev = pick_first(r, meta["owner_fields"])
            addr = pick_first(r, meta["address_fields"])
            boro = pick_first(r, meta["borough_fields"])
            title = pick_first(r, meta["title_fields"]) or "DOB record"
            out.append({
                "date": datetime.now(NY_TZ).strftime("%Y-%m-%d"),
                "source": meta["name"], "title": title, "address": addr, "borough": boro,
                "developers": dev, "url": meta["endpoint"]
            })
    return out

def dataframe_pipeline():
    recs = []
    recs += fetch_yimby_recent()
    recs += fetch_trd_recent()
    recs += fetch_dob_recent()
    recs = [r for r in recs if str(r.get("developers","")).strip()]
    seen = set(); uniq = []
    for r in recs:
        key = (r["source"], r["title"].strip().lower(), r["address"].strip().lower())
        if key in seen: 
            continue
        seen.add(key); uniq.append(r)
    df = pd.DataFrame(uniq)
    if not df.empty:
        df = df[["date","source","title","address","borough","developers","url"]]\
               .sort_values(by=["date","source"], ascending=[False, True])
    return df

def open_gsheet(spreadsheet_id: str):
    creds_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not creds_json:
        raise RuntimeError("Missing GOOGLE_SERVICE_ACCOUNT_JSON env")
    creds = Credentials.from_service_account_info(
        json.loads(creds_json),
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    gc = gspread.authorize(creds)
    return gc.open_by_key(spreadsheet_id)

def ensure_worksheet(sh, title="Daily"):
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows="1000", cols="20")
        # write header
        ws.update([["date","source","title","address","borough","developers","url"]])
    return ws

def append_history_to_sheet(df: pd.DataFrame, spreadsheet_id: str, worksheet_name="Daily"):
    if df is None or df.empty:
        return 0
    sh = open_gsheet(spreadsheet_id)
    ws = ensure_worksheet(sh, worksheet_name)
    # Build existing key set to avoid duplicates across runs
    existing = ws.get_all_values()
    header = ["date","source","title","address","borough","developers","url"]
    if existing and existing[0] == header:
        data_rows = existing[1:]
    else:
        # reset header if mismatch
        ws.clear()
        ws.update([header])
        data_rows = []
    existing_keys = set()
    for row in data_rows:
        # pad row to 7 elements
        row = (row + [""]*7)[:7]
        key = (row[0], row[1], row[2].lower(), row[3].lower())
        existing_keys.add(key)

    new_rows = []
    for _, r in df.iterrows():
        key = (str(r["date"]), str(r["source"]), str(r["title"]).lower(), str(r["address"]).lower())
        if key not in existing_keys:
            new_rows.append([
                r.get("date",""), r.get("source",""), r.get("title",""), r.get("address",""),
                r.get("borough",""), r.get("developers",""), r.get("url","")
            ])
    if not new_rows:
        return 0
    # Use batch append in chunks to avoid size limits
    CHUNK = 200
    total = 0
    for i in range(0, len(new_rows), CHUNK):
        ws.append_rows(new_rows[i:i+CHUNK], value_input_option="USER_ENTERED")
        total += len(new_rows[i:i+CHUNK])
    return total

def main():
    logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
    spreadsheet_id = os.getenv("GSHEET_ID")
    worksheet_name = os.getenv("GSHEET_TAB", "Daily")
    if not spreadsheet_id:
        raise RuntimeError("Missing GSHEET_ID env")
    df = dataframe_pipeline()
    added = append_history_to_sheet(df, spreadsheet_id, worksheet_name)
    print(f"Appended rows: {added}")

if __name__ == "__main__":
    main()
