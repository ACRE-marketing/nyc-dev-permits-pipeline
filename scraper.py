#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NYC 开发&许可每日抓取器（CSV 输出）
依赖：requests beautifulsoup4 feedparser python-dateutil pandas
环境变量（可选）：LOOKBACK_HOURS=24；NYC_SODA_APP_TOKEN=<你的Socrata Token>
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

NY_TZ = tz.gettz("America/New_York")
UTC = tz.gettz("UTC")

LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "24"))
SINCE_DT = datetime.now(NY_TZ) - timedelta(hours=LOOKBACK_HOURS)

HEADERS = {
    "User-Agent": "NYC-Dev-Pipeline/1.0 (+GitHub Actions) PythonRequests",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

def parse_iso(dt_str: Optional[str]) -> Optional[datetime]:
    if not dt_str:
        return None
    fmts = [
        "%a, %d %b %Y %H:%M:%S %z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%d",
    ]
    for fmt in fmts:
        try:
            return datetime.strptime(dt_str, fmt)
        except Exception:
            pass
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    except Exception:
        return None

BOROUGH_WORDS = {
    "manhattan": "Manhattan",
    "brooklyn": "Brooklyn",
    "queens": "Queens",
    "bronx": "Bronx",
    "staten island": "Staten Island",
}

def guess_borough(text: str) -> str:
    t = text.lower()
    for k, v in BOROUGH_WORDS.items():
        if k in t:
            return v
    return ""

ORG_SUFFIX = r"(?:LLC|LLP|LP|Inc\.|Incorporated|Ltd\.|Ltd|Corp\.|Corporation|Company|Group|Partners|Properties|Holdings|Realty|Development|Builders|Construction|Management)"
DEV_PATTERNS = [
    re.compile(r"(?i)(?:is|are) listed as the (?:owner|developer|applicant|sponsor)[^,.]*?([A-Z][\w&'\.\- ]+(?:\s+"+ORG_SUFFIX+r")?)"),
    re.compile(r"(?i)(?:the\s+)?developer(?:s)?\s+(?:is|are)\s+\b([A-Z][\w&'\.\- ]+(?:\s+"+ORG_SUFFIX+r")?)"),
    re.compile(r"(?i)developed\s+by\s+\b([A-Z][\w&'\.\- ]+(?:\s+"+ORG_SUFFIX+r")?)"),
    re.compile(r"(?i)owner\s+(?:is|are)\s+\b([A-Z][\w&'\.\- ]+(?:\s+"+ORG_SUFFIX+r")?)"),
]
ORG_FALLBACK = re.compile(r"\b([A-Z][\w&'\.\- ]+\s(?:"+ORG_SUFFIX+r"))(?!\w)")

def extract_developers_from_text(text: str) -> List[str]:
    names: List[str] = []
    for pat in DEV_PATTERNS:
        for m in pat.finditer(text):
            name = m.group(1).strip().rstrip(",.;:")
            if name and name not in names:
                names.append(name)
    if not names:
        for m in ORG_FALLBACK.finditer(text):
            name = m.group(1).strip().rstrip(",.;:")
            if name and name not in names:
                names.append(name)
    return names[:3]

def fetch_yimby_recent() -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    feeds = ["https://newyorkyimby.com/feed"]
    for feed in feeds:
        d = feedparser.parse(feed)
        for e in d.entries:
            published = None
            for k in ("published", "updated", "created"):
                if hasattr(e, k):
                    published = parse_iso(getattr(e, k)) or published
            if not published and getattr(e, "published_parsed", None):
                published = datetime.fromtimestamp(time.mktime(e.published_parsed), tz=UTC)
            if not published:
                published = datetime.now(UTC)
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
                borough = guess_borough(title + " " + text)
                address = title.split(" in ")[0].replace("Permits Filed for", "").strip()
                out.append({
                    "date": published.astimezone(NY_TZ).strftime("%Y-%m-%d"),
                    "source": "YIMBY",
                    "title": title,
                    "address": address,
                    "borough": borough,
                    "developers": "; ".join(devs),
                    "url": url
                })
            except Exception as ex:
                logging.warning(f"YIMBY parse failed: {url} -> {ex}")
    return out

def fetch_trd_recent(max_links: int = 40) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen: set[str] = set()
    list_pages = [
        "https://therealdeal.com/new-york/",
        "https://therealdeal.com/tag/new-development/",
    ]
    for lp in list_pages:
        try:
            r = requests.get(lp, headers=HEADERS, timeout=25)
            soup = BeautifulSoup(r.text, "html.parser")
            for a in soup.select("a[href]"):
                href = a["href"]
                if not href.startswith("https://therealdeal.com/"):
                    continue
                if any(x in href for x in ("/tag/", "/category/", "/author/", "/video", "/shop", "/events")):
                    continue
                if href in seen:
                    continue
                seen.add(href)
                if len(seen) >= max_links:
                    break
        except Exception as ex:
            logging.warning(f"TRD list fetch failed: {lp} -> {ex}")

    for url in list(seen)[:max_links]:
        try:
            r = requests.get(url, headers=HEADERS, timeout=25)
            soup = BeautifulSoup(r.text, "html.parser")
            dt_el = soup.select_one("time[datetime]")
            dt = parse_iso(dt_el["datetime"]) if dt_el and dt_el.has_attr("datetime") else None
            if not dt:
                dt = datetime.now(UTC)
            if (dt if dt.tzinfo else dt.replace(tzinfo=UTC)).astimezone(NY_TZ) < SINCE_DT:
                continue

            art = soup.select_one("article") or soup
            title_el = art.select_one("h1")
            title = title_el.get_text(strip=True) if title_el else url
            text = " ".join([p.get_text(" ", strip=True) for p in art.select("p")])
            devs = extract_developers_from_text(text)
            borough = guess_borough(title + " " + text)
            m = re.search(r"(\d{1,5} [A-Za-z0-9'\- ]+ (?:Street|St\.|Avenue|Ave\.|Boulevard|Blvd\.|Road|Rd\.|Place|Pl\.|Court|Ct\.|Drive|Dr\.|Lane|Ln\.)(?:,\s?(?:Brooklyn|Manhattan|Queens|Bronx|Staten Island))?)",
                          title + " " + text)
            address = m.group(1) if m else ""
            out.append({
                "date": (dt if dt.tzinfo else dt.replace(tzinfo=UTC)).astimezone(NY_TZ).strftime("%Y-%m-%d"),
                "source": "The Real Deal",
                "title": title,
                "address": address,
                "borough": borough,
                "developers": "; ".join(devs),
                "url": url
            })
        except Exception as ex:
            logging.warning(f"TRD parse failed: {url} -> {ex}")
    return out

SOC_DATASETS = {
    "ipu4-2q9a": {
        "name": "DOB Permit Issuance",
        "endpoint": "https://data.cityofnewyork.us/resource/ipu4-2q9a.json",
        "owner_fields": ["owner_business_name","owner_business","owner_name","owners_business_name",
                         "permittee_business_name","permittee","applicant_business_name","business_name"],
        "address_fields": ["house__","house","street_name","streetname","job_location_street_name","address","location"],
        "borough_fields": ["borough","borocode","bbl_borough","city"],
        "title_fields": ["job_description","work_description","job_type"],
        "date_fields": [":updated_at","issuance_date","issue_date","job_start_date","filing_date"]
    },
    "w9ak-ipjd": {
        "name": "DOB NOW: Build – Job Application Filings",
        "endpoint": "https://data.cityofnewyork.us/resource/w9ak-ipjd.json",
        "owner_fields": ["owner_business_name","owner_name","owner_s_business_name","applicant_business_name",
                         "owner_s_first_name","owner_s_last_name","business_name"],
        "address_fields": ["house_number","street_name","bin","bbl","borough_block_lot","job_location_street_name","address"],
        "borough_fields": ["borough","borough_name","city"],
        "title_fields": ["job_type","proposed_occupancy_description","work_type","job_description"],
        "date_fields": [":updated_at","filing_date","latest_action_date","pre_filing_date"]
    },
    "rbx6-tga4": {
        "name": "DOB NOW: Build – Approved Permits",
        "endpoint": "https://data.cityofnewyork.us/resource/rbx6-tga4.json",
        "owner_fields": ["owner_business_name","owner_name","owner_s_business_name","permittee_business_name",
                         "applicant_business_name","business_name"],
        "address_fields": ["house_number","street_name","address","bin","bbl"],
        "borough_fields": ["borough","borough_name","city"],
        "title_fields": ["job_type","work_type","job_description"],
        "date_fields": [":updated_at","approval_date","filing_date","latest_action_date"]
    },
}

def soda_get(url: str, params: Dict[str, Any]) -> Any:
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

def fetch_dob_recent() -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for dsid, meta in SOC_DATASETS.items():
        url = meta["endpoint"]
        params = {"$order": ":updated_at DESC", "$limit": 1000}
        try:
            rows = soda_get(url, params)
        except Exception as ex:
            logging.warning(f"SODA fetch failed: {dsid} -> {ex}")
            continue
        for r in rows:
            updated_str = None
            for k in meta["date_fields"]:
                updated_str = r.get(k) or updated_str
            keep = True
            if updated_str:
                dt = parse_iso(updated_str)
                if dt:
                    dt_utc = dt if dt.tzinfo else dt.replace(tzinfo=UTC)
                    if dt_utc < (datetime.now(UTC) - timedelta(hours=LOOKBACK_HOURS)):
                        keep = False
            if not keep:
                continue

            dev = pick_first(r, meta["owner_fields"])
            addr = pick_first(r, meta["address_fields"])
            boro = pick_first(r, meta["borough_fields"])
            title = pick_first(r, meta["title_fields"]) or "DOB record"

            out.append({
                "date": datetime.now(NY_TZ).strftime("%Y-%m-%d"),
                "source": meta["name"],
                "title": title,
                "address": addr,
                "borough": boro,
                "developers": dev,
                "url": meta["endpoint"]
            })
    return out

def dataframe_pipeline() -> pd.DataFrame:
    recs: List[Dict[str, Any]] = []
    recs += fetch_yimby_recent()
    recs += fetch_trd_recent()
    recs += fetch_dob_recent()

    recs = [r for r in recs if str(r.get("developers", "")).strip()]
    seen = set()
    uniq: List[Dict[str, Any]] = []
    for r in recs:
        key = (r["source"], r["title"].strip().lower(), r["address"].strip().lower())
        if key in seen:
            continue
        seen.add(key)
        uniq.append(r)

    df = pd.DataFrame(uniq)
    if df.empty:
        return df
    df = df[["date", "source", "title", "address", "borough", "developers", "url"]]            .sort_values(by=["date", "source"], ascending=[False, True])
    return df

def main(outfile: str = "nyc_developers_daily.csv") -> None:
    logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
    logging.info(f"Time window since: {SINCE_DT.strftime('%Y-%m-%d %H:%M %Z')}")
    df = dataframe_pipeline()
    df.to_csv(outfile, index=False)
    print(f"Saved {len(df)} rows -> {outfile}")

if __name__ == "__main__":
    import sys
    outfile = sys.argv[1] if len(sys.argv) > 1 else "nyc_developers_daily.csv"
    main(outfile)
