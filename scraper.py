#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NYC 开发&许可每日抓取器
=================================
来源：
  1) New York YIMBY（RSS + 正文解析：提取 owner/developer/applicant/sponsor）
  2) The Real Deal（列表页 + 正文解析：提取组织名）
  3) NYC Open Data（DOB 相关数据集：最近窗口内记录 + 业主/申请方/许可主体）
输出：
  - CSV：nyc_developers_daily.csv（列：date, source, title, address, borough, developers, url）

依赖：
  pip install requests beautifulsoup4 feedparser python-dateutil pandas
"""

from __future__ import annotations
import os
import re
import sys
import time
import json
import html
import logging
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import List, Dict, Any, Optional

import requests
from bs4 import BeautifulSoup
import feedparser
from dateutil import tz
import pandas as pd

NY_TZ = tz.gettz("America/New_York")
UTC = tz.gettz("UTC")

# === 可配置项 ===
LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "24"))  # 抓取时间窗（小时）
DOB_ONLY_GENERAL = os.getenv("DOB_ONLY_GENERAL", "1") == "1"  # 仅保留 General Construction（默认开启）

SINCE_DT = datetime.now(NY_TZ) - timedelta(hours=LOOKBACK_HOURS)

HEADERS = {
    "User-Agent": "AcreNY-DevBot/1.0 (+https://acre.example) PythonRequests",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ------------------------- 通用工具 -------------------------

def parse_iso(dt_str: str) -> Optional[datetime]:
    for fmt in (
        "%a, %d %b %Y %H:%M:%S %z",  # RSS
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(dt_str, fmt)
        except Exception:
            continue
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
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

@dataclass
class Record:
    date: str
    source: str
    title: str
    address: str
    borough: str
    developers: List[str]
    url: str

# ------------------------- YIMBY -------------------------

YIMBY_FEEDS = ["https://newyorkyimby.com/feed"]

BOROUGH_WORDS = {
    'manhattan': 'Manhattan',
    'brooklyn': 'Brooklyn',
    'queens': 'Queens',
    'bronx': 'Bronx',
    'staten island': 'Staten Island',
}

def extract_developers_from_text(text: str) -> List[str]:
    names: List[str] = []
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

def guess_borough(text: str) -> str:
    t = text.lower()
    for k, v in BOROUGH_WORDS.items():
        if k in t:
            return v
    return ""

def fetch_yimby_recent() -> List[Record]:
    out: List[Record] = []
    for feed in YIMBY_FEEDS:
        d = feedparser.parse(feed)
        for e in d.entries:
            published = None
            for k in ("published", "updated", "created"):
                if hasattr(e, k):
                    published = parse_iso(getattr(e, k)) or published
            if not published and getattr(e, "published_parsed", None):
                published = datetime.fromtimestamp(time.mktime(e.published_parsed), tz=UTC).astimezone(NY_TZ)
            if not published:
                published = datetime.now(NY_TZ)
            if published < SINCE_DT:
                continue

            url = e.link
            try:
                html_resp = requests.get(url, headers=HEADERS, timeout=20)
                soup = BeautifulSoup(html_resp.text, 'html.parser')
                art = soup.select_one('article') or soup
                text = ' '.join([p.get_text(" ", strip=True) for p in art.select('p')])
                devs = extract_developers_from_text(text)
                title = html.unescape(e.title)
                borough = guess_borough(title + " " + text)
                address = title.split(' in ')[0].replace('Permits Filed for', '').strip()
                out.append(Record(
                    date=published.strftime('%Y-%m-%d'),
                    source='YIMBY',
                    title=title,
                    address=address,
                    borough=borough,
                    developers=devs,
                    url=url,
                ))
            except Exception as ex:
                logging.warning(f"YIMBY parse failed: {url} -> {ex}")
    return out

# ------------------------- The Real Deal -------------------------

TRD_LIST_PAGES = [
    "https://therealdeal.com/new-york/",
    "https://therealdeal.com/tag/new-development/",
]
TRD_TIME_SELECTOR = "time[datetime]"

def fetch_trd_recent(max_links: int = 40) -> List[Record]:
    out: List[Record] = []
    seen = set()
    for lp in TRD_LIST_PAGES:
        try:
            r = requests.get(lp, headers=HEADERS, timeout=20)
            soup = BeautifulSoup(r.text, 'html.parser')
            for a in soup.select('a[href]'):
                href = a['href']
                if not href.startswith('https://therealdeal.com/'):
                    continue
                if href in seen:
                    continue
                if any(x in href for x in ('/tag/', '/category/', '/author/', '/video', '/shop', '/events')):
                    continue
                seen.add(href)
                if len(seen) > max_links:
                    break
        except Exception as ex:
            logging.warning(f"TRD list fetch failed: {lp} -> {ex}")

    for url in list(seen)[:max_links]:
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            soup = BeautifulSoup(r.text, 'html.parser')
            dt_el = soup.select_one(TRD_TIME_SELECTOR)
            dt = parse_iso(dt_el['datetime']) if dt_el and dt_el.has_attr('datetime') else None
            dt = (dt or datetime.now(NY_TZ)).astimezone(NY_TZ)
            if dt < SINCE_DT:
                continue

            art = soup.select_one('article') or soup
            title_el = art.select_one('h1')
            title = title_el.get_text(strip=True) if title_el else url
            text = ' '.join([p.get_text(" ", strip=True) for p in art.select('p')])
            devs = extract_developers_from_text(text)
            borough = guess_borough(title + " " + text)
            m = re.search(r"(\d{1,5} [A-Za-z0-9'\- ]+ (?:Street|St\.|Avenue|Ave\.|Boulevard|Blvd\.|Road|Rd\.|Place|Pl\.|Court|Ct\.|Drive|Dr\.|Lane|Ln\.)(?:,?\s+(?:Brooklyn|Manhattan|Queens|Bronx|Staten Island))?)", title + " " + text)
            address = m.group(1) if m else ""
            out.append(Record(
                date=dt.strftime('%Y-%m-%d'),
                source='The Real Deal',
                title=title,
                address=address,
                borough=borough,
                developers=devs,
                url=url,
            ))
        except Exception as ex:
            logging.warning(f"TRD parse failed: {url} -> {ex}")
    return out

# ------------------------- NYC Open Data (DOB) -------------------------

SOC_DATASETS = {
    "ipu4-2q9a": {  # Permit Issuance（BIS）
        "name": "DOB Permit Issuance",
        "endpoint": "https://data.cityofnewyork.us/resource/ipu4-2q9a.json",
        "date_fields": [":updated_at", "issuance_date", "issue_date", "job_start_date", "filing_date"],
        "owner_fields": [
            "owner_business_name", "owner_business", "owner_name", "owners_business_name",
            "permittee_business_name", "permittee", "applicant_business_name", "business_name"
        ],
        "address_fields": ["house__", "house", "street_name", "streetname", "job_location_street_name", "address", "location"],
        "borough_fields": ["borough", "borocode", "bbl_borough", "city"],
        "title_fields": ["job_description", "work_description", "job_type"],
    },
    "w9ak-ipjd": {  # DOB NOW: Build – Job Application Filings
        "name": "DOB NOW: Build – Job Application Filings",
        "endpoint": "https://data.cityofnewyork.us/resource/w9ak-ipjd.json",
        "date_fields": [":updated_at", "filing_date", "latest_action_date", "pre_filing_date"],
        "owner_fields": [
            "owner_business_name", "owner_name", "owner_s_business_name", "applicant_business_name",
            "owner_s_first_name", "owner_s_last_name", "business_name"
        ],
        "address_fields": ["house_number", "street_name", "bin", "bbl", "borough_block_lot", "job_location_street_name", "address"],
        "borough_fields": ["borough", "borough_name", "city"],
        "title_fields": ["job_type", "proposed_occupancy_description", "work_type", "job_description"],
    },
    "rbx6-tga4": {  # DOB NOW: Build – Approved Permits
        "name": "DOB NOW: Build – Approved Permits",
        "endpoint": "https://data.cityofnewyork.us/resource/rbx6-tga4.json",
        "date_fields": [":updated_at", "approval_date", "filing_date", "latest_action_date"],
        "owner_fields": [
            "owner_business_name", "owner_name", "owner_s_business_name", "permittee_business_name",
            "applicant_business_name", "business_name"
        ],
        "address_fields": ["house_number", "street_name", "address", "bin", "bbl"],
        "borough_fields": ["borough", "borough_name", "city"],
        "title_fields": ["job_type", "work_type", "job_description"],
    },
}

SOC_APP_TOKEN = os.getenv("NYC_SODA_APP_TOKEN")

def soda_get(url: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    headers = dict(HEADERS)
    if SOC_APP_TOKEN:
        headers["X-App-Token"] = SOC_APP_TOKEN
    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def pick_first(rec: Dict[str, Any], keys: List[str]) -> str:
    for k in keys:
        v = rec.get(k)
        if not v:
            continue
        if isinstance(v, dict) and 'human_address' in v:
            try:
                addr = json.loads(v['human_address'])
                return f"{addr.get('address','')} {addr.get('city','')}".strip()
            except Exception:
                return str(v)
        if isinstance(v, (list, tuple)):
            return ', '.join(map(str, v))
        return str(v)
    return ""

# === 仅保留 General Construction 的判定 ===
def is_general_construction(rec: Dict[str, Any], meta: Dict[str, Any]) -> bool:
    """
    允许：General Construction / NB / A1/A2/A3 / Demolition / Foundation / Structural
    排除：Plumbing / Sprinkler / Standpipe / Fire Suppression / Mechanical / Boiler / Sign / Curb Cut / Sidewalk Shed 等
    """
    candidate_keys = set(meta.get("title_fields", [])) | {
        "work_type", "job_type", "permit_type", "permit_subtype",
        "work_type_description", "job_description"
    }
    parts = []
    for k in candidate_keys:
        v = rec.get(k)
        if isinstance(v, str) and v.strip():
            parts.append(v)
    t = " ".join(parts).lower().strip()
    if not t:
        return False

    BLOCK = (
        "plumbing", "sprinkler", "standpipe", "fire suppression", "fire-suppression",
        "mechanical", "hvac", "boiler", "fuel burning", "fuel storage",
        "sign", "curb cut", "sidewalk shed", "scaffold", "antenna",
        "sprinklers", "fire alarm"
    )
    if any(b in t for b in BLOCK):
        return False

    ALLOW = (
        "general construction", "ot-general construction", "ot general construction",
        "new building", "foundation", "structural", "demolition"
    )
    if any(a in t for a in ALLOW):
        return True

    if re.search(r"\b(nb|dm|a1|a2|a3)\b", t):
        return True

    return False

def fetch_dob_recent() -> List[Record]:
    out: List[Record] = []
    for dsid, meta in SOC_DATASETS.items():
        url = meta['endpoint']
        params = {"$order": ":updated_at DESC", "$limit": 1000}
        try:
            rows = soda_get(url, params)
        except Exception as ex:
            logging.warning(f"SODA fetch failed: {dsid} -> {ex}")
            continue

        for r in rows:
            # 时间窗口过滤
            updated_str = r.get(":updated_at") or r.get("updated_at") or r.get("approval_date") or r.get("filing_date")
            keep = True
            if updated_str:
                try:
                    dt = parse_iso(updated_str) or datetime.fromisoformat(updated_str.replace('Z', '+00:00'))
                    dt_utc = dt if dt.tzinfo else dt.replace(tzinfo=UTC)
                    if dt_utc < (datetime.now(UTC) - timedelta(hours=LOOKBACK_HOURS)):
                        keep = False
                except Exception:
                    pass
            if not keep:
                continue

            # 只保留 General Construction（开关可通过环境变量控制）
            if DOB_ONLY_GENERAL and not is_general_construction(r, meta):
                continue

            dev  = pick_first(r, meta['owner_fields'])
            addr = pick_first(r, meta['address_fields'])
            boro = pick_first(r, meta['borough_fields'])
            title = pick_first(r, meta['title_fields']) or 'DOB record'
            if not any([dev, addr, boro, title]):
                continue

            out.append(Record(
                date=datetime.now(NY_TZ).strftime('%Y-%m-%d'),
                source=meta['name'],
                title=title,
                address=addr,
                borough=boro,
                developers=[dev] if dev else [],
                url=meta['endpoint'],
            ))
    return out

# ------------------------- 汇总 & 导出 -------------------------

def dedupe(records: List[Record]) -> List[Record]:
    seen = set()
    uniq: List[Record] = []
    for r in records:
        key = (r.source, r.title.strip().lower(), r.address.strip().lower())
        if key in seen:
            continue
        seen.add(key)
        uniq.append(r)
    return uniq

def main(outfile: str = "nyc_developers_daily.csv"):
    logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
    logging.info(f"Time window since: {SINCE_DT.strftime('%Y-%m-%d %H:%M %Z')}  |  DOB_ONLY_GENERAL={DOB_ONLY_GENERAL}")
    recs: List[Record] = []
    recs += fetch_yimby_recent()
    recs += fetch_trd_recent()
    recs += fetch_dob_recent()
    recs = [r for r in recs if r.developers]  # 仅保留识别出开发商/业主的记录
    recs = dedupe(recs)

    rows: List[Dict[str, Any]] = []
    for r in recs:
        rows.append({
            "date": r.date,
            "source": r.source,
            "title": r.title,
            "address": r.address,
            "borough": r.borough,
            "developers": '; '.join(r.developers),
            "url": r.url,
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df.sort_values(by=["date", "source"], ascending=[False, True], inplace=True)
    df.to_csv(outfile, index=False)
    print(f"Saved {len(df)} rows -> {outfile}")

if __name__ == "__main__":
    outfile = sys.argv[1] if len(sys.argv) > 1 else "nyc_developers_daily.csv"
    try:
        main(outfile)
    except Exception:
        logging.exception("Fatal error in scraper; writing placeholder CSV.")
        pd.DataFrame(columns=["date","source","title","address","borough","developers","url"]).to_csv(outfile, index=False)
        print(f"Saved 0 rows -> {outfile} (placeholder due to error)")
