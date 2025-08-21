#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NYC 开发&许可每日抓取器
=================================
用途：
- 每天抓取三类源：
  1) New York YIMBY（RSS + 正文解析：提取“owner/developer/sponsor”）
  2) The Real Deal（站内栏目页 + 正文解析：提取“developer/LLC/Inc/Group/Partners/Development”等组织名）
  3) NYC Open Data（DOB 相关数据集：最近24小时的“新建/许可”并抓取业主/申请方/许可主体字段）
- 统一输出 CSV，再由 GitHub Actions 提交到 public/nyc_developers_daily.csv
- Google Sheet 用 Apps Script 每天拉取并按唯一键去重追加（你已配置）

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

# 运行窗口（默认 24 小时，可通过环境变量 LOOKBACK_HOURS 覆盖）
LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "24"))
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
    # 常见 ISO 尾部 Z 的兼容
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    except Exception:
        return None

ORG_SUFFIX = r"(?:LLC|LLP|LP|Inc\.|Incorporated|Ltd\.|Ltd|Corp\.|Corporation|Company|Group|Partners|Properties|Holdings|Realty|Development|Builders|Construction|Management)"

DEV_PATTERNS = [
    # YIMBY 常见句式
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

YIMBY_FEEDS = [
    "https://newyorkyimby.com/feed",
]

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
    # 兜底：抓 LLC/Inc 等组织名
    if not names:
        for m in ORG_FALLBACK.finditer(text):
            name = m.group(1).strip().rstrip(',.;:')
            if name and name not in names:
                names.append(name)
    return names[:3]  # 控制噪音

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
            # 发布时间
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
                # 正文
                art = soup.select_one('article') or soup
                text = ' '.join([p.get_text(" ", strip=True) for p in art.select('p')])
                devs = extract_developers_from_text(text)
                # 地址/区粗提取
                title = html.unescape(e.title)
                borough = guess_borough(title + " " + text)
                # 地址：YIMBY 标题通常含地址
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
    # 收集近期文章链接
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
                # 过滤一些明显非文章的链接
                if any(x in href for x in ('/tag/', '/category/', '/author/', '/video', '/shop', '/events')):
                    continue
                seen.add(href)
                if len(seen) > max_links:
                    break
        except Exception as ex:
            logging.warning(f"TRD list fetch failed: {lp} -> {ex}")

    # 逐篇解析
    for url in list(seen)[:max_links]:
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            soup = BeautifulSoup(r.text, 'html.parser')
            # 发布时间
            dt_el = soup.select_one(TRD_TIME_SELECTOR)
            if dt_el and dt_el.has_attr('datetime'):
                dt = parse_iso(dt_el['datetime'])
            else:
                dt = None
            dt = (dt or datetime.now(NY_TZ)).astimezone(NY_TZ)
            if dt < SINCE_DT:
                continue

            # 正文
            art = soup.select_one('article') or soup
            title_el = art.select_one('h1')
            title = title_el.get_text(strip=True) if title_el else url
            text = ' '.join([p.get_text(" ", strip=True) for p in art.select('p')])
            devs = extract_developers_from_text(text)
            borough = guess_borough(title + " " + text)
            # 地址（粗提取）
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
            "owner_business_name", "owner_name"_

