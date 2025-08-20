# NYC Dev & DOB to Google Sheet (Append History)
This project scrapes YIMBY, The Real Deal, and NYC DOB Open Data, then **appends** new rows to a Google Sheet daily at 09:00 ET.

## Setup (one-time)
1. Create a Google Cloud project and enable **Google Sheets API**.
2. Create a **Service Account**, download the JSON key.
3. Share your target Google Sheet with the Service Account email (Editor).
4. In your GitHub repo **Settings → Secrets and variables → Actions**, add:
   - `GOOGLE_SERVICE_ACCOUNT_JSON` → paste the whole JSON key content
   - `GSHEET_ID` → the spreadsheet ID from the URL
   - (optional) `NYC_SODA_APP_TOKEN` → Socrata token for NYC Open Data

## Run
- GitHub Actions workflow runs at 09:00 ET daily (cron: 0 13 * * *).
- The script **appends** rows that are not already present, using
  `(date, source, title, address)` as a uniqueness key.
- Worksheet name defaults to `Daily` (change with `GSHEET_TAB`).

## Local Test
```bash
pip install requests beautifulsoup4 feedparser python-dateutil pandas gspread google-auth
export GOOGLE_SERVICE_ACCOUNT_JSON='{"type": "...", ... }'
export GSHEET_ID='your_sheet_id'
python scraper_gsheet.py
```
