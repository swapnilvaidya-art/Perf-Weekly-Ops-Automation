# -*- coding: utf-8 -*-

import os
import time
import json
import requests
import pandas as pd
import gspread
from datetime import datetime
from zoneinfo import ZoneInfo
from google.oauth2.service_account import Credentials
from concurrent.futures import ThreadPoolExecutor


# ======================================================
# START TIMER
# ======================================================
start_time = time.time()


# ======================================================
# ENVIRONMENT VARIABLES
# ======================================================
PRABHAT_SECRET_KEY = os.getenv("PRABHAT_SECRET_KEY")
USERNAME = os.getenv("USERNAME")
SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON")
METABASE_URL = os.getenv("METABASE_URL")

ASSIGNED_QUERY = os.getenv("ASSIGNED_QUERY")
CALLING_QUERY = os.getenv("CALLING_QUERY")
STAGE_CHANGE_QUERY = os.getenv("STAGE_CHANGE_QUERY")

SHEET_ACCESS_KEY = os.getenv("SHEET_ACCESS_KEY")

required_vars = [
    PRABHAT_SECRET_KEY,
    USERNAME,
    SERVICE_ACCOUNT_JSON,
    METABASE_URL,
    ASSIGNED_QUERY,
    CALLING_QUERY,
    STAGE_CHANGE_QUERY,
    SHEET_ACCESS_KEY
]

if not all(required_vars):
    raise ValueError("❌ Missing required environment variables. Check GitHub Secrets.")


# ======================================================
# GOOGLE SHEETS AUTH
# ======================================================
service_info = json.loads(SERVICE_ACCOUNT_JSON)

creds = Credentials.from_service_account_info(
    service_info,
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)

gc = gspread.authorize(creds)


# ======================================================
# METABASE AUTH
# ======================================================
METABASE_HEADERS = {"Content-Type": "application/json"}

res = requests.post(
    METABASE_URL,
    headers=METABASE_HEADERS,
    json={"username": USERNAME.strip(), "password": PRABHAT_SECRET_KEY},
    timeout=60
)

res.raise_for_status()
METABASE_HEADERS["X-Metabase-Session"] = res.json()["id"]

print("✅ Metabase session created")


# ======================================================
# UTILITIES
# ======================================================
def fetch_with_retry(url, headers, retries=5, base_delay=10):
    for attempt in range(1, retries + 1):
        try:
            r = requests.post(url, headers=headers, timeout=180)
            r.raise_for_status()
            return r
        except Exception as e:
            print(f"[Metabase] Attempt {attempt} failed: {e}")
            if attempt < retries:
                time.sleep(base_delay * attempt)
            else:
                raise


def safe_update_range(worksheet, df, start_cell="A1", retries=5, base_delay=15):
    print(f"🔄 Updating sheet: {worksheet.title}")

    if df.empty:
        print(f"⚠️ {worksheet.title} dataframe empty — skipping update")
        return

    values = [df.columns.tolist()] + (
        df.astype(str).fillna("").values.tolist()
    )

    for attempt in range(1, retries + 1):
        try:
            worksheet.clear()
            time.sleep(3)

            worksheet.update(
                range_name=start_cell,
                values=values
            )

            print(f"✅ {worksheet.title} updated successfully")
            return

        except Exception as e:
            print(f"[Sheets] Update attempt {attempt} failed: {e}")
            if attempt < retries:
                sleep_time = base_delay * attempt
                print(f"⏳ Retrying in {sleep_time}s...")
                time.sleep(sleep_time)
            else:
                raise


# ======================================================
# MAIN LOGIC
# ======================================================
print("Fetching Assigned + Calls + StageChange in parallel...")

urls = {
    "Assigned": ASSIGNED_QUERY.strip(),
    "Calls": CALLING_QUERY.strip(),
    "StageChange": STAGE_CHANGE_QUERY.strip()
}

with ThreadPoolExecutor(max_workers=3) as executor:
    futures = {
        name: executor.submit(fetch_with_retry, url, METABASE_HEADERS)
        for name, url in urls.items()
    }
    results = {name: f.result() for name, f in futures.items()}

df_Assigned = pd.DataFrame(results["Assigned"].json())
df_Calls = pd.DataFrame(results["Calls"].json())
df_StageChange = pd.DataFrame(results["StageChange"].json())


# ======================================================
# GOOGLE SHEETS
# ======================================================
print("Connecting to Google Sheets...")
sheet = gc.open_by_key(SHEET_ACCESS_KEY)

ws_assigned = sheet.worksheet("Assigned")
ws_calls = sheet.worksheet("Calls")
ws_stage = sheet.worksheet("StageChange")
ws_main = sheet.worksheet("BOFU Ops")


# ======================================================
# UPDATE SHEETS (SEQUENTIAL + COOLDOWN)
# ======================================================
safe_update_range(ws_assigned, df_Assigned, "A1")
time.sleep(20)

safe_update_range(ws_calls, df_Calls, "A1")
time.sleep(20)

safe_update_range(ws_stage, df_StageChange, "A1")
time.sleep(20)


# ======================================================
# UPDATE TIMESTAMP
# ======================================================
current_time = datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%d-%b-%Y %H:%M:%S")

for attempt in range(1, 6):
    try:
        ws_main.update(
            range_name="B2",
            values=[[current_time]]
        )
        break
    except Exception as e:
        print(f"[Sheets] Timestamp update attempt {attempt} failed: {e}")
        time.sleep(10 * attempt)

print(f"✅ Updated timestamp in B2: {current_time}")



# ======================================================
# TIMER SUMMARY
# ======================================================
elapsed_time = time.time() - start_time
mins, secs = divmod(elapsed_time, 60)

print(f"⏱ Total time taken: {int(mins)}m {int(secs)}s")
print("🎯 Perf Weekly Ops Automation completed successfully!")
