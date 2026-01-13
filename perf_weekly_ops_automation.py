# -*- coding: utf-8 -*-

import os
import time
import json
import requests
import pandas as pd
import gspread
from datetime import datetime
from zoneinfo import ZoneInfo
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from concurrent.futures import ThreadPoolExecutor


# -------------------- START TIMER --------------------
start_time = time.time()

# -------------------- ENV & AUTH (GitHub Secrets) --------------------
sec = os.environ.get("PRABHAT_SECRET_KEY")
User_name = os.environ.get("USERNAME")
service_account_json = os.environ.get("SERVICE_ACCOUNT_JSON")
MB_URl = os.environ.get("METABASE_URL")

# -------------------- METABASE QUERIES --------------------
ASSIGNED_QUERY_Var = os.environ.get("ASSIGNED_QUERY")
CALLING_QUERY_Var = os.environ.get("CALLING_QUERY")
STAGE_CHANGE_QUERY_Var = os.environ.get("STAGE_CHANGE_QUERY")

# -------------------- GOOGLE SHEET --------------------
SAK = os.environ.get("SHEET_ACCESS_KEY")

# -------------------- VALIDATION --------------------
required_vars = [
    sec, User_name, service_account_json, MB_URl,
    ASSIGNED_QUERY_Var, CALLING_QUERY_Var, STAGE_CHANGE_QUERY_Var, SAK
]

if not all(required_vars):
    raise ValueError("❌ Missing one or more required environment variables. Check GitHub Secrets.")

# -------------------- GOOGLE AUTH --------------------
service_info = json.loads(service_account_json)

creds = Credentials.from_service_account_info(
    service_info,
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)

gc = gspread.authorize(creds)

# -------------------- METABASE AUTH --------------------
METABASE_HEADERS = {'Content-Type': 'application/json'}

res = requests.post(
    MB_URl,
    headers={"Content-Type": "application/json"},
    json={"username": User_name.strip(), "password": sec},
    timeout=60
)

res.raise_for_status()
token = res.json()["id"]
METABASE_HEADERS["X-Metabase-Session"] = token

print("✅ Metabase session created")

# -------------------- UTILITIES --------------------
def fetch_with_retry(url, headers, retries=5, delay=15):
    for attempt in range(1, retries + 1):
        try:
            r = requests.post(url, headers=headers, timeout=120)
            r.raise_for_status()
            return r
        except Exception as e:
            print(f"[Metabase] Attempt {attempt} failed: {e}")
            if attempt < retries:
                time.sleep(delay)
            else:
                raise


def safe_update_range(worksheet, df, data_range):
    print(f"🔄 Updating {worksheet.title}")

    backup_data = worksheet.get(data_range)

    try:
        set_with_dataframe(
            worksheet,
            df,
            include_index=False,
            include_column_header=True,
            resize=False
        )
        print(f"✅ {worksheet.title} updated")
    except Exception as e:
        print(f"❌ Update failed for {worksheet.title}, restoring backup")
        worksheet.update(data_range, backup_data)
        raise e


# -------------------- MAIN LOGIC --------------------
print("Fetching Assigned + Calls + StageChange in parallel...")

urls = {
    "Assigned": ASSIGNED_QUERY_Var.strip(),
    "Calls": CALLING_QUERY_Var.strip(),
    "StageChange": STAGE_CHANGE_QUERY_Var.strip()
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

# -------------------- GOOGLE SHEETS --------------------
print("Connecting to Google Sheets...")
sheet = gc.open_by_key(SAK)

ws_1 = sheet.worksheet("Assigned")
ws_2 = sheet.worksheet("Calls")
ws_3 = sheet.worksheet("StageChange")
main_sheet = sheet.worksheet("BOFU Ops")

# -------------------- UPDATE SHEETS --------------------
safe_update_range(ws_1, df_Assigned, "A:F")
time.sleep(3)

safe_update_range(ws_2, df_Calls, "A:E")
time.sleep(3)

safe_update_range(ws_3, df_StageChange, "A:E")

# -------------------- UPDATE TIMESTAMP --------------------
current_time = datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%d-%b-%Y %H:%M:%S")
main_sheet.update("B29", [[current_time]])

print(f"✅ Updated timestamp: {current_time}")

# -------------------- TIMER SUMMARY --------------------
elapsed_time = time.time() - start_time
mins, secs = divmod(elapsed_time, 60)

print(f"⏱ Total time taken: {int(mins)}m {int(secs)}s")
print("🎯 Workflow completed successfully!")
