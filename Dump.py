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

start_time = time.time()

# -------------------- ENV --------------------
sec = os.getenv("PRABHAT_SECRET_KEY")
User_name = os.getenv("USERNAME")
service_account_json = os.getenv("SERVICE_ACCOUNT_JSON")
MB_URl = os.getenv("METABASE_URL")
QUERY_URL = os.getenv("DAILY_DUMP_QUERY")
SAK = os.getenv("SHEET_ACCESS_KEY")
TARGET_SHEET = "Daily Active Dump"

if not sec or not service_account_json:
    raise ValueError("❌ Missing environment variables. Check GitHub secrets.")

# -------------------- GOOGLE AUTH --------------------
service_info = json.loads(service_account_json)
creds = Credentials.from_service_account_info(
    service_info,
    scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
)
gc = gspread.authorize(creds)

# -------------------- METABASE AUTH --------------------
METABASE_HEADERS = {'Content-Type': 'application/json'}
res = requests.post(
    MB_URl,
    headers={"Content-Type": "application/json"},
    json={"username": User_name, "password": sec}
)
res.raise_for_status()
token = res.json()['id']
METABASE_HEADERS['X-Metabase-Session'] = token
print("✅ Metabase session created")

# -------------------- FETCH DATA --------------------
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

print("⏳ Fetching Metabase data...")
response = fetch_with_retry(QUERY_URL, METABASE_HEADERS)
df = pd.DataFrame(response.json())

df["Dump Date"] = datetime.now().strftime("%d/%m/%Y")
print(f"📊 Rows fetched: {len(df)}")

# -------------------- APPEND SHEET --------------------
sheet = gc.open_by_key(SAK)
worksheet = sheet.worksheet(TARGET_SHEET)

existing_data = worksheet.get_all_values()

if len(existing_data) == 0:
    set_with_dataframe(
        worksheet,
        df,
        include_index=False,
        include_column_header=True
    )
    print("📝 First run — writing header + data")
else:
    last_row = len(existing_data)
    values = df.values.tolist()
    worksheet.update(f"A{last_row+1}", values)
    print(f"➕ Appended {len(df)} rows")

elapsed = int(time.time() - start_time)
print(f"⏱ Done in {elapsed} seconds")
print("🎯 Daily dump completed!")
