from google.oauth2 import service_account
from googleapiclient.discovery import build
import pandas as pd
from datetime import datetime
import os

# =======================================================
# CONFIGURATION
# =======================================================
SERVICE_ACCOUNT_FILE = 'screeningfollowup-4a463d7d64cb.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

SPREADSHEET_ID = '1X6Djm-UbmDvvJC39B66mnLYODHv5qNaZi3nMiahZEzk'  # Google Sheet ID
SHEET_NAME = 'Mapping'

OUTPUT_DIR = r"D:\matching_harsh\Lineup_Followup\input"
today_str = datetime.now().strftime("%d-%m-%Y")
output_filename = f"MASTER FILE LOCATIONS - {SHEET_NAME}.xlsx"
output_path = os.path.join(OUTPUT_DIR, output_filename)

# =======================================================
# PREPARE OUTPUT FOLDER
# =======================================================
os.makedirs(OUTPUT_DIR, exist_ok=True)

# =======================================================
# AUTHENTICATE GOOGLE SHEETS
# =======================================================
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)
service = build('sheets', 'v4', credentials=credentials)

# =======================================================
# FETCH DATA FROM GOOGLE SHEET
# =======================================================
print(f"📥 Fetching sheet '{SHEET_NAME}' from master file...")

result = service.spreadsheets().values().get(
    spreadsheetId=SPREADSHEET_ID,
    range=f"{SHEET_NAME}"
).execute()

values = result.get('values', [])
if not values:
    raise ValueError(f"❌ No data found in sheet '{SHEET_NAME}'")

# =======================================================
# CONVERT TO DATAFRAME (preserve all columns)
# =======================================================
header = [str(h).strip().replace('"', '') for h in values[0]]
data = values[1:]

# Pad each row to ensure it has same number of columns as header
max_cols = len(header)
for i, row in enumerate(data):
    if len(row) < max_cols:
        data[i] = row + [''] * (max_cols - len(row))
    elif len(row) > max_cols:
        data[i] = row[:max_cols]

df = pd.DataFrame(data, columns=header)

print(f"✅ Loaded full dataset: {len(df)} rows, {len(df.columns)} columns")

# =======================================================
# FILTER ONLY ACTIVE JOBS (optional)
# =======================================================
if 'Active /Inactive' in df.columns:
    before_count = len(df)
    df = df[df['Active /Inactive'].astype(str).str.strip().str.lower() == 'active']
    after_count = len(df)
    print(f"✅ Filtered only 'Active' jobs: {after_count} of {before_count} rows retained.")
else:
    print("⚠️ Column 'Active /Inactive' not found; no filtering applied.")

# =======================================================
# SAVE TO EXCEL
# =======================================================
df.to_excel(output_path, index=False)
print(f"💾 Saved full sheet '{SHEET_NAME}' successfully to: {output_path}")
print(f"📊 Total rows saved: {len(df)} | Total columns: {len(df.columns)}")
try:
    import subprocess
    print("▶️ Running main7.py ...")
    subprocess.run(["python", r"D:\matching_harsh\Lineup_Followup\main7.py"], check=True)
    print("✅ main7.py executed successfully!")
except Exception as e:
    print(f"❌ Failed to run main7.py: {e}")
