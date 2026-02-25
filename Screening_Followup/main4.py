# ============================================================
# main10_fixed_service.py  (Finploy Location Mapping - Updated for New Service Account)
# ============================================================

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import re

# ============================================================
# CONFIGURATION
# ============================================================
main_file = r"D:\matching_harsh\Screening_Followup\output\output2.xlsx"
OUTPUT_DIR = r"D:\matching_harsh\Screening_Followup\output"

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

output_file = os.path.join(OUTPUT_DIR, "output3.xlsx")
additional_file = os.path.join(OUTPUT_DIR, "additional_new_location.xlsx")

# ============================================================
# GOOGLE SHEET AUTHENTICATION (NEW SERVICE ACCOUNT)
# ============================================================
SERVICE_ACCOUNT_FILE = r"D:\matching_harsh\Screening_Followup\screeningfollowup-4a463d7d64cb.json"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
client = gspread.authorize(creds)

# ============================================================
# OPEN GOOGLE SHEET (Finploy Location Master)
# ============================================================
sheet_url = "https://docs.google.com/spreadsheets/d/11Yye2zMLOgb0J8wBjH0VJNuOV28AAERNPxr3RE2OO-E/edit#gid=0"
sheet = client.open_by_url(sheet_url).sheet1
data = sheet.get_all_records()
df_location = pd.DataFrame(data)
print(f"✅ Loaded location master: {len(df_location)} rows.")

# ============================================================
# LOAD MAIN DATASET
# ============================================================
df_main = pd.read_excel(main_file)
print(f"✅ Loaded main dataset: {len(df_main)} rows.")

# ============================================================
# CLEAN REFERENCE COLUMNS
# ============================================================
def clean_text(value):
    """Cleans up text by removing punctuation and extra spaces."""
    value = str(value).strip().lower()
    value = re.sub(r"[.,;:]+$", "", value)
    return value

for col in ["area", "city"]:
    if col in df_location.columns:
        df_location[col] = df_location[col].astype(str).apply(clean_text)

if "id" in df_location.columns:
    df_location["id"] = df_location["id"].astype(str).str.strip()

# ============================================================
# LOCATION MAPPING LOGIC
# ============================================================
unmatched_rows = []  # store unmatched candidate rows

def map_location(loc, row):
    """Return location ID if found, else record full candidate row."""
    if pd.isna(loc) or str(loc).strip() == "":
        unmatched_rows.append({**row.to_dict(), "unmatched_location": "blank"})
        return "NA"

    loc_clean = clean_text(loc)

    # Check area first
    match_area = df_location[df_location["area"] == loc_clean] if "area" in df_location.columns else pd.DataFrame()
    if not match_area.empty:
        return match_area["id"].values[0]

    # Then check city
    match_city = df_location[df_location["city"] == loc_clean] if "city" in df_location.columns else pd.DataFrame()
    if not match_city.empty:
        return match_city["id"].values[0]

    # Not found
    unmatched_rows.append({**row.to_dict(), "unmatched_location": loc})
    return "NA"

# Apply mapping
if "location" not in df_main.columns:
    raise Exception("❌ 'location' column missing in main dataset!")

df_main["finploy_id"] = df_main.apply(lambda x: map_location(x.get("location", ""), x), axis=1)
print("✅ Location mapping applied.")

# ============================================================
# SAVE UPDATED MAIN DATASET
# ============================================================
df_main.to_excel(output_file, index=False)
print(f"✅ Phase 2.3 completed. Updated file saved as '{output_file}'.")

# ============================================================
# HANDLE UNMATCHED LOCATIONS
# ============================================================
if unmatched_rows:
    df_unmatched_new = pd.DataFrame(unmatched_rows)
    df_unmatched_new.drop_duplicates(inplace=True)
    df_unmatched_new["status"] = "new_location_needed"

    if os.path.exists(additional_file):
        df_existing = pd.read_excel(additional_file)
        df_combined = pd.concat([df_existing, df_unmatched_new], ignore_index=True)
        df_combined.drop_duplicates(inplace=True)
        df_combined.to_excel(additional_file, index=False)
        print(f"⚠️ Unmatched locations appended to '{additional_file}'.")
    else:
        df_unmatched_new.to_excel(additional_file, index=False)
        print(f"⚠️ Unmatched locations saved to new file '{additional_file}'.")
else:
    print("✅ All locations matched successfully — no unmatched candidates found.")
try:
    import subprocess
    print("▶️ Running main5.py ...")
    subprocess.run(["python", r"D:\matching_harsh\Screening_Followup\main5.py"], check=True)
    print("✅ main5.py executed successfully!")
except Exception as e:
    print(f"❌ Failed to run main5.py: {e}")