# ============================================================
# main10_fixed.py  (Finploy Location Mapping - Full Version)
# ============================================================

import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import re

# ============================================================
# CONFIGURATION
# ============================================================
main_file = r"D:\matching_harsh\Job_matching_Screened\output\output2.xlsx"  # Input file
OUTPUT_DIR = r"D:\matching_harsh\Job_matching_Screened\output"

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

output_file = os.path.join(OUTPUT_DIR, "output3.xlsx")  # Updated main dataset
additional_file = os.path.join(OUTPUT_DIR, "additional_new_location.xlsx")  # Unmatched candidate details

# ============================================================
# GOOGLE SHEET SETUP
# ============================================================
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)
client = gspread.authorize(creds)

# Open Google Sheet (Finploy Location Master)
sheet_url = "https://docs.google.com/spreadsheets/d/11Yye2zMLOgb0J8wBjH0VJNuOV28AAERNPxr3RE2OO-E/edit#gid=0"
sheet = client.open_by_url(sheet_url).sheet1
data = sheet.get_all_records()
df_location = pd.DataFrame(data)

# ============================================================
# LOAD MAIN DATASET
# ============================================================
df_main = pd.read_excel(main_file)

# ============================================================
# CLEAN REFERENCE COLUMNS
# ============================================================
def clean_text(value):
    """Cleans up text by removing punctuation and extra spaces."""
    value = str(value).strip().lower()
    value = re.sub(r'[.,;:]+$', '', value)
    return value

df_location['area'] = df_location['area'].astype(str).apply(clean_text)
df_location['city'] = df_location['city'].astype(str).apply(clean_text)
df_location['id'] = df_location['id'].astype(str).str.strip()

# ============================================================
# LOCATION MAPPING LOGIC
# ============================================================
unmatched_rows = []  # store full candidate rows if location not found

def map_location(loc, row):
    """Return location ID if found, else record full candidate row."""
    if pd.isna(loc) or str(loc).strip() == "":
        unmatched_rows.append({**row.to_dict(), "unmatched_location": "blank"})
        return "NA"

    loc_clean = clean_text(loc)

    # Check area
    match_area = df_location[df_location['area'] == loc_clean]
    if not match_area.empty:
        return match_area['id'].values[0]

    # Check city
    match_city = df_location[df_location['city'] == loc_clean]
    if not match_city.empty:
        return match_city['id'].values[0]

    # Not found → store full row (copy ensures unique records)
    unmatched_rows.append({**row.to_dict(), "unmatched_location": loc})
    return "NA"

# Apply mapping (row-wise)
df_main['finploy_id'] = df_main.apply(lambda x: map_location(x.get('location', ''), x), axis=1)

# ============================================================
# SAVE UPDATED MAIN DATASET
# ============================================================
df_main.to_excel(output_file, index=False)
print(f"✅ Phase 2.3 completed successfully. Updated file saved as '{output_file}'.")

# ============================================================
# HANDLE UNMATCHED LOCATIONS (SAVE FULL DETAILS)
# ============================================================
if unmatched_rows:
    df_unmatched_new = pd.DataFrame(unmatched_rows)
    df_unmatched_new.drop_duplicates(inplace=True)
    df_unmatched_new["status"] = "new_location_needed"

    if os.path.exists(additional_file):
        # Append to existing file without duplicates
        df_existing = pd.read_excel(additional_file)
        df_combined = pd.concat([df_existing, df_unmatched_new], ignore_index=True)
        df_combined.drop_duplicates(inplace=True)
        df_combined.to_excel(additional_file, index=False)
        print(f"⚠️ Unmatched location candidates appended to '{additional_file}'.")
    else:
        # Create new file
        df_unmatched_new.to_excel(additional_file, index=False)
        print(f"⚠️ Unmatched location candidates saved to new file '{additional_file}'.")
else:
    print("✅ All locations matched successfully — no unmatched candidates found.")
import subprocess
try:
    import subprocess
    print("▶️ Running main3.py ...")
    subprocess.run(["python", r"D:\matching_harsh\Job_matching_Screened\main3.py"], check=True)
    print("✅ main3.py executed successfully!")
except Exception as e:
    print(f"❌ Failed to run main3.py: {e}")  

