# ============================================================
# main10_fixed_city_first.py  (Finploy Location Mapping - SMART Version)
# ============================================================

import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import re
import subprocess

# ============================================================
# CONFIGURATION
# ============================================================
main_file = r"D:\matching_harsh\Job_matching_unscreened\output\output2.xlsx"
OUTPUT_DIR = r"D:\matching_harsh\Job_matching_unscreened\output"

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

output_file = os.path.join(OUTPUT_DIR, "output3.xlsx")
additional_file = os.path.join(OUTPUT_DIR, "additional_new_location.xlsx")

# ============================================================
# GOOGLE SHEET SETUP
# ============================================================
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]
creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)
client = gspread.authorize(creds)

# Open Finploy Location Master sheet
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
    value = str(value).lower().strip()
    value = re.sub(r'[.,;:]+$', '', value)
    return value

df_location['area'] = df_location['area'].astype(str).apply(clean_text)
df_location['city'] = df_location['city'].astype(str).apply(clean_text)
df_location['id'] = df_location['id'].astype(str).str.strip()

# ============================================================
# SMART LOCATION CLEANING + MULTI-CITY MATCHING
# ============================================================

GENERIC_WORDS = [
    "district", "city", "and", "dist", "mandal", "taluka", "village",
    "nagar", "gram", "junction", "jn", "jct", "town"
]

def clean_location_part(text):
    """Removes generic words & unwanted tokens."""
    text = text.lower().strip()

    # Remove generic words
    for bad in GENERIC_WORDS:
        text = re.sub(rf"\b{bad}\b", "", text)

    # Remove punctuation
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    return text


def split_location(loc):
    """Splits input into multiple possible city candidates."""
    loc = str(loc).lower()

    # Split on common delimiters
    parts = re.split(r",|/|-|&| and ", loc)

    cleaned_parts = []
    for part in parts:
        c = clean_location_part(part)
        if c:
            cleaned_parts.append(c)

    return cleaned_parts


unmatched_rows = []


def map_location(loc, row):
    """Return Finploy city ID based on smart matching."""
    if pd.isna(loc) or str(loc).strip() == "":
        unmatched_rows.append({**row.to_dict(), "unmatched_location": "blank"})
        return "NA"

    # Extract multiple city possibilities
    loc_parts = split_location(loc)

    # 1️⃣ CITY MATCH FIRST
    for city in loc_parts:
        match = df_location[df_location['city'] == city]
        if not match.empty:
            return match['id'].values[0]

    # 2️⃣ AREA MATCH SECOND
    for city in loc_parts:
        match = df_location[df_location['area'] == city]
        if not match.empty:
            return match['id'].values[0]

    # 3️⃣ Not found → store full row
    unmatched_rows.append({**row.to_dict(), "unmatched_location": loc})
    return "NA"


# ============================================================
# APPLY LOCATION MAPPING
# ============================================================
df_main['finploy_id'] = df_main.apply(lambda x: map_location(x.get('location', ''), x), axis=1)

# ============================================================
# SAVE UPDATED MAIN DATASET
# ============================================================
df_main.to_excel(output_file, index=False)
print(f"✅ Mapping completed successfully! File saved → {output_file}")

# ============================================================
# HANDLE UNMATCHED LOCATIONS
# ============================================================
if unmatched_rows:
    df_unmatched_new = pd.DataFrame(unmatched_rows).drop_duplicates()
    df_unmatched_new["status"] = "new_location_needed"

    if os.path.exists(additional_file):
        df_existing = pd.read_excel(additional_file)
        df_combined = pd.concat([df_existing, df_unmatched_new], ignore_index=True).drop_duplicates()
        df_combined.to_excel(additional_file, index=False)
        print(f"⚠️ Unmatched locations appended → {additional_file}")
    else:
        df_unmatched_new.to_excel(additional_file, index=False)
        print(f"⚠️ Unmatched locations saved → {additional_file}")
else:
    print("✅ All locations matched! No unmatched candidates found.")

# ============================================================
# RUN NEXT SCRIPT
# ============================================================
try:
    print("▶️ Running main12.py ...")
    subprocess.run(["python", r"D:\matching_harsh\Job_matching_unscreened\main12.py"], check=True)
    print("✅ main12.py executed successfully!")
except Exception as e:
    print(f"❌ Failed to run main12.py: {e}")
