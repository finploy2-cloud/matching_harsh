# ============================================================
# main5.py – Finploy Final Integration (Preserves "name" + all columns safely)
# ============================================================

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import string

# ============================================================
# CONFIGURATION
# ============================================================
OUTPUT_DIR = r"D:\matching_harsh\Screening_Followup\output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

input_file = os.path.join(OUTPUT_DIR, "output3.xlsx")
output_file = os.path.join(OUTPUT_DIR, "output4.xlsx")

SERVICE_ACCOUNT_FILE = (
    r"D:\matching_harsh\Screening_Followup\screeningfollowup-4a463d7d64cb.json"
)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
client = gspread.authorize(creds)

# ============================================================
# LOAD GOOGLE SHEET (Finploy Location Master)
# ============================================================
sheet_id = "11Yye2zMLOgb0J8wBjH0VJNuOV28AAERNPxr3RE2OO-E"
sheet = client.open_by_key(sheet_id).sheet1

headers = sheet.row_values(1)
seen = {}
unique_headers = []
for h in headers:
    h_clean = h.strip()
    if h_clean in seen:
        seen[h_clean] += 1
        h_clean = f"{h_clean}_{seen[h_clean]}"
    else:
        seen[h_clean] = 0
    unique_headers.append(h_clean)

data_rows = sheet.get_all_values()[1:]  # skip header row
df_location = pd.DataFrame(data_rows, columns=unique_headers)
print(f"✅ Loaded location master ({len(df_location)} rows)")

# ============================================================
# ENSURE REQUIRED COLUMNS EXIST
# ============================================================
required_cols = ["area", "city", "state", "city_wise_id", "pincode", "id"]
for col in required_cols:
    if col not in df_location.columns:
        df_location[col] = "0"

for col in ["area", "city", "id", "city_wise_id", "pincode"]:
    df_location[col] = df_location[col].astype(str).str.strip().str.rstrip(string.punctuation)

# ============================================================
# LOAD MAIN DATASET
# ============================================================
df_main = pd.read_excel(input_file)
print(f"✅ Loaded main dataset ({len(df_main)} rows)")

# ============================================================
# RENAME LEGACY COLUMNS
# ============================================================
rename_map = {}
if "link" in df_main.columns:
    rename_map["link"] = "name of candidate"
if "meta-data" in df_main.columns:
    rename_map["meta-data"] = "experience"
df_main.rename(columns=rename_map, inplace=True)

# ============================================================
# MAP LOCATION
# ============================================================
unmatched = []

def map_location(loc):
    loc_clean = str(loc).strip().lower().rstrip(string.punctuation)
    if not loc_clean:
        unmatched.append("blank")
        return pd.Series(["0", "0", "0", "0", "0", "0"])

    match_city = df_location[df_location["city"].str.lower().str.rstrip(string.punctuation) == loc_clean]
    if not match_city.empty:
        row = match_city.iloc[0]
        return pd.Series([row["id"], row["area"], row["city"], row["state"], row.get("city_wise_id", "0"), row.get("pincode", "0")])

    match_area = df_location[df_location["area"].str.lower().str.rstrip(string.punctuation) == loc_clean]
    if not match_area.empty:
        row = match_area.iloc[0]
        return pd.Series([row["id"], row["area"], row["city"], row["state"], row.get("city_wise_id", "0"), row.get("pincode", "0")])

    unmatched.append(loc)
    return pd.Series(["0", "0", "0", "0", "0", "0"])

df_main[["finploy_id", "area", "city", "state", "city_id", "candidate_pincode"]] = df_main["location"].apply(map_location)
print(f"✅ Location mapping complete. Unmatched locations: {len(unmatched)}")

# ============================================================
# FIX DEPARTMENT & PRODUCT
# ============================================================
def fix_dept_prod(x):
    dept = x.get("department", "0")
    prod = x.get("product", "0")

    dept = str(dept).strip() if pd.notna(dept) else "0"
    prod = str(prod).strip() if pd.notna(prod) else "0"

    if not dept or dept.lower() == "nan":
        dept = "0"
    if not prod or prod.lower() == "nan":
        prod = "0"

    try:
        if dept.replace(".", "", 1).isdigit():
            dept = str(int(float(dept)))
        if prod.replace(".", "", 1).isdigit():
            prod = str(int(float(prod)))
    except Exception:
        pass

    return pd.Series([dept, prod])

df_main[["department", "product"]] = df_main.apply(fix_dept_prod, axis=1)

# ============================================================
# GENERATE COMPOSIT KEY
# ============================================================
df_main["city_id"] = df_main["city_id"].replace("", "0").fillna("0")
df_main["composit_key"] = (
    df_main["city_id"].astype(str)
    + "_"
    + df_main["department"].astype(str)
    + "_"
    + df_main["product"].astype(str)
    + "_"
    + df_main["clean_salary"].astype(str)
)

# ============================================================
# ADD CANDIDATE ID
# ============================================================
if "candidate_id" not in df_main.columns:
    df_main.insert(0, "candidate_id", range(1, len(df_main) + 1))
    print(f"✅ Candidate IDs created for {len(df_main)} records.")
else:
    if not pd.api.types.is_numeric_dtype(df_main["candidate_id"]):
        df_main["candidate_id"] = pd.to_numeric(df_main["candidate_id"], errors="coerce")
    missing_ids = df_main["candidate_id"].isna().sum()
    if missing_ids > 0:
        start_val = df_main["candidate_id"].max(skipna=True)
        if pd.isna(start_val):
            start_val = 0
        new_ids = range(int(start_val) + 1, int(start_val) + 1 + missing_ids)
        df_main.loc[df_main["candidate_id"].isna(), "candidate_id"] = new_ids
        print(f"✅ Filled {missing_ids} missing candidate IDs.")
    else:
        print("✅ Existing candidate_id column found — no overwrite performed.")

# ============================================================
# DETECT CONTACT COLUMN
# ============================================================
contact_col = next(
    (c for c in df_main.columns if "contact" in c.lower() or "mobile" in c.lower() or "phone" in c.lower()),
    None,
)
if contact_col:
    print(f"📞 Detected contact column: {contact_col}")
else:
    print("⚠️ No contact column detected.")

# ============================================================
# SELECT FINAL COLUMNS (Keep all but reorder key ones first)
# ============================================================
priority_cols = [
    "candidate_id",
    "name",                    # ✅ keep original 'name'
    "candidate_name",
    "name of candidate",
    contact_col,
    "link href",
    "experience",
    "meta-data 2",
    "location",
    "name_location",
    "employment-detail",
    "designation",
    "company",
    "year",
    "education 2",
    "clean_salary",
    "Modification",
    "Activity",
    "finploy_id",
    "area",
    "city",
    "state",
    "city_id",
    "department",
    "product",
    "composit_key",
    "candidate_pincode",
]

# Only keep valid ones
priority_cols = [c for c in priority_cols if c and c in df_main.columns]

# Keep all other columns as well, without losing anything
remaining_cols = [c for c in df_main.columns if c not in priority_cols]
final_order = priority_cols + remaining_cols

# Reorder DataFrame
df_final = df_main[final_order]

# ============================================================
# SAVE FINAL OUTPUT
# ============================================================
df_final.to_excel(output_file, index=False)
print(f"🎯 Phase 2.5 completed successfully. Final file saved as '{output_file}'")
try:
    import subprocess
    print("▶️ Running main7.py ...")
    subprocess.run(["python", r"D:\matching_harsh\Screening_Followup\main6.py"], check=True)
    print("✅ main7.py executed successfully!")
except Exception as e:
    print(f"❌ Failed to run main7.py: {e}")