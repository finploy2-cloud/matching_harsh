import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import os

# ============================================================
# CONFIGURATION
# ============================================================
input_excel_path = r"D:\matching_harsh\Job_matching_Screened\final_output\all_job_matches\all_job_matches_duplicate.xlsx"
credentials_path = r"D:\matching_harsh\Job_matching_Screened\service_account.json"

google_sheet_name = "Tracker -Candidates"
worksheet_name = "SCREENING"

# ============================================================
# GOOGLE SHEET AUTHENTICATION
# ============================================================
scope = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds = Credentials.from_service_account_file(credentials_path, scopes=scope)
client = gspread.authorize(creds)

sheet = client.open(google_sheet_name)
worksheet = sheet.worksheet(worksheet_name)

# ============================================================
# READ DATA FROM GOOGLE SHEET
# ============================================================
tracker_data = worksheet.get_all_records()
tracker_df = pd.DataFrame(tracker_data)

required_columns = ["name_location", "Date", "Remark", "Contact"]
for col in required_columns:
    if col not in tracker_df.columns:
        raise KeyError(f"❌ Column '{col}' not found in Google Sheet.")

# ============================================================
# HELPER: ROBUST DATE PARSING
# ============================================================
def parse_date_robust(val):
    if pd.isna(val) or str(val).strip() == "":
        return pd.NaT
    
    val_str = str(val).strip()
    # Common formats to try
    formats = [
        "%d-%m-%Y", "%d/%m/%Y",  # DD-MM-YYYY, DD/MM/YYYY
        "%Y-%m-%d", "%Y/%m/%d",  # ISO
        "%m-%d-%Y", "%m/%d/%Y",  # US (Fallback)
        "%d-%m-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S" # With time
    ]
    
    for fmt in formats:
        try:
            return pd.to_datetime(val_str, format=fmt)
        except (ValueError, TypeError):
            continue
            
    return pd.NaT

# Normalize string columns
tracker_df["name_location"] = tracker_df["name_location"].astype(str).str.strip()
tracker_df["Remark"] = tracker_df["Remark"].astype(str).fillna("").str.strip()

# Apply robust date parsing
tracker_df["Date"] = tracker_df["Date"].apply(parse_date_robust)

# ============================================================
# LOAD INPUT EXCEL FILE
# ============================================================
if not os.path.exists(input_excel_path):
    raise FileNotFoundError(f"❌ Input file not found at: {input_excel_path}")

input_df = pd.read_excel(input_excel_path)

if 'name_location' not in input_df.columns:
    raise KeyError("❌ 'name_location' column not found in input Excel file.")

# Preserve original column order for later
input_columns = list(input_df.columns)

# Normalize for matching
input_df["name_location"] = input_df["name_location"].astype(str).str.strip()

# ============================================================
# FIND LATEST ENTRY AND APPLY FILTER LOGIC
# ============================================================
def get_latest_valid_entry(row):
    """
    Find latest SCREENING entry by name_location.
    1. Match by name_location.
    2. If found, retrieve 'Contact' (Phone) and 'Date'.
    3. Apply 90-day Rule:
       - IF (Within 90 Days) AND (Remark in ['not interested', 'drop']) -> BLOCK
       - ELSE -> KEEP
    4. Return the retrieved Phone as 'clean_phone'.
    """
    # Use name_location as the lookup key since input phone might be empty
    name_loc = str(row.get("name_location", "")).strip()
    
    if not name_loc or name_loc == "nan":
        # Cannot match without name_location, keep as is (likely new/unknown)
        return pd.Series({"clean_phone": row.get("clean_phone", ""), "remark": "", "keep": True})

    # Filter Tracker for this name_location
    matches = tracker_df.loc[tracker_df["name_location"] == name_loc]
    
    if matches.empty:
        # No history found, keep candidate
        return pd.Series({"clean_phone": row.get("clean_phone", ""), "remark": "", "keep": True})

    # Get latest entry by Date
    latest_row = matches.sort_values(by="Date", ascending=True, na_position='first').iloc[-1]
    
    latest_date = latest_row["Date"]
    remark = latest_row["Remark"] if pd.notna(latest_row["Remark"]) else ""
    contact = latest_row["Contact"] if pd.notna(latest_row["Contact"]) else ""
    
    # Use the Contact from Google Sheet if input doesn't have it
    final_phone = str(contact).strip()
    if not final_phone:
         final_phone = str(row.get("clean_phone", "")).strip()

    keep = True
    remark_lower = str(remark).lower().strip()
    days_diff = None

    if pd.notna(latest_date):
        days_diff = (datetime.now() - latest_date).days
    
    # THE CORE RULE: WITHIN 90 DAYS + NEGATIVE REMARK -> BLOCK
    if days_diff is not None and days_diff <= 90:
        if remark_lower in ["not interested", "drop" , "lineup"]:
            keep = False
            
    return pd.Series({"clean_phone": final_phone, "remark": remark, "keep": keep})

# ============================================================
# APPLY LOGIC TO EACH CANDIDATE
# ============================================================
results = input_df.apply(get_latest_valid_entry, axis=1)

# We update the original dataframe with the found data
input_df["clean_phone"] = results["clean_phone"] # FILL EMPTY PHONE
input_df["remark"] = results["remark"]
input_df["keep"] = results["keep"]

# Filter only valid rows
filtered_df = input_df[input_df["keep"] == True].drop(columns=["keep"])

# ============================================================
# SAVE OUTPUT FILES (Ensure filtered keeps input columns + enriched cols)
# ============================================================
output_dir = os.path.dirname(input_excel_path)
filtered_path = os.path.join(output_dir, "all_job_matches_filtered.xlsx")
unique_path = os.path.join(output_dir, "all_job_matches_unique_phone.xlsx")

# Ensure filtered_df includes all original input columns PLUS clean_phone & remark from SCREENING
filtered_columns = input_columns.copy()
for extra in ["clean_phone", "remark"]:
    if extra not in filtered_columns:
        filtered_columns.append(extra)

# Reindex to enforce column presence and order
filtered_df = filtered_df.reindex(columns=filtered_columns)

# Save filtered file
filtered_df.to_excel(filtered_path, index=False)

# Save unique file (based on clean_phone)
if "clean_phone" in filtered_df.columns:
    unique_df = filtered_df.drop_duplicates(subset=["clean_phone"], keep="first")
    unique_df.to_excel(unique_path, index=False)
else:
    print("⚠️ 'clean_phone' column not found — unique file not created.")
    unique_df = pd.DataFrame()

# ============================================================
# DONE
# ============================================================
print("✅ Processing complete!")
print(f"📁 Filtered output saved at: {filtered_path}")
if not unique_df.empty:
    print(f"📁 Unique phone output saved at: {unique_path}")
import subprocess
try:
    import subprocess
    print("▶️ Running main7.py ...")
    subprocess.run(["python", r"D:\matching_harsh\Job_matching_Screened\main7.py"], check=True)
    print("✅ main7.py executed successfully!")
except Exception as e:
    print(f"❌ Failed to run main7.py: {e}")