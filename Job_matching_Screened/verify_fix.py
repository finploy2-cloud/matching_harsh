import pandas as pd
from datetime import datetime

# Mock Date for Verification (Today = 2026-01-20)
mock_now = datetime(2026, 1, 20)

# Helper copied from main6.py
def parse_date_robust(val):
    if pd.isna(val) or str(val).strip() == "":
        return pd.NaT
    
    val_str = str(val).strip()
    formats = [
        "%d-%m-%Y", "%d/%m/%Y",  # DD-MM-YYYY, DD/MM/YYYY
        "%Y-%m-%d", "%Y/%m/%d",  # ISO
        "%m-%d-%Y", "%m/%d/%Y",  # US (Fallback)
        "%d-%m-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"
    ]
    
    for fmt in formats:
        try:
            return pd.to_datetime(val_str, format=fmt)
        except (ValueError, TypeError):
            continue
            
    return pd.NaT

def check_logic_name_based(row, mock_tracker_df):
    name_loc = row.get("name_location")
    phone_in = row.get("clean_phone")
    
    matches = mock_tracker_df.loc[mock_tracker_df["name_location"] == name_loc]
    
    if matches.empty:
        return True, "No History -> Keep", phone_in

    latest_row = matches.sort_values(by="Date_Parsed", ascending=True).iloc[-1]
    latest_date = latest_row["Date_Parsed"]
    remark = latest_row["Remark"]
    contact = latest_row["Contact"]
    
    # Enrich Phone
    final_phone = str(contact) if pd.notna(contact) else str(phone_in)
    
    days_diff = None
    if pd.notna(latest_date):
        days_diff = (mock_now - latest_date).days

    # Logic: Block if <= 90 days AND (Not Interested OR Drop)
    if days_diff is not None and days_diff <= 90:
        if str(remark).lower() in ["not interested", "drop"]:
            return False, f"Blocked ({remark})", final_phone
            
    return True, "Kept", final_phone

# Tracker Data (The "Database")
tracker_data = [
    {"name_location": "User_Recent_Fail", "Date": "19-01-2026", "Remark": "Not Interested", "Contact": "999-FILLED-FROM-DB"},
    {"name_location": "User_Old_Pass", "Date": "20-09-2025", "Remark": "Not Interested", "Contact": "888-FILLED-FROM-DB"},
]
tracker_df = pd.DataFrame(tracker_data)
tracker_df["Date_Parsed"] = tracker_df["Date"].apply(parse_date_robust)

# Input Data (Simulating Excel with empty phones)
input_rows = [
    {"name_location": "User_Recent_Fail", "clean_phone": ""},
    {"name_location": "User_Old_Pass", "clean_phone": ""},
    {"name_location": "Unknown_User", "clean_phone": "000-NEW-USER"},
]

print("\n--- Testing Name Lookup + Phone Enrichment + 90 Day Rule ---")
for row in input_rows:
    keep, reason, phone = check_logic_name_based(row, tracker_df)
    print(f"Name: {row['name_location']} | Phone Out: {phone} | Result: {keep} ({reason})")
