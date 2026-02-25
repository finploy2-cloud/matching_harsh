import time
import math
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread.exceptions import APIError
from datetime import datetime
import tkinter as tk

# ======================================================
# CONFIG
# ======================================================
TXT_PATH = r'D:\matching_harsh\Job_matching_Screened\final_input\LIST_1021022601_20260221-135947.txt'
ALL_MATCHES_XLSX = r'D:\matching_harsh\Job_matching_Screened\final_output\all_job_matches\all_job_matches_filtered.xlsx'
SERVICE_ACCOUNT = r'D:\matching_harsh\Job_matching_Screened\service_account.json'

GS_WORKBOOK_NAME = 'Tracker -Candidates'
TAB_SCREENING = 'SCREENING'
TAB_LINEUP = 'LINEUP'

# ======================================================
# HELPERS
# ======================================================
STATUS_MAP_SCREENING = {'NI': 'Not Interested', 'INTSTD': 'Lineup', 'DROP': 'Drop'}
VALID_REMARKS = [
    '#N/A','Call back','Drop','Hold','Ignore','Interested','Lineup',
    'Location Not Available','Not Interested','Remark','Ringing','Switchoff','Duplicate'
]

def norm(h):
    return h.lower().replace(" ", "").replace("_", "")

def extract_date_time(value):
    try:
        dt = pd.to_datetime(value, errors="coerce")
        if pd.isna(dt):
            dt = datetime.now()
    except:
        dt = datetime.now()
    return dt.strftime("%d-%m-%Y"), dt.strftime("%H:%M:%S")

def map_rec_screening(user):
    if pd.isna(user): return ""
    u = str(user).strip().upper()
    if u.startswith("COMP"): u = u.replace("COMP","")
    return {'4':'Soham','3':'Antara','9':'Shraddha','5':'Nandhini','VDAD':''}.get(u,u)

def map_rec_lineup(user):
    if user is None: return ""
    u = str(user).strip().upper()
    return {
        "COMP9":"Shraddha","COMP4":"Soham","COMP3":"Antara",
        "COMP5":"Nandhini","COMP2":"Omkar","VDAD":""
    }.get(u, map_rec_screening(u))

def map_remark_screening(status):
    if pd.isna(status): return "Ringing"
    s = str(status).strip().upper()
    mapped = STATUS_MAP_SCREENING.get(s,"Ringing")
    return mapped if mapped in VALID_REMARKS else "Ringing"

def sanitize_rows_for_gs(rows, headers, label):
    cleaned = []
    for r_idx, row in enumerate(rows, start=1):
        buf = []
        for c_idx, v in enumerate(row):
            bad = (
                v is None or
                (isinstance(v,float) and (math.isnan(v) or math.isinf(v))) or
                (isinstance(v,str) and v.lower() in ("nan","inf","-inf"))
            )
            if bad:
                buf.append("")
            else:
                buf.append(str(v))
        cleaned.append(buf)
    return cleaned

def safe_api(fn,*a,**k):
    for _ in range(5):
        try:
            return fn(*a,**k)
        except APIError as e:
            if "429" in str(e) or "503" in str(e):
                time.sleep(2)
            else:
                raise

def col_idx_to_letter(idx):
    """Convert column index (0-based) to column letter (A, B, ..., Z, AA, AB, ...)"""
    result = ""
    idx += 1  # Convert to 1-based
    while idx > 0:
        idx -= 1
        result = chr(65 + (idx % 26)) + result
        idx //= 26
    return result

# ======================================================
# LOAD DATA
# ======================================================
txt_df = pd.read_csv(TXT_PATH, sep="\t", dtype=str, engine="python")
txt_df.columns = txt_df.columns.str.strip().str.lower()

matches_df = pd.read_excel(ALL_MATCHES_XLSX, dtype=str)
matches_df.columns = matches_df.columns.str.strip().str.lower()

merged_df = matches_df.merge(
    txt_df,
    left_on="clean_phone",
    right_on="phone_number",
    how="left"
)

# ======================================================
# GOOGLE AUTH
# ======================================================
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]
gc = gspread.authorize(
    ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT, scope)
)

ws_screening = gc.open(GS_WORKBOOK_NAME).worksheet(TAB_SCREENING)
ws_lineup = gc.open(GS_WORKBOOK_NAME).worksheet(TAB_LINEUP)

# ======================================================
# SCREENING (UNCHANGED LOGIC)
# ======================================================
screen_values = ws_screening.get_all_values()
headers = screen_values[0]
col_idx = {h:i for i,h in enumerate(headers)}
screen_df = pd.DataFrame(screen_values[1:], columns=headers)

new_rows = []
updated_indices = set()
unique_candidates = merged_df.drop_duplicates("clean_phone", keep="last")

for _, row in unique_candidates.iterrows():
    phone = str(row.get("clean_phone","")).strip()
    if not phone:
        continue

    entry_date_str, entry_time_str = extract_date_time(row.get("entry_date"))
    rec = map_rec_screening(row.get("user"))
    remark = map_remark_screening(row.get("status"))
    comment = str(row.get("comments","") or "").strip()
    name = str(row.get("name of candidate","")).strip()
    location = str(row.get("location","")).strip()

    matches = (
        screen_df.index[screen_df["Contact"].astype(str).str.strip()==phone].tolist()
        if "Contact" in screen_df.columns else []
    )

    if matches:
        # Entry exists - UPDATE all values except Contact column
        match_idx = matches[-1]
        for col_name in headers:
            if col_name == "Contact":
                # Don't update Contact column
                continue
            elif col_name == "candidate_id":
                # Keep existing candidate_id
                continue
            elif col_name == "Date":
                screen_df.at[match_idx, col_name] = entry_date_str
            elif col_name == "Rec":
                screen_df.at[match_idx, col_name] = rec
            elif col_name == "Remark":
                screen_df.at[match_idx, col_name] = remark
            elif col_name == "Comment":
                screen_df.at[match_idx, col_name] = comment
            elif col_name == "Computer_Time":
                screen_df.at[match_idx, col_name] = entry_time_str
            elif col_name == "Name":
                screen_df.at[match_idx, col_name] = name
            elif col_name == "Location":
                screen_df.at[match_idx, col_name] = location
        
        updated_indices.add(match_idx)
    else:
        # Entry doesn't exist - CREATE new row
        next_id = int(pd.to_numeric(
            screen_df.get("candidate_id",pd.Series()),
            errors="coerce"
        ).max() or 0) + 1

        buf = [""]*len(headers)

        def put(h,v):
            if h in col_idx:
                buf[col_idx[h]] = v

        put("candidate_id",str(next_id))
        put("Date",entry_date_str)
        put("Rec",rec)
        put("Contact",phone)
        put("Remark",remark)
        put("Comment",comment)
        put("Computer_Time",entry_time_str)
        put("Name",name)
        put("Location",location)

        new_rows.append(buf)


# Handle updated rows - update them in Google Sheets
if updated_indices:
    for idx in updated_indices:
        # Get the row number in the sheet (add 2 because sheet starts at row 1, and row 1 is headers)
        row_number = idx + 2
        updated_row = screen_df.iloc[idx].tolist()
        updated_row = sanitize_rows_for_gs([updated_row], headers, "SCREENING")[0]
        # Calculate the correct column range based on number of headers
        end_col = col_idx_to_letter(len(headers) - 1)
        range_name = f"A{row_number}:{end_col}{row_number}"
        safe_api(ws_screening.update, values=[updated_row], range_name=range_name, value_input_option="USER_ENTERED")

# Handle new rows - append them to Google Sheets
if new_rows:
    new_rows = sanitize_rows_for_gs(new_rows, headers, "SCREENING")
    safe_api(ws_screening.append_rows, new_rows, value_input_option="USER_ENTERED")

# ======================================================
# LINEUP (FULL MAPPING FIXED)
# ======================================================
line_values = ws_lineup.get_all_values()
line_headers = line_values[0]
line_idx = {norm(h):i for i,h in enumerate(line_headers)}

line_df = pd.DataFrame(line_values[1:], columns=line_headers)
candidate_series = pd.to_numeric(line_df.get("candidate_id"), errors="coerce")
lineup_max_id = int(candidate_series.max()) if candidate_series.notna().any() else 0

intstd_df = txt_df[txt_df["status"].str.strip().str.lower()=="intstd"]
lineup_merge = matches_df.merge(
    intstd_df,
    left_on="clean_phone",
    right_on="phone_number",
    how="inner"
)

line_new_rows = []

for _, r in lineup_merge.iterrows():
    lineup_max_id += 1
    l_date_str, l_time_str = extract_date_time(r.get("entry_date"))
    buf = [""]*len(line_headers)

    def put(h,v):
        k = norm(h)
        if k in line_idx:
            buf[line_idx[k]] = v

    put("candidate_id",str(lineup_max_id))
    put("Date",l_date_str)
    put("Computer_Time",l_time_str)
    put("HR",r.get("job_hr_name",""))
    put("Recruiter",map_rec_lineup(r.get("user")))
    put("Role",r.get("job_designation",""))
    put("Company applied",r.get("job_company",""))
    put("Location",r.get("location",""))
    put("Name",r.get("name of candidate",""))
    put("Contact",r.get("clean_phone",""))
    put("Curr Salary",r.get("clean_salary",""))
    put("Current Company",r.get("company",""))
    put("Current Designation",r.get("designation",""))
    put("Comment",r.get("comments",""))
    put("Status","Lineup")
    put("name_location",r.get("name_location",""))
    put("finploy_loc_id",r.get("finploy_id",""))
    put("finploy_city_id",r.get("city_id",""))
    put("PRODUCT",r.get("product",""))
    put("DEPARTMENT",r.get("department",""))
    put("PINCODE",r.get("candidate_pincode",""))
    put("Manual/Computer","Computer")
    put("Job_id",r.get("job_id",""))
    put("Experience",r.get("experience",""))
    put("Education",r.get("education 2",""))

    line_new_rows.append(buf)

if line_new_rows:
    line_new_rows = sanitize_rows_for_gs(line_new_rows, line_headers, "LINEUP")
    safe_api(ws_lineup.append_rows, line_new_rows, value_input_option="USER_ENTERED")

# ======================================================
# SUMMARY
# ======================================================
print("SCREENING appended:", len(new_rows))
print("LINEUP appended:", len(line_new_rows))

tk.Tk().mainloop()