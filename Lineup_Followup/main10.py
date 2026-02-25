import os
import time
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread.exceptions import APIError
import tkinter as tk
from tkinter import scrolledtext
from datetime import datetime

# ======================================================
# CONFIG
# ======================================================
TXT_PATH = r'D:\matching_harsh\Lineup_Followup\input\LIST_1019122504_20251219-154641.txt'
ALL_MATCHES_XLSX = r"D:\matching_harsh\Lineup_Followup\final_output\all_job_matches.xlsx\filtered_candidates.xlsx"

GS_WORKBOOK_NAME = 'Tracker -Candidates'
TAB_SCREENING = 'SCREENING'
TAB_LINEUP = 'Lineup'

SCREENING_LAST_COL = "AT"
LINEUP_LAST_COL    = "BK"

SERVICE_JSON = r'D:\matching_harsh\Lineup_Followup\screeningfollowup-4a463d7d64cb.json'


# ======================================================
# HELPERS
# ======================================================
def clean_val(v):
    if v is None:
        return ""
    if isinstance(v, float):
        if pd.isna(v) or v == float("inf") or v == float("-inf"):
            return ""
    v = str(v).strip()
    if v.lower() == "nan":
        return ""
    return v

VALID_REMARKS = [
    '#N/A', 'Call back', 'Drop', 'Hold', 'Ignore',
    'Interested', 'Lineup', 'Location Not Available',
    'Not Interested', 'Remark', 'Ringing', 'Switchoff', 'Duplicate'
]

STATUS_MAP_SCREENING = {'NI': 'Not Interested', 'INTSTD': 'Lineup', 'DROP': 'Drop'}

def map_rec_screening(user):
    if pd.isna(user): return ""
    user = str(user).strip().upper()
    if user.startswith('COMP'): user = user.replace('COMP', '')
    return {'4': 'Soham', '3': 'Antara', '9': 'Shraddddha', '5': 'Nandhini', 'VDAD': ''}.get(user, user)

def map_rec_lineup(rec):
    if rec is None: return ""
    rec = str(rec).strip().upper()
    if rec == "COMP9": return "Shraddha"
    if rec == "COMP4": return "Soham"
    if rec == "COMP3": return "Antara"
    if rec == "COMP5": return "Nandhini"
    if rec == "VDAD":  return ""
    return map_rec_screening(rec)

def map_remark_screening(status):
    if pd.isna(status): return 'Ringing'
    s = str(status).strip().upper()
    mapped = STATUS_MAP_SCREENING.get(s, 'Ringing')
    return mapped if mapped in VALID_REMARKS else 'Ringing'

def extract_date_time(value):
    val = clean_val(value)
    fmts = [
        "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S", "%d-%m-%Y %H:%M:%S",
        "%d/%m/%Y %H:%M:%S", "%d-%m-%Y %H:%M",
        "%d/%m/%Y %H:%M"
    ]
    for f in fmts:
        try:
            dt = datetime.strptime(val, f)
            return dt.strftime("%d-%m-%Y"), dt.strftime("%H:%M:%S")
        except:
            pass

    for f in ["%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"]:
        try:
            d = datetime.strptime(val, f)
            return d.strftime("%d-%m-%Y"), "00:00:00"
        except:
            pass

    now = datetime.now()
    return now.strftime("%d-%m-%Y"), now.strftime("%H:%M:%S")


# ======================================================
# CONNECT GOOGLE SHEETS
# ======================================================
def gauth():
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_JSON, scope)
    return gspread.authorize(creds)

def open_ws(gc, workbook, title):
    wb = gc.open(workbook)
    for ws in wb.worksheets():
        if ws.title.strip().casefold() == title.strip().casefold():
            return ws
    raise ValueError(f"Worksheet '{title}' not found")


# ======================================================
# LOAD FILES
# ======================================================
txt_df = pd.read_csv(TXT_PATH, sep="\t", dtype=str, engine="python")
txt_df.columns = txt_df.columns.str.lower()

matches_df = pd.read_excel(ALL_MATCHES_XLSX, dtype=str)
matches_df.columns = matches_df.columns.str.lower()

if "clean_phone" not in matches_df.columns:
    matches_df["clean_phone"] = matches_df["contact"].astype(str).str.strip()

merged_df = matches_df.merge(
    txt_df[['phone_number','status','user','comments','entry_date',
            'last_name','address1','address2','address3','city']],
    left_on="clean_phone",
    right_on="phone_number",
    how="left"
)


# ======================================================
# OPEN SHEETS
# ======================================================
gc = gauth()
ws_scr = open_ws(gc, GS_WORKBOOK_NAME, TAB_SCREENING)
ws_lin = open_ws(gc, GS_WORKBOOK_NAME, TAB_LINEUP)

scr_values = ws_scr.get_all_values()
scr_header = scr_values[0]

lin_values = ws_lin.get_all_values()
lin_header = lin_values[0]


# ======================================================
# SCREENING WITH 30-DAY LOGIC
# ======================================================
screen_updates = []
screen_appends = []

existing_df = pd.DataFrame(scr_values[1:], columns=scr_header)

unique = merged_df.drop_duplicates("clean_phone", keep="last")

for _, r in unique.iterrows():
    phone = clean_val(r.get("contact"))
    if phone == "":
        continue

    matched = existing_df.index[
        existing_df["Contact"].astype(str).str.strip() == phone
    ].tolist()

    date_str, time_str = extract_date_time(r.get("entry_date"))

    do_update = False
    do_append = False

    if matched:
        latest_idx = matched[-1]
        last_row_df = existing_df.iloc[latest_idx]

        last_date_sheet = clean_val(last_row_df.get("Date"))

        try:
            last_dt = datetime.strptime(last_date_sheet, "%d-%m-%Y")
        except:
            last_dt = datetime.min

        days_diff = (datetime.now() - last_dt).days

        if days_diff <= 30:
            do_update = True
        else:
            do_append = True

    else:
        do_append = True

    # ---------------------------------------------------
    # PERFORM UPDATE
    # ---------------------------------------------------
    if do_update:
        rownum = matched[-1] + 2
        original_row = scr_values[rownum - 2].copy()

        def put(col, val):
            if col in scr_header:
                original_row[scr_header.index(col)] = clean_val(val)

        put("Date", date_str)
        put("Rec", map_rec_screening(r.get("user")))
        put("Contact", phone)
        put("Remark", map_remark_screening(r.get("status")))
        put("Comment", r.get("comments"))
        put("Location", r.get("location"))
        put("Name", r.get("name"))
        put("Salary", r.get("clean_salary"))
        put("Current Company", r.get("current company"))
        put("Current Designation", r.get("current designation"))
        put("name_location", r.get("name_location"))
        put("finploy_id", r.get("finploy_id"))
        put("finploy_city_id", r.get("city_id"))
        put("finploy_city", r.get("city"))
        put("Composit_key", r.get("address1"))
        put("Manual/Computer", "Half Computer")
        put("Experience", r.get("experience"))
        put("Education", r.get("education 2"))
        put("Graduation_year", r.get("graduation_year"))
        put("Computer_Time", time_str)

        rng = f"A{rownum}:{SCREENING_LAST_COL}{rownum}"
        screen_updates.append({"range": rng, "values": [original_row]})

    # ---------------------------------------------------
    # PERFORM APPEND
    # ---------------------------------------------------
    if do_append:
        new_row = ["" for _ in scr_header]

        next_id = int(pd.to_numeric(
            existing_df.get('candidate_id', pd.Series([])),
            errors='coerce'
        ).max() or 0) + 1

        if "candidate_id" in scr_header:
            new_row[scr_header.index("candidate_id")] = str(next_id)

        def put(col, val):
            if col in scr_header:
                new_row[scr_header.index(col)] = clean_val(val)

        put("Date", date_str)
        put("Rec", map_rec_screening(r.get("user")))
        put("Contact", phone)
        put("Remark", map_remark_screening(r.get("status")))
        put("Comment", r.get("comments"))
        put("Location", r.get("location"))
        put("Name", r.get("name"))
        put("Salary", r.get("clean_salary"))
        put("Current Company", r.get("current company"))
        put("Current Designation", r.get("current designation"))
        put("name_location", r.get("name_location"))
        put("finploy_id", r.get("finploy_id"))
        put("finploy_city_id", r.get("city_id"))
        put("finploy_city", r.get("city"))
        put("Composit_key", r.get("address1"))
        put("Manual/Computer", "Half Computer")
        put("Experience", r.get("experience"))
        put("Education", r.get("education 2"))
        put("Graduation_year", r.get("graduation_year"))
        put("Computer_Time", time_str)

        screen_appends.append(new_row)



# ======================================================
# APPLY SCREENING UPDATES
# ======================================================
if screen_updates:
    ws_scr.batch_update(screen_updates)

if screen_appends:
    ws_scr.append_rows(screen_appends, value_input_option="USER_ENTERED")



# ======================================================
# LINEUP SECTION — UNCHANGED
# ======================================================
line_rows = []

intstd = txt_df[txt_df["status"].astype(str).str.lower() == "intstd"]

line_df = matches_df.merge(intstd, left_on="clean_phone", right_on="phone_number", how="inner")
line_df = line_df.drop_duplicates(subset=["clean_phone","job_id","job_company"])

for _, r in line_df.iterrows():
    d,t = extract_date_time(r.get("entry_date"))

    new_row = ["" for _ in lin_header]

    def put(col, val):
        if col in lin_header:
            new_row[lin_header.index(col)] = clean_val(val)

    put("Date", d)
    put("Computer_Time", t)
    put("HR", r.get("job_hr_name"))
    put("Recruiter", map_rec_lineup(r.get("user")))
    put("Role", r.get("job_designation"))
    put("Company applied", r.get("job_company"))
    put("Location", r.get("location"))
    put("Name", r.get("name"))
    put("Contact", r.get("clean_phone"))
    put("Curr Salary", r.get("clean_salary"))
    put("Current Company", r.get("current company"))
    put("Current Designation", r.get("current designation"))
    put("Comment", r.get("comments"))
    put("Status", "Lineup")
    put("name_location", r.get("name_location"))
    put("finploy_loc_id", r.get("finploy_id"))
    put("finploy_city_id", r.get("city_id"))
    put("PRODUCT", r.get("product"))
    put("DEPARTMENT", r.get("department"))
    put("PINCODE", r.get("candidate_pincode"))
    put("Manual/Computer", "Computer")
    put("Job_id", r.get("job_id"))
    put("Experience", r.get("experience"))
    put("Education", r.get("education 2"))

    line_rows.append(new_row)

if line_rows:
    ws_lin.append_rows(line_rows, value_input_option="USER_ENTERED")



# ======================================================
# SUMMARY TKINTER WINDOW
# ======================================================
root = tk.Tk()
root.title("Summary")
root.geometry("900x650")

tk.Label(root, text="✔ Final Update Summary", font=("Segoe UI",16,"bold")).pack()
tk.Label(root, text=f"SCREENING: Updated {len(screen_updates)} | Appended {len(screen_appends)}",
         font=("Segoe UI",12,"bold"), fg="blue").pack()
tk.Label(root, text=f"LINEUP: Appended {len(line_rows)}",
         font=("Segoe UI",12,"bold"), fg="green").pack(pady=(0,10))

root.mainloop()
