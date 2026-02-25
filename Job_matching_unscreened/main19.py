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
# CONFIGURATION
# ======================================================
TXT_PATH = r'D:\matching_harsh\Job_matching_unscreened\final_input\LIST_1024022606_20260224-172436.txt'
ALL_MATCHES_XLSX = r"D:\matching_harsh\Job_matching_unscreened\final_output\all_job_matches_phone.xlsx"

GS_WORKBOOK_NAME = 'Tracker -Candidates'
TAB_SCREENING = 'SCREENING'
TAB_LINEUP = 'Lineup'

# ======================================================
# HELPERS
# ======================================================
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
    return {'4': 'Soham', '3': 'Antara', '9': 'Shraddha', '5': 'Nandhini','VDAD': '','Omkar': '2'}.get(user, user)

def map_rec_lineup(rec):
    if rec is None: return ""
    rec = str(rec).strip().upper()
    if rec == "COMP9": return "Shraddha"
    elif rec == "COMP4": return "Soham"
    elif rec == "COMP3": return "Antara"
    elif rec == "COMP5": return "Nandhini"
    elif rec == "COMP2": return "Omkar"
    elif rec == "VDAD": return ""
    return map_rec_screening(rec)

def map_remark_screening(status):
    if pd.isna(status): return 'Ringing'
    s = str(status).strip().upper()
    mapped = STATUS_MAP_SCREENING.get(s, 'Ringing')
    return mapped if mapped in VALID_REMARKS else 'Ringing'

def extract_date_time(value):
    val = (str(value).strip() if value is not None else "")
    fmts = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%S",
            "%d-%m-%Y %H:%M:%S", "%d/%m/%Y %H:%M:%S", "%d-%m-%Y %H:%M", "%d/%m/%Y %H:%M")
    for f in fmts:
        try:
            dt = datetime.strptime(val, f)
            return dt.strftime("%d-%m-%Y"), dt.strftime("%H:%M:%S")
        except Exception:
            pass

    for f in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            d = datetime.strptime(val, f)
            return d.strftime("%d-%m-%Y"), "00:00:00"
        except:
            pass

    now = datetime.now()
    return now.strftime("%d-%m-%Y"), now.strftime("%H:%M:%S")

def safe_api_call(func, *args, **kwargs):
    wait_time = 2
    for attempt in range(6):
        try:
            return func(*args, **kwargs)
        except APIError as e:
            if "429" in str(e) or "503" in str(e):
                print(f"⚠️ API limit hit, waiting {wait_time}s...")
                time.sleep(wait_time)
                wait_time *= 2
            else:
                raise
    print("❌ Failed after retries.")
    return None

def open_ws(gc, workbook, title):
    wb = gc.open(workbook)
    for ws in wb.worksheets():
        if ws.title.strip().casefold() == title.strip().casefold():
            return ws
    raise ValueError(f"Worksheet '{title}' not found in '{workbook}'")

# ======================================================
# LOAD FILES
# ======================================================
txt_df = pd.read_csv(TXT_PATH, sep="\t", dtype=str, encoding="utf-8", engine="python")
txt_df.columns = txt_df.columns.str.strip().str.lower()

matches_df = pd.read_excel(ALL_MATCHES_XLSX, dtype=str)
matches_df.columns = matches_df.columns.str.strip().str.lower()

merged_df = matches_df.merge(
    txt_df[['phone_number', 'status', 'user', 'comments', 'entry_date', 'last_name',
            'address1', 'address2', 'address3', 'city']],
    left_on='clean_phone', right_on='phone_number', how='left'
)

# ======================================================
# GOOGLE AUTH
# ======================================================
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name(
    r'D:\matching_harsh\Job_matching_unscreened\service_account.json', scope
)
gc = gspread.authorize(creds)
ws_screening = open_ws(gc, GS_WORKBOOK_NAME, TAB_SCREENING)
ws_lineup = open_ws(gc, GS_WORKBOOK_NAME, TAB_LINEUP)

# ======================================================
# SCREENING PROCESSING
# ======================================================
screen_values = ws_screening.get_all_values()
headers = screen_values[0]
col_idx = {c: i + 1 for i, c in enumerate(headers)}
screen_df = pd.DataFrame(screen_values[1:], columns=headers)

batch_updates, new_rows = [], []
updated_rows, appended_rows = [], []

# ⭐ GET EXISTING CANDIDATE IDS FOR SCREENING
existing_screen_ids = pd.to_numeric(
    screen_df.get("candidate_id", pd.Series([])),
    errors="coerce"
).dropna()

SCREENING_NEXT_ID = int(existing_screen_ids.max() or 0) + 1

unique_candidates = merged_df.drop_duplicates('clean_phone', keep='last')

for _, row in unique_candidates.iterrows():
    
    phone = str(row.get('clean_phone', '')).strip()
    if not phone:
        continue

    entry_date_val = row.get('entry_date')
    entry_date_str, entry_time_str = extract_date_time(entry_date_val)

    rec = map_rec_screening(row.get('user'))
    remark = map_remark_screening(row.get('status'))
    comment = str(row.get('comments', '') or '').strip()

    loc_val = str(row.get('location', '') or '').strip()
    name_val = str(row.get('name of candidate', '') or '').strip()
    sal_val = str(row.get('clean_salary', '') or '').strip()
    comp_val = str(row.get('company', '') or '').strip()
    desig_val = str(row.get('designation', '') or '').strip()
    nameloc_val = str(row.get('name_location', '') or '').strip()
    finploy_id_val = str(row.get('finploy_id', '') or '').strip()
    finploy_city_value = str(row.get('city', '')).strip()
    city_id_val = str(row.get('city_id', '')).strip()
    compkey_val = str(row.get('address1', '') or '').strip()
    Experience = str(row.get('experience', '') or '').strip()
    Education = str(row.get('education 2', '') or '').strip()
    Graduation_year = str(row.get('graduation_year', '') or '').strip()

    matches = screen_df.index[screen_df['Contact'].astype(str).str.strip() == phone].tolist()

    # ⭐ Existing number → keep SAME candidate_id
    if matches:
        idx = matches[-1]
        rownum = idx + 2
        current_id = screen_df.loc[idx, "candidate_id"]

        # UPDATE all fields normally, do NOT change candidate_id
        to_update = [
            ('Remark', remark),
            ('Comment', comment),
            ('Rec', rec),
            ('Date', entry_date_str),
            ('Location', loc_val),
            ('Name', name_val),
            ('Salary', sal_val),
            ('Current Company', comp_val),
            ('Current Designation', desig_val),
            ('name_location', nameloc_val),
            ('finploy_id', finploy_id_val),
            ('finploy_city_id', city_id_val),
            ('Composit_key', compkey_val),
            ('Education', Education),
            ('Experience', Experience),
            ('Graduation_year', Graduation_year),
            ('Computer_Time', entry_time_str)
        ]

        for h, v in to_update:
            if h in col_idx and v != "":
                a1 = gspread.utils.rowcol_to_a1(rownum, col_idx[h])
                batch_updates.append({'range': a1, 'values': [[v]]})

        updated_rows.append(f"UPDATED | {phone} | ID={current_id}")
        continue

    # ⭐ Phone NOT FOUND → assign NEW candidate_id
    new_id = SCREENING_NEXT_ID
    SCREENING_NEXT_ID += 1

    new_row = ['' for _ in headers]

    def put(h, v):
        if h in col_idx:
            new_row[col_idx[h] - 1] = v

    put('candidate_id', str(new_id))

    put('Date', entry_date_str)
    put('Rec', rec)
    put('Contact', phone)
    put('Remark', remark)
    put('Comment', comment)
    put('Location', loc_val)
    put('Name', name_val)
    put('Salary', sal_val)
    put('Current Company', comp_val)
    put('Current Designation', desig_val)
    put('name_location', nameloc_val)
    put('finploy_id', finploy_id_val)
    put('finploy_city_id', city_id_val)
    put('finploy_city', finploy_city_value)
    put('Composit_key', compkey_val)
    put('Manual/Computer', 'Half Computer')
    put('Education', Education)
    put('Experience', Experience)
    put('Graduation_year', Graduation_year)
    put('Computer_Time', entry_time_str)

    new_rows.append(new_row)
    appended_rows.append(f"NEW | {phone} | ID={new_id}")

# PUSH SCREENING UPDATES
for i in range(0, len(batch_updates), 40):
    safe_api_call(ws_screening.batch_update, batch_updates[i:i + 40])

if new_rows:
    safe_api_call(ws_screening.append_rows, new_rows, value_input_option='USER_ENTERED')

# ======================================================
# LINEUP PROCESSING  (ALWAYS NEW candidate_id)
# ======================================================
line_values = ws_lineup.get_all_values()
line_headers = line_values[0]
line_idx = {c: i + 1 for i, c in enumerate(line_headers)}

# ⭐ Read existing candidate_ids from LINEUP
line_df_existing = pd.DataFrame(line_values[1:], columns=line_headers)
existing_line_ids = pd.to_numeric(line_df_existing.get("candidate_id", pd.Series([])),
                                  errors="coerce").dropna()

LINEUP_NEXT_ID = int(existing_line_ids.max() or 0) + 1

line_new_rows, lineup_logs = [], []

intstd_df = txt_df[txt_df['status'].astype(str).str.lower().eq('intstd')]
lineup_merge = matches_df.merge(
    intstd_df, left_on='clean_phone', right_on='phone_number', how='inner'
)
lineup_merge.columns = lineup_merge.columns.str.strip().str.lower()

before_dedupe = len(lineup_merge)
lineup_merge = lineup_merge.drop_duplicates(subset=['clean_phone', 'job_id', 'job_company'])
after_dedupe = len(lineup_merge)
print(f"Deduped lineup: {before_dedupe - after_dedupe} removed")

# INSERT NEW LINEUP ROWS
for _, r in lineup_merge.iterrows():

    new_row = ['' for _ in line_headers]

    def put(h, v):
        if h in line_idx:
            new_row[line_idx[h] - 1] = v

    # ⭐ Assign new candidate_id ALWAYS
    put("candidate_id", str(LINEUP_NEXT_ID))
    LINEUP_NEXT_ID += 1

    # Paste rest
    l_date_str, l_time_str = extract_date_time(r.get('entry_date'))

    put('Date', l_date_str)
    put('Computer_Time', l_time_str)
    put('HR', str(r.get('job_hr_name', '')))
    put('Recruiter', map_rec_lineup(r.get('user')))
    put('Role', str(r.get('job_designation', '')))
    put('Company applied', str(r.get('job_company', '')))
    put('Location', str(r.get('location', '')))
    put('Name', str(r.get('name of candidate', '')))
    put('Contact', str(r.get('clean_phone', '')))
    put('Curr Salary', str(r.get('clean_salary', '')))
    put('Current Company', str(r.get('company', '')))
    put('Current Designation', str(r.get('designation', '')))
    put('Comment', str(r.get('comments', '')))
    put('Status', 'Lineup')
    put('name_location', str(r.get('name_location', '')))
    put('finploy_loc_id', str(r.get('finploy_id', '')))
    put('finploy_city_id', str(r.get('city_id', '')))
    put('PRODUCT', str(r.get('product', '')))
    put('DEPARTMENT', str(r.get('department', '')))
    put('PINCODE', str(r.get('candidate_pincode', '')))
    put('Manual/Computer', 'Computer')
    put('Job_id', str(r.get('job_id', '')))
    put('Job_composit_key', str(r.get('job_composit_key', '')))
    put('Experience', str(r.get('experience', '')))
    put('Education', str(r.get('education 2', '')))

    line_new_rows.append(new_row)
    lineup_logs.append(f"{r.get('clean_phone', '')} | ID={LINEUP_NEXT_ID-1}")

if line_new_rows:
    safe_api_call(ws_lineup.append_rows, line_new_rows, value_input_option='USER_ENTERED')

# ======================================================
# TKINTER SUMMARY
# ======================================================
root = tk.Tk()
root.title("Summary")
root.geometry("900x650")

tk.Label(root, text="✔ Update Summary", font=("Segoe UI", 16, "bold")).pack()
tk.Label(
    root,
    text=f"SCREENING: Updated {len(updated_rows)} | Appended {len(appended_rows)}",
    font=("Segoe UI", 12, "bold"), fg="blue"
).pack()
tk.Label(
    root,
    text=f"LINEUP: Appended {len(line_new_rows)}",
    font=("Segoe UI", 12, "bold"), fg="green"
).pack(pady=(0, 10))

def box(title, lines, color):
    tk.Label(root, text=title, font=("Segoe UI", 12, "bold"), fg=color).pack()
    t = scrolledtext.ScrolledText(root, wrap=tk.WORD, width=110, height=10)
    t.pack(padx=10, pady=5)
    for ln in (lines or ["None"]):
        t.insert(tk.END, ln + "\n")

box("SCREENING UPDATED", updated_rows, "blue")
box("SCREENING APPENDED", appended_rows, "green")
box("LINEUP APPENDED", lineup_logs, "darkgreen")

root.mainloop()
