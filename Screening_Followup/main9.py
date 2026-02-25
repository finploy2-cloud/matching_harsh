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
TXT_PATH = r'D:\matching_harsh\Screening_Followup\input\LIST_1001122504_20251201-171517.txt'
ALL_MATCHES_XLSX = r"D:\matching_harsh\Screening_Followup\final_output\all_job_matches.xlsx\all_job_matches_duplicate.xlsx"

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
    return {'4': 'Soham', '3': 'Antara', '9': 'Shraddha', 'VDAD': ''}.get(user, user)

def map_rec_lineup(rec):
    if rec is None: return ""
    rec = str(rec).strip().upper()
    if rec == "COMP9": return "Shraddha"
    elif rec == "COMP4": return "Soham"
    elif rec == "COMP3": return "Antara"
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
    date_only_fmts = ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y")
    for f in date_only_fmts:
        try:
            d = datetime.strptime(val, f)
            return d.strftime("%d-%m-%Y"), "00:00:00"
        except Exception:
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

# --- Normalize phone key to 'contact'
if 'phone_number' in txt_df.columns and 'contact' not in txt_df.columns:
    txt_df['contact'] = txt_df['phone_number'].astype(str).str.strip()

if 'contact' not in matches_df.columns and 'clean_phone' in matches_df.columns:
    matches_df['contact'] = matches_df['clean_phone'].astype(str).str.strip()
elif 'contact' in matches_df.columns:
    matches_df['contact'] = matches_df['contact'].astype(str).str.strip()

merged_df = matches_df.merge(
    txt_df[['contact', 'status', 'user', 'comments', 'entry_date', 'last_name',
            'address1', 'address2', 'address3', 'city']],
    on='contact', how='left'
)

# ======================================================
# GOOGLE AUTH
# ======================================================
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name(
    r'D:\matching_harsh\Screening_Followup\screeningfollowup-4a463d7d64cb.json', scope
)
gc = gspread.authorize(creds)
ws_screening = open_ws(gc, GS_WORKBOOK_NAME, TAB_SCREENING)
ws_lineup = open_ws(gc, GS_WORKBOOK_NAME, TAB_LINEUP)

# ======================================================
# SCREENING PROCESSING
# ======================================================
screen_values = ws_screening.get_all_values()
headers = [h.strip() for h in screen_values[0]]
col_idx = {c.strip(): i + 1 for i, c in enumerate(headers)}
screen_df = pd.DataFrame(screen_values[1:], columns=headers)

batch_updates, new_rows = [], []
updated_rows, appended_rows = [], []

unique_candidates = merged_df.drop_duplicates('contact', keep='last')

for _, row in unique_candidates.iterrows():
    phone = str(row.get('contact', '')).strip()
    if not phone:
        continue

    entry_date_val = row.get('entry_date')
    entry_date_str, entry_time_str = extract_date_time(entry_date_val)

    rec = map_rec_screening(row.get('user'))
    remark = map_remark_screening(row.get('status'))
    comment = str(row.get('comments', '') or '').strip()

    loc_val = str(row.get('location', '') or '').strip()
    name_val = str(row.get('name', '') or '').strip()
    sal_val = str(row.get('clean_salary', '') or '').strip()
    comp_val = str(row.get('current company', '') or row.get('company', '') or '').strip()
    desig_val = str(row.get('current designation', '') or row.get('designation', '') or '').strip()
    nameloc_val = str(row.get('name_location', '') or '').strip()
    finploy_id_val = str(row.get('finploy_id', '') or '').strip()
    finploy_city_value = str(row.get('city', '') or '').strip()
    city_id_val = str(row.get('city_id', '') or '').strip()
    compkey_val = str(row.get('address1', '') or '').strip()
    Experience = str(row.get('experience', '') or '').strip()
    Education = str(row.get('education 2', '') or '').strip()
    Graduation_year = str(row.get('graduation_year', '') or '').strip()

    matches = (
        screen_df.index[screen_df['Contact'].astype(str).str.strip() == phone].tolist()
        if 'Contact' in screen_df.columns else []
    )

    def fill_screening_row(row_buf):
        def safe_put(header_name, value):
            for h in col_idx.keys():
                if h.strip().casefold() == header_name.strip().casefold():
                    row_buf[col_idx[h] - 1] = value
                    break

        safe_put('Date', entry_date_str)
        safe_put('Rec', rec)
        safe_put('Contact', phone)
        safe_put('Remark', remark)
        safe_put('Comment', comment)
        safe_put('Location', loc_val)
        safe_put('Name', name_val)
        safe_put('Salary', sal_val)
        safe_put('Current Company', comp_val)
        safe_put('Current Designation', desig_val)
        safe_put('name_location', nameloc_val)
        safe_put('finploy_id', finploy_id_val)
        safe_put('finploy_city_id', city_id_val)
        safe_put('finploy_city', finploy_city_value)
        safe_put('Composit_key', compkey_val)
        safe_put('Manual/Computer', 'Half Computer')
        safe_put('Education', Education)
        safe_put('Experience', Experience)
        safe_put('Graduation_year', Graduation_year)
        safe_put('Computer_Time', entry_time_str)

    if not matches:
        next_id = int(pd.to_numeric(screen_df.get('candidate_id', pd.Series([])), errors='coerce').max() or 0) + 1
        new_row = ['' for _ in headers]
        fill_screening_row(new_row)
        new_rows.append(new_row)
        appended_rows.append(f"NEW | {phone} | {name_val or 'N/A'} | {remark}")
        continue

    idx = matches[-1]
    rownum = idx + 2
    to_update = [
        ('Remark', remark), ('Comment', comment), ('Rec', rec),
        ('Date', entry_date_str), ('Location', loc_val), ('Name', name_val),
        ('Salary', sal_val), ('Current Company', comp_val),
        ('Current Designation', desig_val), ('name_location', nameloc_val),
        ('finploy_id', finploy_id_val), ('finploy_city_id', city_id_val),
        ('Composit_key', compkey_val), ('Education', Education),
        ('Experience', Experience), ('Graduation_year', Graduation_year),
        ('Computer_Time', entry_time_str),
    ]
    for h, v in to_update:
        for key in col_idx.keys():
            if key.strip().casefold() == h.strip().casefold():
                a1 = gspread.utils.rowcol_to_a1(rownum, col_idx[key])
                batch_updates.append({'range': a1, 'values': [[v]]})
                break
    updated_rows.append(f"UPDATED | {phone} | {remark}")

for i in range(0, len(batch_updates), 40):
    safe_api_call(ws_screening.batch_update, batch_updates[i:i + 40])
if new_rows:
    safe_api_call(ws_screening.append_rows, new_rows, value_input_option='USER_ENTERED')

# ======================================================
# LINEUP PROCESSING (FIXED)
# ======================================================
line_values = ws_lineup.get_all_values()
line_headers = [h.strip() for h in line_values[0]]
line_idx = {c.strip(): i + 1 for i, c in enumerate(line_headers)}
line_new_rows, lineup_logs = [], []

intstd_df = txt_df[txt_df['status'].astype(str).str.lower().eq('intstd')].copy()
lineup_merge = matches_df.merge(intstd_df, on='contact', how='inner')
lineup_merge.columns = lineup_merge.columns.str.strip().str.lower()

before_dedupe = len(lineup_merge)
lineup_merge = lineup_merge.drop_duplicates(subset=['candidate_id', 'job_id', 'job_company'])
after_dedupe = len(lineup_merge)
print(f"✅ Deduped lineup: {before_dedupe - after_dedupe} duplicates removed. Final count: {after_dedupe}")

for _, r in lineup_merge.iterrows():
    new_row = ['' for _ in line_headers]

    def safe_put(header_name, value):
        for h in line_idx.keys():
            if h.strip().casefold() == header_name.strip().casefold():
                new_row[line_idx[h] - 1] = value
                break

    l_date_str, _ = extract_date_time(r.get('entry_date'))

    curr_company = (
        str(r.get('current company', '') or r.get('company applied for', '') or r.get('company', '') or '').strip()
    )
    curr_designation = (
        str(r.get('current designation', '') or r.get('designation', '') or r.get('job_designation', '') or '').strip()
    )

    safe_put('Date', l_date_str)
    safe_put('Computer_Time', datetime.now().strftime("%H:%M:%S"))
    safe_put('HR', str(r.get('job_hr_name', '')).strip())
    safe_put('Recruiter', map_rec_lineup(r.get('user')))
    safe_put('Role', str(r.get('job_designation', '')).strip())
    safe_put('Company applied', str(r.get('job_company', '')).strip())
    safe_put('Location', str(r.get('location', '')).strip())
    safe_put('Name', str(r.get('name', '')).strip())
    safe_put('Contact', str(r.get('contact', '')).strip())
    safe_put('Curr Salary', str(r.get('clean_salary', '')).strip())
    safe_put('Current Company', curr_company)
    safe_put('Current Designation', curr_designation)
    safe_put('Comment', str(r.get('comments', '')).strip())
    safe_put('Status', 'Lineup')
    safe_put('name_location', str(r.get('name_location', '')).strip())
    safe_put('finploy_loc_id', str(r.get('finploy_id', '')).strip())
    safe_put('finploy_city_id', str(r.get('city_id', '')).strip())
    safe_put('finploy_city', str(r.get('city', '')).strip())
    safe_put('PRODUCT', str(r.get('product', '')).strip())
    safe_put('DEPARTMENT', str(r.get('department', '')).strip())
    safe_put('PINCODE', str(r.get('candidate_pincode', '')).strip())
    safe_put('Manual/Computer', 'Computer')
    safe_put('Job_id', str(r.get('job_id', '')).strip())
    safe_put('Experience', str(r.get('experience', '')).strip())
    safe_put('Education', str(r.get('education', '') or r.get('education 2', '')).strip())

    line_new_rows.append(new_row)
    lineup_logs.append(f"{r.get('contact', '')} | {curr_company} | {curr_designation}")

if line_new_rows:
    safe_api_call(ws_lineup.append_rows, line_new_rows, value_input_option='USER_ENTERED')

# ======================================================
# TKINTER SUMMARY
# ======================================================
root = tk.Tk()
root.title("Summary")
root.geometry("900x650")

tk.Label(root, text="✔ Update Summary", font=("Segoe UI", 16, "bold")).pack()
tk.Label(root, text=f"SCREENING: Updated {len(updated_rows)} | Appended {len(appended_rows)}",
         font=("Segoe UI", 12, "bold"), fg="blue").pack()
tk.Label(root, text=f"LINEUP: Appended {len(line_new_rows)}",
         font=("Segoe UI", 12, "bold"), fg="green").pack(pady=(0, 10))

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
