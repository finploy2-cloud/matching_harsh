import re
import gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
import mysql.connector
import pandas as pd

GS_WORKBOOK_NAME = "Tracker -Candidates"
TAB_LINEUP = "LINEUP"

def normalize_phone(value):
    digits = re.sub(r"\D", "", value or "")
    return digits[-10:] if len(digits) > 10 else digits

def clean_text(value):
    if pd.isna(value):
        return None
    text = str(value).strip()
    return text or None

# -------------------------------------------------
# GOOGLE SHEET AUTH
# -------------------------------------------------
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
creds = ServiceAccountCredentials.from_json_keyfile_name(
    r"D:\matching_harsh\Job_matching_Unscreened\candidate_jobs_formate\service_account.json", scope
)
gc = gspread.authorize(creds)
ws_lineup = gc.open(GS_WORKBOOK_NAME).worksheet(TAB_LINEUP)

rows = ws_lineup.get_all_values()
headers = [h.strip() for h in rows[0]]
df = pd.DataFrame(rows[1:], columns=headers)

df["Contact"] = df["Contact"].astype(str).str.strip()
df["candidate_id"] = df["candidate_id"].astype(str).str.strip()
df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

df = df[
    (df["Contact"] != "") &
    df["candidate_id"].ne("") &
    df["Date"].notna()
]

df["phone"] = df["Contact"].apply(normalize_phone)

# -------------------------------------------------
# SQL: FETCH ONLY ELIGIBLE INTERESTED CANDIDATES
# -------------------------------------------------
db = mysql.connector.connect(
    host="65.0.211.89",
    user="harsh875_finploy_user1",
    password="Make*45@23+67",
    database="harsh875_finploy_com",
)
cursor = db.cursor()

cursor.execute("""
    SELECT phone_no
    FROM candidate_jobs
    WHERE button_response_inst = 'interested'
      AND (lineup_company_hr IS NULL OR lineup_company_hr = 0)
      AND (lineup_remark IS NULL OR lineup_remark = 0)
      AND (lineup_comment IS NULL OR lineup_comment = 0)
      AND lineup_date IS NULL
      AND (lineup_finploy_hr IS NULL OR lineup_finploy_hr = 0)
""")

eligible_phones = {row[0] for row in cursor.fetchall()}

print(f"Eligible interested candidates: {len(eligible_phones)}")

# -------------------------------------------------
# FILTER LINEUP DATA TO ONLY ELIGIBLE PHONES
# -------------------------------------------------
df = df[df["phone"].isin(eligible_phones)]

df = (
    df.sort_values(by=["phone", "Date"], ascending=[True, False])
      .drop_duplicates(subset=["phone"], keep="first")
)

print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] candidates to process: {len(df)}")

# -------------------------------------------------
# UPDATE ONLY REQUIRED ROWS
# -------------------------------------------------
updated = 0

for _, row in df.iterrows():
    lineup_date = row["Date"].to_pydatetime()

    cursor.execute("""
        UPDATE candidate_jobs
        SET lineupId = %s,
            lineup_company_hr = %s,
            lineup_remark = %s,
            lineup_comment = %s,
            lineup_date = %s,
            lineup_finploy_hr = %s
        WHERE phone_no = %s
          AND button_response_inst = 'interested'
    """, (
        row["candidate_id"],
        clean_text(row.get("HR")),
        clean_text(row.get("Status")),
        clean_text(row.get("Comment")),
        lineup_date,
        clean_text(row.get("Recruiter")),
        row["phone"]
    ))

    if cursor.rowcount:
        updated += 1
        print(f"updated lineup → {row['phone']}")

db.commit()
cursor.close()
db.close()

print(f"Rows actually updated: {updated}")

# -------------------------------------------------
# RUN NEXT SCRIPT
# -------------------------------------------------
import subprocess
subprocess.run(
    ["python", r"D:\matching_harsh\Job_matching_Unscreened\candidate_jobs_formate\hr_name.py"],
    check=True
)
