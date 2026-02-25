import re
import gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
import mysql.connector
import pandas as pd

GS_WORKBOOK_NAME = "Tracker -Candidates"
TAB_LINEUP = "LINEUP"

def normalize_phone(value: str) -> str:
    digits = re.sub(r"\D", "", value or "")
    if len(digits) > 10:
        digits = digits[-10:]        # keep last 10 digits if a country code is included
    return digits

def clean_text(value):
    if pd.isna(value):
        return None
    text = str(value).strip()
    return text or None

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
creds = ServiceAccountCredentials.from_json_keyfile_name(
    r"D:\matching_harsh\Lineup_Followup\candidate_jobs_formate\service_account.json", scope
)
gc = gspread.authorize(creds)
ws_lineup = gc.open(GS_WORKBOOK_NAME).worksheet(TAB_LINEUP)

rows = ws_lineup.get_all_values()
headers = [h.strip() for h in rows[0]]
df = pd.DataFrame(rows[1:], columns=headers)

df["Contact"] = df["Contact"].astype(str).str.strip()
df["candidate_id"] = df["candidate_id"].astype(str).str.strip()
df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
df = df[(df["Contact"] != "") & df["candidate_id"].ne("") & df["Date"].notna()]

df_sorted = df.sort_values(by=["Contact", "Date"], ascending=[True, False])
df_latest = df_sorted.drop_duplicates(subset=["Contact"], keep="first")

print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] candidates to process: {len(df_latest)}")

db = mysql.connector.connect(
    host="65.0.211.89",
    user="harsh875_finploy_user1",
    password="Make*45@23+67",
    database="harsh875_finploy_com",
)
cursor = db.cursor()

updated = 0
for _, row in df_latest.iterrows():
    candidate_id = row["candidate_id"]
    contact = normalize_phone(row["Contact"])
    if not candidate_id or not contact:
        continue
    hr_name = clean_text(row.get("HR"))
    Recruiter = clean_text(row.get("Recruiter"))
    Status = clean_text(row.get("Status"))
    comment = clean_text(row.get("Comment"))
    lineup_date = row.get("Date")
    if isinstance(lineup_date, pd.Timestamp):
        lineup_date = lineup_date.to_pydatetime()

    sql = """
        UPDATE candidate_jobs
        SET lineupId = %s,
            lineup_company_hr = %s,
            lineup_remark = %s,
            lineup_comment = %s,
            lineup_date = %s,
            lineup_finploy_hr = %s
        WHERE button_response = 'interested' AND phone_no = %s
    """
    cursor.execute(sql, (candidate_id, hr_name, Status, comment, lineup_date, Recruiter, contact))
    if cursor.rowcount:
        updated += 1
        print(f"matched phone {contact} -> lineupId {candidate_id}")
           

db.commit()
cursor.close()
db.close()

print(f"rows actually updated: {updated}")

try:
    import subprocess
    print("🚀 Running hr_name.py ...")
    subprocess.run(
        ["python", r"D:\matching_harsh\Lineup_Followup\candidate_jobs_formate\hr_name.py"],
        check=True
    )
    print("✔ hr_name.py executed successfully!")

except Exception as e:
    print(f"❌ Failed to run hr_name.py: {e}")