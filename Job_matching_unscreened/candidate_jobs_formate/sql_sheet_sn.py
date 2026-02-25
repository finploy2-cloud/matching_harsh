"""
FINPLOY SQL → SCREENING SYNC (NOT INTERESTED SYNC)
WITH SCRIPT_RUN_LOG LOGGING (SUCCESS / FAILURE)
"""

import gspread
import pandas as pd
import mysql.connector
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import re
import gspread.utils
import traceback


# =====================================================================
# GOOGLE SHEET LOGGING CONFIG  (Script_Run_Log)
# =====================================================================
LOG_SHEET_ID = "1fa620bLHdr3DP0d91_IvSo5Jt3P0gGPWblDq4j4LP80"
LOG_TAB_NAME = "Harsh_Python_Programs"
LOG_SERVICE_ACCOUNT_FILE = r"D:\matching_harsh\Job_matching_Screened\candidate_jobs_formate\service_account.json"

PROGRAM_NAME = "Job Matching Unscreened"


def write_log(status, reason):
    """Write success/failure logs to Script_Run_Log sheet."""
    try:
        scope = ["https://spreadsheets.google.com/feeds",
                 "https://www.googleapis.com/auth/drive"]

        creds = ServiceAccountCredentials.from_json_keyfile_name(
            LOG_SERVICE_ACCOUNT_FILE, scope
        )
        gc = gspread.authorize(creds)
        ws = gc.open_by_key(LOG_SHEET_ID).worksheet(LOG_TAB_NAME)

        now = datetime.now()
        row = [
            now.strftime("%d-%m-%Y"),  # Date
            status,                    # 1 or 0
            PROGRAM_NAME,              # Script name
            now.strftime("%H:%M:%S"),  # Time
            reason                     # Message / Error
        ]

        ws.append_row(row)
        print("📝 Log written:", row)

    except Exception as e:
        print("⚠ Logging failed:", e)


# =====================================================================
# ORIGINAL SCRIPT CODE (UNCHANGED)
# =====================================================================

def normalize_phone(s):
    if not s:
        return ""
    s = str(s)
    s = re.sub(r"[^\d]", "", s)
    return s[-10:] if len(s) >= 10 else ""


def norm(h: str) -> str:
    return h.strip().lower().replace(" ", "").replace("_", "")


class ScreeningNotInterestedSync:
    def __init__(self):
        self.SHEET_ID = "1rA6u8Z03Tq9icAAIGP1Ki6FA9U_wFKjsKaizYLkMb_M"
        self.TAB = "SCREENING"
        self.SERVICE_ACCOUNT_FILE = r"D:\matching_harsh\Job_matching_Screened\candidate_jobs_formate\service_account.json"

        self.SQL_CONFIG = {
            "host": "65.0.211.89",
            "user": "harsh875_finploy_user1",
            "password": "Make*45@23+67",
            "database": "harsh875_finploy_com",
            "port": 3306,
        }

        self.gc = None
        self.ws = None
        self.headers = []
        self.norm_headers = []
        self.all_values = []

    def fetch_sql(self):
        conn = mysql.connector.connect(**self.SQL_CONFIG)
        cursor = conn.cursor(dictionary=True)

        query = """
        SELECT
            candidate_id,
            candidate_name,
            phone_no,
            candidate_current_salary,
            candidate_current_company,
            candidate_destination,
            Location,
            Company,
            role,
            job_company,
            job_location,
            lineup_company_hr,
            finploy_hr,
            lineup_comment,
            finploy_id,
            finploy_city_id,
            finploy_city,
            finploy_state,
            `date` AS sql_click_date,
            `time` AS sql_click_time,
            button_response,
            NOT_INTERESTED
        FROM candidate_jobs
        WHERE NOT_INTERESTED = 'not interested'
          AND (button_response IS NULL OR button_response <> 'Interested')
        ORDER BY created_date DESC
        """

        cursor.execute(query)
        rows = cursor.fetchall()
        conn.close()

        df = pd.DataFrame(rows)
        df["phone_clean"] = df["phone_no"].apply(normalize_phone)

        df = df.drop_duplicates(subset=["phone_clean"], keep="first")

        print("✅ SQL unique NOT INTERESTED phones:", len(df))
        return df

    def connect_to_sheet(self):
        scope = ["https://spreadsheets.google.com/feeds",
                 "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            self.SERVICE_ACCOUNT_FILE, scope
        )
        self.gc = gspread.authorize(creds)
        self.ws = self.gc.open_by_key(self.SHEET_ID).worksheet(self.TAB)

        self.all_values = self.ws.get_all_values()
        self.headers = self.all_values[0]
        self.norm_headers = [norm(h) for h in self.headers]

        print("📄 Connected to SCREENING sheet.")

    def find_col(self, names, label):
        for i, h in enumerate(self.norm_headers):
            if h in names:
                return i
        raise RuntimeError(f"❌ Missing column: {label}")

    def build_index(self):
        idx_contact = self.find_col({norm("Contact")}, "Contact")
        idx_remark = self.find_col({norm("Remark")}, "Remark")

        existing = {}
        for r, row in enumerate(self.all_values[1:], start=2):
            phone = normalize_phone(row[idx_contact]) if len(row) > idx_contact else ""
            if phone:
                existing[phone] = r

        return {
            "idx_contact": idx_contact,
            "idx_remark": idx_remark,
            "existing": existing,
        }

    def build_append_row(self, sql_row):
        today = datetime.now().strftime("%d-%m-%Y")
        out = [""] * len(self.headers)

        mapping = {
            "candidate_id": sql_row.get("candidate_id", ""),
            "date": today,
            "hr": sql_row.get("lineup_company_hr", ""),
            "rec": sql_row.get("finploy_hr", ""),
            "role": sql_row.get("role", ""),
            "companyappliedfor": sql_row.get("Company", ""),
            "location": sql_row.get("Location", ""),
            "name": sql_row.get("candidate_name", ""),
            "contact": sql_row.get("phone_no", ""),
            "salary": sql_row.get("candidate_current_salary", ""),
            "currentcompany": sql_row.get("candidate_current_company", ""),
            "currentdesignation": sql_row.get("candidate_destination", ""),
            "comment": sql_row.get("lineup_comment", ""),
            "remark": "Not Interested",
            "finployid": sql_row.get("finploy_id", ""),
            "finploycityid": sql_row.get("finploy_city_id", ""),
            "finploycity": sql_row.get("finploy_city", ""),
            "finploystate": sql_row.get("finploy_state", ""),
        }

        for i, h in enumerate(self.norm_headers):
            if h in mapping:
                out[i] = mapping[h]

        return out

    def apply_sync(self, df_sql, idx):
        existing = idx["existing"]
        idx_remark = idx["idx_remark"]

        updates = []
        append_rows = []
        update_count = 0
        append_count = 0
        processed = set()

        for _, row in df_sql.iterrows():
            phone = row["phone_clean"]
            if not phone:
                continue

            if phone in processed:
                continue
            processed.add(phone)

            if phone in existing:
                sheet_row = existing[phone]

                a1 = gspread.utils.rowcol_to_a1(sheet_row, idx_remark + 1)
                updates.append({"range": a1, "values": [["Not Interested"]]})
                update_count += 1

                print(f"🔄 UPDATE ONLY: {phone}")
            else:
                append_rows.append(self.build_append_row(row))
                append_count += 1
                print(f"🆕 APPEND NEW: {phone}")

        if updates:
            self.ws.batch_update(updates)

        if append_rows:
            self.ws.append_rows(append_rows)

        print(f"\n📊 SUMMARY → Updated: {update_count}, Appended: {append_count}")

    def run(self):
        df_sql = self.fetch_sql()
        if df_sql.empty:
            print("No NOT INTERESTED rows.")
            return

        self.connect_to_sheet()
        idx = self.build_index()
        self.apply_sync(df_sql, idx)


# =====================================================================
# MAIN ENTRY WITH WRAPPER
# =====================================================================
if __name__ == "__main__":
    try:
        print("🚀 Starting NOT INTERESTED → SCREENING sync...")
        sync = ScreeningNotInterestedSync()
        sync.run()
        print("✅ Completed.")

        # SUCCESS LOG
        write_log(1, "Harsh as a coder is Great")

    except Exception as e:
        error_text = traceback.format_exc()
        print("❌ ERROR OCCURRED:\n", error_text)

        # FAILURE LOG
        write_log(0, error_text)
