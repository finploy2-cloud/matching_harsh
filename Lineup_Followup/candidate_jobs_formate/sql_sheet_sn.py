"""
FINPLOY SQL → SCREENING SYNC (NOT INTERESTED SYNC)

Fixed:
1) NEVER append the same phone number twice.
2) If phone exists → ALWAYS UPDATE ONLY Remark.
3) If phone does NOT exist → APPEND only once.
4) FINPLOY fields correctly mapped.
5) SQL duplicates also removed.
"""

import gspread
import pandas as pd
import mysql.connector
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import re
import gspread.utils

# --- PHONE NORMALIZER ---
def normalize_phone(s):
    if not s:
        return ""
    s = str(s)
    s = re.sub(r"[^\d]", "", s)
    return s[-10:] if len(s) >= 10 else ""


# --- HEADER NORMALIZATION ---
def norm(h: str) -> str:
    return h.strip().lower().replace(" ", "").replace("_", "")


class ScreeningNotInterestedSync:
    def __init__(self):
        # Google Sheet config
        self.SHEET_ID = "1rA6u8Z03Tq9icAAIGP1Ki6FA9U_wFKjsKaizYLkMb_M"
        self.TAB = "SCREENING"
        self.SERVICE_ACCOUNT_FILE = r"D:\matching_harsh\Lineup_Followup\candidate_jobs_formate\service_account.json"

        # SQL config
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


    # --- SQL FETCH (NOT INTERESTED ONLY) ---
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

        # 🚫 Remove duplicates inside SQL itself
        df = df.drop_duplicates(subset=["phone_clean"], keep="first")

        print("✅ SQL unique NOT INTERESTED phones:", len(df))
        return df


    # --- CONNECT TO GOOGLE SHEET ---
    def connect_to_sheet(self):
        scope = ["https://spreadsheets.google.com/feeds",
                 "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            self.SERVICE_ACCOUNT_FILE, scope)
        self.gc = gspread.authorize(creds)
        self.ws = self.gc.open_by_key(self.SHEET_ID).worksheet(self.TAB)

        self.all_values = self.ws.get_all_values()
        self.headers = self.all_values[0]
        self.norm_headers = [norm(h) for h in self.headers]

        print("📄 Connected to SCREENING sheet.")


    # --- FIND COLUMN INDEX BY HEADER ---
    def find_col(self, names, label):
        for i, h in enumerate(self.norm_headers):
            if h in names:
                return i
        raise RuntimeError(f"❌ Missing column: {label}")


    # --- SCREENING INDEX BUILD ---
    def build_index(self):
        idx_contact = self.find_col({norm("Contact")}, "Contact")
        idx_remark = self.find_col({norm("Remark")}, "Remark")

        existing = {}
        for r, row in enumerate(self.all_values[1:], start=2):
            phone = normalize_phone(row[idx_contact]) if len(row) > idx_contact else ""
            if phone:
                existing[phone] = r    # last row wins

        return {
            "idx_contact": idx_contact,
            "idx_remark": idx_remark,
            "existing": existing,
        }


    # --- BUILD NEW ROW FOR APPEND ---
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


    # --- APPLY SYNC LOGIC ---
    def apply_sync(self, df_sql, idx):
        existing = idx["existing"]
        idx_remark = idx["idx_remark"]

        updates = []
        append_rows = []
        update_count = 0
        append_count = 0

        # 🚫 Track phones handled during this run
        processed = set()

        for _, row in df_sql.iterrows():
            phone = row["phone_clean"]
            if not phone:
                continue

            if phone in processed:
                continue   # 🔥 prevents duplicates in same run
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


    # --- RUN MAIN PROCESS ---
    def run(self):
        df_sql = self.fetch_sql()
        if df_sql.empty:
            print("No NOT INTERESTED rows.")
            return

        self.connect_to_sheet()
        idx = self.build_index()
        self.apply_sync(df_sql, idx)



if __name__ == "__main__":
    print("🚀 Starting NOT INTERESTED → SCREENING sync...")
    sync = ScreeningNotInterestedSync()
    sync.run()
    print("✅ Completed.")
