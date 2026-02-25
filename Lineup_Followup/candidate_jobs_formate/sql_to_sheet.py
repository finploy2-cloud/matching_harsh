"""
FINPLOY SQL → GOOGLE SHEET SYNC SCRIPT (Phone-based sync)

Rules:
1) Key  phone_no (Contact column in LINEUP).

2) If phone_no= NOT in LINEUP:
     - APPEND new row
     - Date = TODAY (dd-mm-YYYY)
     - candidate_id = last candidate_id in sheet + 1
     - digit_instd_date = SQL `date`  (sql_click_date)
     - digit_instd_time = SQL `time`  (sql_click_time)
     - digital_instd    = "yes"
     - Manual/Computer  = "digital"

3) If phone_no EXISTS in LINEUP:
     - UPDATE in that SAME row:
          digit_instd_date = sql_click_date
          digit_instd_time = sql_click_time
          digital_instd    = "yes"
          Manual/Computer  = "digital"
     - Do NOT change candidate_id, Date, etc.
"""

import gspread
import pandas as pd
import mysql.connector
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import gspread.utils
import re

def normalize_phone(s):
    if not s:
        return ""
    s = str(s)
    # Remove spaces, dashes, NBSP, brackets, unicode junk
    s = re.sub(r"[^\d]", "", s)

    # Keep last 10 digits
    if len(s) >= 10:
        return s[-10:]

    return s



def norm(h: str) -> str:
    return h.strip().lower().replace(" ", "").replace("_", "")


class FinployLineupSync:
    def __init__(self):
        # Google Sheet configuration
        self.SHEET_ID = "1rA6u8Z03Tq9icAAIGP1Ki6FA9U_wFKjsKaizYLkMb_M"
        self.TAB_LINEUP = "LINEUP"
        self.SERVICE_ACCOUNT_FILE = r"D:\matching_harsh\Lineup_Followup\candidate_jobs_formate\service_account.json"

        # SQL configuration
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

    # 1) SQL
    def fetch_sql_data(self):
        conn = mysql.connector.connect(**self.SQL_CONFIG)
        cursor = conn.cursor(dictionary=True)

        query = """
        SELECT
            candidate_id,
            lineup_date,
            lineup_company_hr,
            finploy_hr,
            role,
            job_company,
            job_location,
            candidate_name,
            phone_no,
            candidate_current_salary,
            candidate_current_company,
            candidate_destination,
            lineup_comment,
            lineup_remark,
            created_date,
            candidate_current_location,
            candidate_current_salary AS cleaned_slary,
            job_id,
            salary,
            Company,
            Location,
            `date` AS sql_click_date,
            `time` AS sql_click_time
        FROM candidate_jobs
        WHERE button_response = 'Interested'
        ORDER BY created_date DESC
        """

        cursor.execute(query)
        rows = cursor.fetchall()
        conn.close()

        if not rows:
            print("⚠️ No 'Interested' rows found in SQL.")
            return pd.DataFrame([])
        
        df = pd.DataFrame(rows)

        # Clean up SQL TIME format (remove any extra parts)
        if "sql_click_time" in df.columns:
            df["sql_click_time"] = df["sql_click_time"].astype(str).str.extract(r'(\d{2}:\d{2}:\d{2})')

        if "sql_click_date" not in df.columns:
            df["sql_click_date"] = ""
        if "sql_click_time" not in df.columns:
            df["sql_click_time"] = ""

        print(f"✅ Retrieved {len(df)} row(s) from SQL.")

        # DEBUG sample
        try:
            print("🔍 SQL sample (phone_no, sql_click_date, sql_click_time):")
            print(df[["phone_no", "sql_click_date", "sql_click_time"]].head(10))
        except Exception as e:
            print("🔍 SQL debug print failed:", e)

        
        print("STEP DEBUG → Total SQL rows:", len(df))

        # Count invalid phones
        df["phone_clean"] = df["phone_no"].apply(normalize_phone)
        missing_phone = df[df["phone_clean"] == ""]
        print("STEP DEBUG → Missing/Invalid Phones:", len(missing_phone))
        print(missing_phone[["phone_no"]].head(20))

        # Count duplicate phones
        dupes = df[df.duplicated("phone_clean", keep="first")]
        print("STEP DEBUG → Duplicate Phones:", len(dupes))
        print(dupes[["phone_no", "phone_clean"]].head(20))


        return df

    # 2) SHEET
    def connect_to_sheet(self):
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            self.SERVICE_ACCOUNT_FILE, scope
        )
        self.gc = gspread.authorize(creds)
        self.ws = self.gc.open_by_key(self.SHEET_ID).worksheet(self.TAB_LINEUP)

        self.all_values = self.ws.get_all_values()
        if not self.all_values:
            raise RuntimeError("LINEUP tab needs a header row before appending data.")

        self.headers = self.all_values[0]
        self.norm_headers = [norm(h) for h in self.headers]

        print(f"✅ Connected to Google Sheet tab: {self.TAB_LINEUP}")
        print("Headers:", self.headers)

    # helper: robust column locator
    def find_col(self, candidates, required_name):
        for i, h in enumerate(self.norm_headers):
            if h in candidates:
                return i
        raise RuntimeError(f"❌ Missing required column for {required_name}.")

    # 3) INDEX + next candidate_id
    def build_sheet_index(self):
        # Contact column (phone)
        idx_contact = self.find_col(
            {norm("contact"), "mobile", "phonenumber"},
            "Contact (phone_no)",
        )
        # candidate_id column
        idx_candidate_id = self.find_col(
            {norm("candidate_id"), "candidateid", "cid"},
            "candidate_id",
        )
        # digit_instd_date column
        idx_digit_instd_date = self.find_col(
            {norm("digit_instd_date"), "digitinstddate", "digitinstdate"},
            "digit_instd_date",
        )
        # digit_instd_time column
        idx_digit_instd_time = self.find_col(
            {norm("digit_instd_time"), "digitinstdtime", "digitinsttime"},
            "digit_instd_time",
        )
        # digital_instd column
        idx_digital_instd = self.find_col(
            {norm("digital_instd"), "digitalinst", "digitalinsted"},
            "digital_instd",
        )

        # Manual/Computer column (if exists)
        idx_manual_computer = None
        for i, h in enumerate(self.norm_headers):
            if h in {"manual/computer", "manualcomputer"}:
                idx_manual_computer = i
                break

        # optional Date column for appended rows
        idx_date = None
        for i, h in enumerate(self.norm_headers):
            if h in {norm("date"), "linedate", "lineupdate"}:
                idx_date = i
                break

        print(
            f"➡ Using columns (0-based): "
            f"contact={idx_contact}, candidate_id={idx_candidate_id}, "
            f"digit_instd_date={idx_digit_instd_date}, digit_instd_time={idx_digit_instd_time}, "
            f"digital_instd={idx_digital_instd}, manual_computer={idx_manual_computer}, "
            f"date={idx_date}"
        )

        # Map: NORMALIZED phone_no -> row number (first occurrence)
        existing_norm = {}
        for r, row in enumerate(self.all_values[1:], start=2):
            contact_raw = row[idx_contact].strip() if len(row) > idx_contact else ""
            norm_phone = normalize_phone(contact_raw)

            if norm_phone:
                if norm_phone not in existing_norm:
                    existing_norm[norm_phone] = []
                existing_norm[norm_phone].append(r)


        # compute next_candidate_id from last non-empty candidate_id
        next_candidate_id = 1
        for row in reversed(self.all_values[1:]):
            if len(row) > idx_candidate_id:
                val = row[idx_candidate_id].strip()
                if val:
                    try:
                        next_candidate_id = int(val) + 1
                        break
                    except ValueError:
                        continue

        print(f"➡ Next candidate_id will start from: {next_candidate_id}")
        print(f"➡ Indexed {len(existing_norm)} existing phone(s) (normalized).")

        return {
            "idx_contact": idx_contact,
            "idx_candidate_id": idx_candidate_id,
            "idx_digit_instd_date": idx_digit_instd_date,
            "idx_digit_instd_time": idx_digit_instd_time,
            "idx_digital_instd": idx_digital_instd,
            "idx_manual_computer": idx_manual_computer,
            "idx_date": idx_date,
            "existing_norm": existing_norm,   # <-- use this
            "next_candidate_id": next_candidate_id,
        }


    # 4) Map SQL row → full sheet row for APPEND
    def map_sql_row_to_sheet(self, sql_row, candidate_id, idx):
        out = [""] * len(self.headers)

        def get(col):
            return "" if col not in sql_row or pd.isna(sql_row[col]) else str(sql_row[col])

        for col_idx, header in enumerate(self.headers):
            nh = norm(header)
            if nh in ("hr", "lineuphr", "companyhr"):
                out[col_idx] = get("lineup_company_hr")
            elif nh in ("recruiter", "finployhr"):
                out[col_idx] = get("finploy_hr")
            elif nh == "role":
                out[col_idx] = get("role")
            elif nh in ("companyapplied", "company"):
                out[col_idx] = get("Company")
            elif nh == "location":
                out[col_idx] = get("Location")
            elif nh == "name":
                out[col_idx] = get("candidate_name")
            elif nh in ("contact", "mobile", "phonenumber"):
                out[col_idx] = get("phone_no")
            elif nh in ("currsalary", "currentsalary"):
                out[col_idx] = get("candidate_current_salary")
            elif nh in ("currentcompany",):
                out[col_idx] = get("candidate_current_company")
            elif nh in ("currentdesignation",):
                out[col_idx] = get("candidate_destination")
            elif nh == "comment":
                out[col_idx] = get("lineup_comment")
            elif nh == "status":
                out[col_idx] = get("lineup_remark")
            elif nh in ("jobid", "job_id"):
                out[col_idx] = get("job_id")
            elif nh in ("manual/computer", "manualcomputer"):
                out[col_idx] = "digital"

        # candidate_id
        out[idx["idx_candidate_id"]] = str(candidate_id)

        # Date for appended rows = today (if Date col exists)
        if idx["idx_date"] is not None:
            out[idx["idx_date"]] = datetime.now().strftime("%d-%m-%Y")

        # digit_instd_date = sql_click_date (kept, then overwritten by formatted)
        out[idx["idx_digit_instd_date"]] = get("sql_click_date")

        # digit_instd_date = formatted SQL date
        sql_click_date = self.format_sql_date(sql_row.get("sql_click_date"))
        out[idx["idx_digit_instd_date"]] = sql_click_date

        # digit_instd_time = formatted SQL time
        if idx["idx_digit_instd_time"] is not None:
            sql_click_time = self.format_sql_time(sql_row.get("sql_click_time"))
            out[idx["idx_digit_instd_time"]] = sql_click_time

        # Manual/Computer column logic (your requested behavior)
        if idx["idx_manual_computer"] is not None:
            existing_value = out[idx["idx_manual_computer"]].strip()

            if existing_value == "" or existing_value is None:
                out[idx["idx_manual_computer"]] = "digital"   # write only if empty
            else:
                out[idx["idx_manual_computer"]] = existing_value  # keep as is
        
        
        for col_idx, header in enumerate(self.headers):
            if norm(header) == "status":
                out[col_idx] = "Lineup"        

        # DEBUG: show what we are about to append
        try:
            phone_debug = get("phone_no")
            time_debug = ""
            if idx["idx_digit_instd_time"] is not None:
                time_debug = out[idx["idx_digit_instd_time"]]
            print(
                f"APPEND DEBUG → phone={phone_debug}, "
                f"digit_instd_date={out[idx['idx_digit_instd_date']]}, "
                f"digit_instd_time={time_debug}"
            )
        except Exception as e:
            print("APPEND DEBUG error:", e)

        return out

    # Helper for formatting sql_click_date
    def format_sql_date(self, value):
        """
        Accepts:
          - datetime/datetime-like object
          - 'YYYY-MM-DD'
          - 'YYYY-MM-DD HH:MM:SS'
          - already 'DD-MM-YYYY'
        Returns:
          - 'DD-MM-YYYY' or '' if not parseable
        """
        if pd.isna(value) or value == "":
            return ""

        # If MySQL gives a datetime object
        if isinstance(value, datetime):
            return value.strftime("%d-%m-%Y")

        s = str(value).strip()
        if not s:
            return ""

        # Take just date part if there's time
        if " " in s:
            s = s.split(" ")[0]  # e.g. '2025-11-25'

        # Case 1: 'YYYY-MM-DD' → convert to 'DD-MM-YYYY'
        if len(s) == 10 and s[4] == "-" and s[7] == "-":
            y, m, d = s.split("-")
            return f"{d}-{m}-{y}"

        # Case 2: already 'DD-MM-YYYY'
        if len(s) == 10 and s[2] == "-" and s[5] == "-":
            return s

        # Fallback: give up, return empty
        return ""
    # Helper for formatting sql_click_time
    def format_sql_time(self, value):
        if pd.isna(value) or value == "":
            return ""
        if isinstance(value, datetime):
            return value.strftime("%H:%M:%S")
        s = str(value)
        return s[:8]

    # 5) Apply logic (phone-based)
    def apply_sync(self, df_sql, idx):
        existing_norm = idx["existing_norm"]
        idx_digit_instd_date = idx["idx_digit_instd_date"]
        idx_digit_instd_time = idx["idx_digit_instd_time"]
        idx_digital_instd = idx["idx_digital_instd"]
        idx_manual_computer = idx["idx_manual_computer"]
        next_candidate_id = idx["next_candidate_id"]

        append_buffer = []
        update_requests = []
        update_count = 0
        append_count = 0

        print(f"🚀 Starting sync for {len(df_sql)} SQL rows...")

        for _, sql_row in df_sql.iterrows():
            phone_raw = "" if pd.isna(sql_row.get("phone_no")) else str(sql_row.get("phone_no")).strip()
            phone_norm = normalize_phone(phone_raw)

            # If no phone → append as fresh row
            if not phone_norm:
                new_row = self.map_sql_row_to_sheet(sql_row, next_candidate_id, idx)
                append_buffer.append(new_row)
                next_candidate_id += 1
                append_count += 1
                print(f"➕ APPEND (no/invalid phone): candidate_id={next_candidate_id-1}")
                continue

            sql_click_date = self.format_sql_date(sql_row.get("sql_click_date"))
            sql_click_time = self.format_sql_time(sql_row.get("sql_click_time"))

            if phone_norm in existing_norm:
                all_rows = existing_norm[phone_norm]   # list of ALL rows for this phone

                for r in all_rows:
                    row_data = self.all_values[r - 1]

                    existing_date_str = ""
                    if len(row_data) > idx_digit_instd_date:
                        existing_date_str = row_data[idx_digit_instd_date].strip()

                    try:
                        existing_date = datetime.strptime(existing_date_str, "%d-%m-%Y") if existing_date_str else None
                        new_date = datetime.strptime(sql_click_date, "%d-%m-%Y") if sql_click_date else None
                    except Exception:
                        existing_date, new_date = None, None

                    should_update = False

                    # CASE 1 — SQL has a valid date and it is newer OR LINEUP empty
                    if new_date:
                        if not existing_date or new_date >= existing_date:
                            should_update = True

                    # CASE 2 — SQL has a time
                    elif sql_click_time:
                        should_update = True

                    # CASE 3 — fallback — SQL click date exists
                    elif sql_row.get("sql_click_date"):
                        should_update = True

                    if should_update:
                        sub_updates = []

                        if sql_click_date:
                            a1_digit_date = gspread.utils.rowcol_to_a1(r, idx_digit_instd_date + 1)
                            sub_updates.append({"range": a1_digit_date, "values": [[sql_click_date]]})

                        if idx_digit_instd_time is not None and sql_click_time:
                            a1_digit_time = gspread.utils.rowcol_to_a1(r, idx_digit_instd_time + 1)
                            sub_updates.append({"range": a1_digit_time, "values": [[sql_click_time]]})

                        if idx_digital_instd is not None:
                            a1_digital = gspread.utils.rowcol_to_a1(r, idx_digital_instd + 1)
                            sub_updates.append({"range": a1_digital, "values": [["yes"]]})

                        if idx_manual_computer is not None:
                            a1_manual = gspread.utils.rowcol_to_a1(r, idx_manual_computer + 1)
                            sub_updates.append({"range": a1_manual, "values": [["digital"]]})

                        update_requests.extend(sub_updates)
                        update_count += 1

                        print(f"✅ UPDATE: phone={phone_norm} row={r} | old={existing_date_str} → {sql_click_date}")


            else:
                # New phone → append row
                new_row = self.map_sql_row_to_sheet(sql_row, next_candidate_id, idx)
                append_buffer.append(new_row)
                next_candidate_id += 1
                append_count += 1
                print(f"➕ APPEND: NEW phone_raw={phone_raw} norm={phone_norm} | date={sql_click_date} time={sql_click_time}")

        # Apply writes after loop
        if append_buffer:
            self.ws.append_rows(append_buffer, value_input_option="USER_ENTERED")
            print(f"✅ Appended {append_count} new row(s).")

        if update_requests:
            self.ws.batch_update(update_requests)
            print(f"🛠️ Updated {update_count} existing row(s).")

        if not append_buffer and not update_requests:
            print("ℹ️ No changes made (no new phones, no matching phones).")

        print(f"\n📊 SUMMARY → checked={len(df_sql)} | appended={append_count} | updated={update_count}")


    # 6) Run
    def run(self):
        print("🚀 Starting Finploy Lineup Sync (phone-based)…")
        df_sql = self.fetch_sql_data()
        if df_sql.empty:
            print("🎯 Nothing to sync.")
            return
        self.connect_to_sheet()
        idx = self.build_sheet_index()
        self.apply_sync(df_sql, idx)
        print("🎯 Sync completed successfully.")


if __name__ == "__main__":
    print("▶ Starting phone_based_sync.py ...")
    try:
        sync = FinployLineupSync()
        print("▶ FinployLineupSync object created.")
        sync.run()
        print("✅ Script finished without unhandled errors.")
    except Exception as e:
        import traceback
        print("❌ Script crashed with an error:")
        print(e)
        traceback.print_exc()
try:
    import subprocess
    print("🚀 Running Not Interested to Screening  ...")
    subprocess.run(
        ["python", r"D:\matching_harsh\Lineup_Followup\candidate_jobs_formate\sql_sheet_sn.py"],
        check=True
    )
    print("✔ Interested to Screening  executed successfully!")

except Exception as e:
    print(f"❌ Failed to run Interested to Screening : {e}")