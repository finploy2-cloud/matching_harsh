"""
FINPLOY SQL → GOOGLE SHEET SYNC SCRIPT (Phone-based sync)

Rules:
1) Key = phone_no (Contact column in LINEUP).

2) If phone_no NOT in LINEUP:
     - APPEND new row (full mapped fields)
     - Date = TODAY (dd-mm-YYYY)  [only if Date column exists]
     - candidate_id = last candidate_id in sheet + 1
     - digit_instd_date = latest SQL `date` for that phone (sql_click_date)
     - digit_instd_time = latest SQL `time` for that phone (sql_click_time)
     - digital_instd    = "yes"

3) If phone_no EXISTS in LINEUP:
     - UPDATE that SAME row(s) with latest SQL click:
          digit_instd_date = latest sql_click_date
          digit_instd_time = latest sql_click_time
          digital_instd    = "yes"
     - Do NOT change candidate_id, Date, Manual/Computer, etc.
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
    s = re.sub(r"[^\d]", "", s)
    return s[-10:] if len(s) >= 10 else s


def norm(h: str) -> str:
    return h.strip().lower().replace(" ", "").replace("_", "")


class FinployLineupSync:
    def __init__(self):
        # Google Sheet configuration
        self.SHEET_ID = "1rA6u8Z03Tq9icAAIGP1Ki6FA9U_wFKjsKaizYLkMb_M"
        self.TAB_LINEUP = "LINEUP"
        self.SERVICE_ACCOUNT_FILE = r"D:\process\candidate_jobs_formate\service_account.json"

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

    # ---------------------------
    # SQL
    # ---------------------------
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
            `inst_date` AS sql_click_date,
            `inst_time` AS sql_click_time
        FROM candidate_jobs
        WHERE button_response_inst = 'Interested'
        ORDER BY created_date DESC
        """

        cursor.execute(query)
        rows = cursor.fetchall()
        conn.close()

        if not rows:
            print("⚠️ No 'Interested' rows found in SQL.")
            return pd.DataFrame([])

        df = pd.DataFrame(rows)

        # keep time as HH:MM:SS if extra junk exists
        if "sql_click_time" in df.columns:
            df["sql_click_time"] = df["sql_click_time"].astype(str).str.extract(r"(\d{2}:\d{2}:\d{2})")

        if "sql_click_date" not in df.columns:
            df["sql_click_date"] = ""
        if "sql_click_time" not in df.columns:
            df["sql_click_time"] = ""

        print(f"✅ Retrieved {len(df)} row(s) from SQL.")
        return df

    # ---------------------------
    # SHEET
    # ---------------------------
    def connect_to_sheet(self):
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(self.SERVICE_ACCOUNT_FILE, scope)
        self.gc = gspread.authorize(creds)
        self.ws = self.gc.open_by_key(self.SHEET_ID).worksheet(self.TAB_LINEUP)

        self.all_values = self.ws.get_all_values()
        if not self.all_values:
            raise RuntimeError("LINEUP tab needs a header row before appending data.")

        self.headers = self.all_values[0]
        self.norm_headers = [norm(h) for h in self.headers]

        print(f"✅ Connected to Google Sheet tab: {self.TAB_LINEUP}")
        print("Headers:", self.headers)

    def find_col(self, candidates, required_name):
        for i, h in enumerate(self.norm_headers):
            if h in candidates:
                return i
        raise RuntimeError(f"❌ Missing required column for {required_name}.")

    def build_sheet_index(self):
        idx_contact = self.find_col({norm("contact"), "mobile", "phonenumber"}, "Contact (phone_no)")
        idx_candidate_id = self.find_col({norm("candidate_id"), "candidateid", "cid"}, "candidate_id")
        idx_digit_instd_date = self.find_col({norm("digit_instd_date"), "digitinstddate", "digitinstdate"}, "digit_instd_date")
        idx_digit_instd_time = self.find_col({norm("digit_instd_time"), "digitinstdtime", "digitinsttime"}, "digit_instd_time")
        idx_digital_instd = self.find_col({norm("digital_instd"), "digitalinst", "digitalinsted"}, "digital_instd")

        # optional Date column
        idx_date = None
        for i, h in enumerate(self.norm_headers):
            if h in {norm("date"), "linedate", "lineupdate"}:
                idx_date = i
                break

        # phone -> list of row numbers
        existing_norm = {}
        for r, row in enumerate(self.all_values[1:], start=2):
            contact_raw = row[idx_contact].strip() if len(row) > idx_contact else ""
            p = normalize_phone(contact_raw)
            if p:
                existing_norm.setdefault(p, []).append(r)

        # next candidate_id from last non-empty candidate_id
        next_candidate_id = 1
        for row in reversed(self.all_values[1:]):
            if len(row) > idx_candidate_id:
                v = row[idx_candidate_id].strip()
                if v:
                    try:
                        next_candidate_id = int(v) + 1
                        break
                    except ValueError:
                        pass

        print(
            f"➡ Using columns (0-based): contact={idx_contact}, candidate_id={idx_candidate_id}, "
            f"digit_instd_date={idx_digit_instd_date}, digit_instd_time={idx_digit_instd_time}, "
            f"digital_instd={idx_digital_instd}, date={idx_date}"
        )
        print(f"➡ Next candidate_id will start from: {next_candidate_id}")
        print(f"➡ Indexed {len(existing_norm)} existing phone(s).")

        return {
            "idx_contact": idx_contact,
            "idx_candidate_id": idx_candidate_id,
            "idx_digit_instd_date": idx_digit_instd_date,
            "idx_digit_instd_time": idx_digit_instd_time,
            "idx_digital_instd": idx_digital_instd,
            "idx_date": idx_date,
            "existing_norm": existing_norm,
            "next_candidate_id": next_candidate_id,
        }

    # ---------------------------
    # FORMATTERS
    # ---------------------------
    def format_sql_date(self, value):
        if pd.isna(value) or value == "":
            return ""
        if isinstance(value, datetime):
            return value.strftime("%d-%m-%Y")

        s = str(value).strip()
        if not s:
            return ""
        if " " in s:
            s = s.split(" ")[0]

        # YYYY-MM-DD
        if len(s) == 10 and s[4] == "-" and s[7] == "-":
            y, m, d = s.split("-")
            return f"{d}-{m}-{y}"

        # DD-MM-YYYY
        if len(s) == 10 and s[2] == "-" and s[5] == "-":
            return s

        return ""

    def format_sql_time(self, value):
        if pd.isna(value) or value == "":
            return ""
        if isinstance(value, datetime):
            return value.strftime("%H:%M:%S")
        s = str(value).strip()
        return s[:8] if s else ""

    # ---------------------------
    # APPEND MAPPER
    # ---------------------------
    def map_sql_row_to_sheet(self, sql_row, candidate_id, idx, final_click_date, final_click_time):
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
                # your code forces "Lineup" on append
                out[col_idx] = "Lineup"
            elif nh in ("jobid", "job_id"):
                out[col_idx] = get("job_id")

        # candidate_id
        out[idx["idx_candidate_id"]] = str(candidate_id)

        # Date = today (if exists)
        if idx["idx_date"] is not None:
            out[idx["idx_date"]] = datetime.now().strftime("%d-%m-%Y")

        # digit click info (LATEST already computed per phone)
        out[idx["idx_digit_instd_date"]] = final_click_date or ""
        if idx["idx_digit_instd_time"] is not None:
            out[idx["idx_digit_instd_time"]] = final_click_time or ""

        # digital_instd = yes
        out[idx["idx_digital_instd"]] = "yes"

        return out

    # ---------------------------
    # CORE: BUILD LATEST CLICK PER PHONE (SQL)
    # ---------------------------
    def build_latest_sql_by_phone(self, df_sql):
        """
        Returns dict:
          phone -> {
            "dt": datetime,
            "date": "DD-MM-YYYY",
            "time": "HH:MM:SS",
            "row": pandas Series (latest row)
          }
        """
        latest = {}

        for _, r in df_sql.iterrows():
            phone = normalize_phone(r.get("phone_no"))
            if not phone:
                continue

            date_str = self.format_sql_date(r.get("sql_click_date"))
            time_str = self.format_sql_time(r.get("sql_click_time"))

            # If click date missing, we still may want to append, but "latest dt" can't be built.
            # So we use created_date fallback if possible, else skip from "latest click" map.
            dt = None

            if date_str:
                try:
                    dt = datetime.strptime(f"{date_str} {time_str or '00:00:00'}", "%d-%m-%Y %H:%M:%S")
                except Exception:
                    dt = None

            if dt is None:
                # fallback to created_date if present
                cd = r.get("created_date")
                try:
                    if isinstance(cd, datetime):
                        dt = cd
                    elif cd and str(cd).strip():
                        # try parse common mysql string
                        dt = datetime.fromisoformat(str(cd).replace(" ", "T"))
                except Exception:
                    dt = None

            if dt is None:
                # last fallback: very old constant, so any real dt will beat it
                dt = datetime(1900, 1, 1)

            if phone not in latest or dt > latest[phone]["dt"]:
                latest[phone] = {
                    "dt": dt,
                    "date": date_str or "",
                    "time": time_str or "",
                    "row": r,
                }

        print(f"✅ Latest SQL record resolved for {len(latest)} phone(s)")
        return latest

    # ---------------------------
    # APPLY SYNC (APPEND + UPDATE)
    # ---------------------------
    def apply_sync(self, df_sql, idx):
        existing_norm = idx["existing_norm"]
        next_candidate_id = idx["next_candidate_id"]

        idx_digit_instd_date = idx["idx_digit_instd_date"]
        idx_digit_instd_time = idx["idx_digit_instd_time"]
        idx_digital_instd = idx["idx_digital_instd"]

        latest_by_phone = self.build_latest_sql_by_phone(df_sql)

        update_requests = []
        append_buffer = []

        updated_rows = 0
        appended_rows = 0

        # --------
        # 1) UPDATE existing phones using latest click
        # --------
        for phone, rows in existing_norm.items():
            if phone not in latest_by_phone:
                continue

            latest = latest_by_phone[phone]
            latest_date = latest["date"]
            latest_time = latest["time"]

            # If no click date at all, don't overwrite sheet date with blank
            if not latest_date and not latest_time:
                continue

            for r in rows:
                row_data = self.all_values[r - 1]

                existing_date = ""
                if len(row_data) > idx_digit_instd_date:
                    existing_date = row_data[idx_digit_instd_date].strip()

                existing_time = ""
                if idx_digit_instd_time is not None and len(row_data) > idx_digit_instd_time:
                    existing_time = row_data[idx_digit_instd_time].strip()

                # If already identical, skip
                if (latest_date and existing_date == latest_date) and (not latest_time or existing_time == latest_time):
                    continue

                if latest_date:
                    a1 = gspread.utils.rowcol_to_a1(r, idx_digit_instd_date + 1)
                    update_requests.append({"range": a1, "values": [[latest_date]]})

                if idx_digit_instd_time is not None and latest_time:
                    a1 = gspread.utils.rowcol_to_a1(r, idx_digit_instd_time + 1)
                    update_requests.append({"range": a1, "values": [[latest_time]]})

                if idx_digital_instd is not None:
                    a1 = gspread.utils.rowcol_to_a1(r, idx_digital_instd + 1)
                    update_requests.append({"range": a1, "values": [["yes"]]})

                updated_rows += 1

        # --------
        # 2) APPEND phones not in sheet (one per phone, using latest SQL row)
        # --------
        for phone, payload in latest_by_phone.items():
            if phone in existing_norm:
                continue

            sql_row = payload["row"]
            latest_date = payload["date"]
            latest_time = payload["time"]

            new_row = self.map_sql_row_to_sheet(
                sql_row=sql_row,
                candidate_id=next_candidate_id,
                idx=idx,
                final_click_date=latest_date,
                final_click_time=latest_time,
            )
            append_buffer.append(new_row)
            appended_rows += 1
            print(f"➕ APPEND: phone={phone} candidate_id={next_candidate_id} date={latest_date} time={latest_time}")
            next_candidate_id += 1

        # --------
        # 3) APPEND rows with invalid/no phone (each as separate row)
        # --------
        # (kept similar to your append script; these cannot be matched to update later)
        no_phone_df = df_sql.copy()
        no_phone_df["__p"] = no_phone_df["phone_no"].apply(normalize_phone)
        no_phone_rows = no_phone_df[no_phone_df["__p"] == ""]

        for _, sql_row in no_phone_rows.iterrows():
            latest_date = self.format_sql_date(sql_row.get("sql_click_date"))
            latest_time = self.format_sql_time(sql_row.get("sql_click_time"))

            new_row = self.map_sql_row_to_sheet(
                sql_row=sql_row,
                candidate_id=next_candidate_id,
                idx=idx,
                final_click_date=latest_date,
                final_click_time=latest_time,
            )
            append_buffer.append(new_row)
            appended_rows += 1
            print(f"➕ APPEND (no/invalid phone): candidate_id={next_candidate_id}")
            next_candidate_id += 1

        # --------
        # WRITE to Sheet
        # --------
        if append_buffer:
            self.ws.append_rows(append_buffer, value_input_option="USER_ENTERED")
            print(f"✅ Appended {appended_rows} new row(s).")
        else:
            print("ℹ️ No rows to append.")

        if update_requests:
            self.ws.batch_update(update_requests)
            print(f"🛠️ Updated {updated_rows} existing row(s) with latest click data.")
        else:
            print("ℹ️ No rows required update.")

        print(f"\n📊 SUMMARY → SQL rows={len(df_sql)} | appended={appended_rows} | updated={updated_rows}")

    # ---------------------------
    # RUN
    # ---------------------------
    def run(self):
        print("🚀 Starting Finploy Lineup Sync (append + update)…")
        df_sql = self.fetch_sql_data()
        if df_sql.empty:
            print("🎯 Nothing to sync.")
            return

        self.connect_to_sheet()
        idx = self.build_sheet_index()
        self.apply_sync(df_sql, idx)
        print("🎯 Sync completed successfully.")


if __name__ == "__main__":
    print("▶ Starting phone_based_sync_combined.py ...")
    try:
        sync = FinployLineupSync()
        sync.run()
        print("✅ Script finished without unhandled errors.")
    except Exception as e:
        import traceback
        print("❌ Script crashed with an error:")
        print(e)
        traceback.print_exc()
# ---------------------------------------------------------
# Run blast_auto.py after SQL insertion is done
# ---------------------------------------------------------
try:
    import subprocess
    print("🚀 Running Not Interested to Screening  ...")
    subprocess.run(
        ["python", r"D:\matching_harsh\Job_matching_Screened\candidate_jobs_formate\sql_sheet_sn.py"],
        check=True
    )
    print("✔ Interested to Screening  executed successfully!")

except Exception as e:
    print(f"❌ Failed to run Interested to Screening : {e}")
 