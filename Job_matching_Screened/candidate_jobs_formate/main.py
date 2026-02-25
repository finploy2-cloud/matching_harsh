import re
from pathlib import Path
from contextlib import closing
import subprocess

import pandas as pd
from datetime import datetime
from openpyxl.utils import get_column_letter

try:
    import mysql.connector
    from mysql.connector import Error as MySQLError
except ImportError:
    mysql = None
    MySQLError = Exception
else:
    mysql = mysql.connector

INPUT_FILE = Path(r"D:\matching_harsh\Job_matching_Screened\final_output\dedup_unique.xlsx")
OUTPUT_DIR = Path(r"D:\matching_harsh\Job_matching_Screened\candidate_jobs_formate\output")
OUTPUT_FILENAME = "finploy_template.xlsx"

SQL_DB_CONFIG = {
    "host": "65.0.211.89",
    "user": "harsh875_finploy_user1",
    "password": "Make*45@23+67",
    "database": "harsh875_finploy_com",
    "port": 3306,
}
SQL_TABLE_NAME = "candidate_jobs"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------
# Template definition
# ---------------------------
TEMPLATE_COLUMNS = [
    "sr_no",
    "candidate_id",
    "job_id",
    "phone_no",
    "slug",
    "sent_slug",
    "candidate_username",
    "candidate_current_company",
    "candidate_destination",
    "candidate_current_location",
    "candidate_current_salary",
    "candidate_name",
    "salary",
    "role",
    "Company",
    "click_timestamp",
    "job_company",
    "job_location",
    "button_response_inst",
    "posted_1",
    "Location",
    "button_response",
    "posted",
    "NOT_INTERESTED",
    "date",
    "time",
    "created_date",
    "finploy_hr",
    "comment",
    "lineupId",
    "lineup_company_hr",
    "lineup_remark",
    "lineup_comment",
    "lineup_date",
    "lineup_finploy_hr",
    "company_hr",
    "finploy_id",
    "finploy_area",
    "finploy_city_id",
    "finploy_city",
    "finploy_state",
    "inst_date",
    "inst_time"
]

# Each template column mapped to the normalized input header it should copy.
COLUMN_MAP = {
    "job_id": "job_id",
    "phone_no": "clean_phone",
    "candidate_name": "name_of_candidate",
    "salary": "job_salary",
    "role": "job_designation",
    "Company": "job_company",
    "Location": "job_location",
    "candidate_username":"name_of_candidate",
    "candidate_current_company":"company",
    "candidate_destination":"designation",
    "candidate_current_location":"location",
    "candidate_current_salary":"clean_salary",
    "company_hr":"job_hr_name",
    "finploy_id":"finploy_id",
    "finploy_area":"area",
    "finploy_city_id":"city_id",
    "finploy_city":"city",
    "finploy_state":"state"
}
def fetch_next_sr_no() -> int:
    if mysql is None:
        return 1
    try:
        with closing(mysql.connect(**SQL_DB_CONFIG)) as connection, closing(connection.cursor()) as cursor:
            cursor.execute(f"SELECT MAX(sr_no) FROM {SQL_TABLE_NAME}")
            max_sr = cursor.fetchone()[0]
            return (max_sr or 0) + 1
    except MySQLError as exc:
        print(f"[ERROR] Could not read sr_no from SQL: {exc}")
        raise SystemExit(1)
    
# ✅ NEW FUNCTION: Fetch next candidate_id
def fetch_next_candidate_id() -> int:
    if mysql is None:
        return 1
    try:
        with closing(mysql.connect(**SQL_DB_CONFIG)) as connection, closing(connection.cursor()) as cursor:
            cursor.execute(f"SELECT MAX(CAST(candidate_id AS UNSIGNED)) FROM {SQL_TABLE_NAME}")
            max_id = cursor.fetchone()[0]
            return (max_id or 0) + 1
    except MySQLError as exc:
        print(f"[ERROR] Could not read candidate_id from SQL: {exc}")
        raise SystemExit(1)

def normalize_column_name(name: str) -> str:
    """Normalize headers by lowercasing and collapsing punctuation/spaces."""
    normalized = re.sub(r"[^0-9a-zA-Z]+", "_", name.strip().lower())
    normalized = re.sub(r"_+", "_", normalized)
    return normalized.strip("_")


# ---------------------------
# Load the input file
# ---------------------------
if not INPUT_FILE.exists():
    print(f"[ERROR] Input file not found: {INPUT_FILE}")
    raise SystemExit(1)

try:
    df = pd.read_excel(INPUT_FILE, dtype=str)
    print(f"[OK] Loaded input file with {len(df)} rows.")
except Exception as exc:
    print(f"[ERROR] Could not load input file: {exc}")
    raise SystemExit(1)

if df.empty:
    print("[WARN] Input spreadsheet is empty. A header-only workbook will be generated.")

# Clean up the data for reliable mapping.
df = df.fillna("").applymap(lambda value: str(value).strip())

# Prepare lookup from normalized header -> original header.
normalized_lookup = {
    normalize_column_name(original): original for original in df.columns
}

# ---------------------------
# Shape data into the template
# ---------------------------
row_count = len(df.index)
template_data = {column: [""] * row_count for column in TEMPLATE_COLUMNS}
template_df = pd.DataFrame(template_data)

starting_sr_no = fetch_next_sr_no()
starting_candidate_id = fetch_next_candidate_id()

 
if row_count:
    template_df["sr_no"] = list(range(starting_sr_no, starting_sr_no + row_count))
    template_df["candidate_id"] = list(range(starting_candidate_id, starting_candidate_id + row_count))

missing_sources = []
for target_column, source_key in COLUMN_MAP.items():
    source_column = normalized_lookup.get(source_key)
    if not source_column:
        missing_sources.append(source_key)
        continue
    template_df[target_column] = df[source_column]

# Stamp the current timestamp into created_date if present.
if "created_date" in template_df.columns:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if row_count:
        template_df["created_date"] = [timestamp] * row_count

# Fill remaining empty cells with zero (excluding slug, which gets a formula).
if row_count:
    zero_fill_columns = [col for col in TEMPLATE_COLUMNS if col != "slug"]
    template_df[zero_fill_columns] = template_df[zero_fill_columns].replace("", "0")

if row_count:
    slug_values = (
        template_df["candidate_id"].astype(str)
        + "_"
        + template_df["job_id"].astype(str)
        + "_"
        + template_df["sr_no"].astype(str)
    )
    template_df["slug"] = slug_values
    template_df["sent_slug"] = slug_values

if missing_sources:
    missing_list = ", ".join(sorted(set(missing_sources)))
    print(f"[WARN] Missing columns in source file: {missing_list}")
else:
    print("[OK] All mapped columns were found in the input file.")

# ---------------------------
# Save the output workbook
# ---------------------------
output_path = OUTPUT_DIR / OUTPUT_FILENAME
try:
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        template_df.to_excel(writer, index=False, header=True)
        sheet_name = next(iter(writer.sheets))
        sheet = writer.sheets[sheet_name]

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
     template_df.to_excel(writer, index=False, header=True)

    print(f"[OK] Template workbook generated successfully: {output_path}")
except Exception as exc:
    print(f"[ERROR] Could not sa  ve workbook: {exc}")
    raise SystemExit(1)

# ---------------------------------------------------
# Run sql_insertion.py AFTER successful template creation
# ---------------------------------------------------
try:
    import subprocess
    print("🟦 Running sql_insertion.py...")
    subprocess.run(
        ["python", r"D:\matching_harsh\Job_matching_Screened\candidate_jobs_formate\sql_insertion.py"],
        check=True
    )
    print("✔ sql_insertion.py executed successfully!")

except Exception as e:
    print(f"❌ Failed to run sql_insertion.py: {e}")

