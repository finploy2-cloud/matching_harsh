# =========================
# main23.py
# =========================
import pandas as pd
import mysql.connector
from mysql.connector import errors
from datetime import datetime
import re
import time
import random
import subprocess
import json

# =====================================================
# CONFIGURATION
# =====================================================
EXCEL_FILE = r"D:\matching_harsh\Job_matching_unscreened\resume_output\output6.xlsx"
MAPPING_FILE = r"D:\matching_harsh\Job_matching_unscreened\mobile_userid_map.json"

SQL_CONFIG = {
    "host": "65.0.211.89",
    "user": "harsh875_finploy_user1",
    "password": "Make*45@23+67",
    "database": "harsh875_finploy_com",
    "port": 3306
}

TABLE_NAME = "candidates"
BATCH_SIZE = 10
MAX_RETRIES = 5

# =====================================================
# LOAD EXCEL
# =====================================================
df = pd.read_excel(EXCEL_FILE, engine="openpyxl")

if "clean_phone" not in df.columns:
    raise ValueError("❌ clean_phone column not found in output6.xlsx")

# =====================================================
# COLUMN MAPPING (❌ DO NOT CHANGE)
# =====================================================
column_mapping = {
    "name of candidate": "username",
    "clean_phone": "mobile_number",
    "Company": "companyname",
    "Designation": "jobrole",
    "location": "current_location",
    "clean_salary": "salary",
    "link href": "unique_link",
    "Modified": "updated"
}

available_cols = [c for c in column_mapping if c in df.columns]
df = df[available_cols].rename(columns=column_mapping)

# =====================================================
# CLEAN MOBILE NUMBER
# =====================================================
def normalize_phone(value):
    if pd.isna(value):
        return None
    digits = re.sub(r"\D", "", str(value))
    return digits[-10:] if len(digits) >= 10 else None

df["mobile_number"] = df["mobile_number"].apply(normalize_phone)
df = df[df["mobile_number"].notna()]
df = df[df["username"].notna()]
df = df.sort_values("mobile_number").reset_index(drop=True)

# =====================================================
# MYSQL CONNECTION
# =====================================================
conn = mysql.connector.connect(**SQL_CONFIG)
conn.autocommit = False
cursor = conn.cursor(buffered=True)

cursor.execute("SELECT COALESCE(MAX(user_id),0) FROM candidates")
next_user_id = cursor.fetchone()[0] + 1

print(f"➡ Starting inserts from user_id = {next_user_id}")

mobile_userid_map = {}

# =====================================================
# DEADLOCK SAFE EXECUTE
# =====================================================
def execute_with_retry(query, params):
    for attempt in range(MAX_RETRIES):
        try:
            cursor.execute(query, params)
            return
        except errors.InternalError as e:
            if e.errno == 1213:
                time.sleep((2 ** attempt) + random.random())
            else:
                raise
    raise RuntimeError("❌ Too many deadlock retries")

# =====================================================
# MAIN PROCESS
# =====================================================
for i in range(0, len(df), BATCH_SIZE):
    batch = df.iloc[i:i + BATCH_SIZE]

    try:
        for _, row in batch.iterrows():
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            data = row.to_dict()
            data.update({
                "associate_id": 1,
                "associate_name": "Harsh",
                "associate_mobile": "8104748399",
                "status": "active",
                "otp": "",
                "password": "",
                "created": now,
                "updated": data.get("updated") or now
            })

            execute_with_retry(
                "SELECT user_id FROM candidates WHERE mobile_number=%s",
                (data["mobile_number"],)
            )
            existing = cursor.fetchone()

            if existing:
                user_id = existing[0]
                mobile_userid_map[data["mobile_number"]] = user_id

                set_clause = ", ".join(f"{k}=%s" for k in data)
                execute_with_retry(
                    f"UPDATE candidates SET {set_clause} WHERE mobile_number=%s",
                    list(data.values()) + [data["mobile_number"]]
                )
            else:
                user_id = next_user_id
                next_user_id += 1
                data["user_id"] = user_id

                cols = ", ".join(data.keys())
                vals = ", ".join(["%s"] * len(data))

                execute_with_retry(
                    f"INSERT INTO candidates ({cols}) VALUES ({vals})",
                    list(data.values())
                )

                mobile_userid_map[data["mobile_number"]] = user_id

        conn.commit()
        print(f"✅ Batch committed ({i + len(batch)}/{len(df)})")

    except Exception as e:
        conn.rollback()
        raise

cursor.close()
conn.close()

# =====================================================
# SAVE MOBILE → USER_ID MAP
# =====================================================
with open(MAPPING_FILE, "w") as f:
    json.dump(mobile_userid_map, f)

print("🎉 Candidates sync complete")

# =====================================================
# RUN main24.py
# =====================================================
subprocess.run(
    ["python", r"D:\matching_harsh\Job_matching_unscreened\main24.py", MAPPING_FILE],
    check=True
)
