import pandas as pd
import re
from datetime import datetime
import os
import subprocess
import time
import shutil
import win32com.client

# ==========================================================
# CONFIG
# ==========================================================
INPUT_FILE = r"D:\matching_harsh\Job_matching_unscreened\final_output\all_job_matches_phone_unique.xlsx"
OUTPUT_FILE = r"D:\matching_harsh\Job_matching_unscreened\resume_output\output5.xlsx"

NAUKRI_CV_FOLDER = r"D:\matching_harsh\Job_matching_unscreened\Naukri_cv"
CV_OUTPUT_FOLDER = r"D:\matching_harsh\Job_matching_unscreened\cv"

# ==========================================================
# LOAD FILE
# ==========================================================
df = pd.read_excel(INPUT_FILE, engine="openpyxl")

# ==========================================================
# HELPER: CLEAN PHONE NUMBER
# ==========================================================
def clean_phone(p):
    p = str(p)
    p = re.sub(r"\D", "", p)
    return p

# ==========================================================
# HELPER: GET TIMESTAMP (24 hr)
# ==========================================================
def get_timestamp():
    return datetime.now().strftime("%Y%m%d%H%M%S")

# ==========================================================
# 1️⃣ BUILD predi_filename
# FORMAT (NAUKRI EXACT):
# Naukri_<ONLY_ALPHABETS>[<y>y_<m>m]
# ==========================================================
def make_predi_filename(row):
    name_raw = str(row.get("name of candidate", "")).strip()

    # 🔥 CRITICAL FIX — MATCH NAUKRI LOGIC
    # Remove dots, spaces, symbols → KEEP ONLY A-Z
    name = re.sub(r"[^A-Za-z]", "", name_raw)

    exp = str(row.get("experience", "")).strip().lower()

    if not name:
        return ""

    if not exp:
        return f"Naukri_{name}"

    match_ym = re.search(r"(\d+)\s*y.*?(\d+)\s*m", exp)
    match_float = re.search(r"(\d+\.?\d*)", exp)

    if match_ym:
        y, m = match_ym.groups()
        return f"Naukri_{name}[{y}y_{m}m]"

    if match_float:
        val = float(match_float.group(1))
        y = int(val)
        m = int(round((val - y) * 10))
        return f"Naukri_{name}[{y}y_{m}m]"

    return f"Naukri_{name}"

df["predi_filename"] = df.apply(make_predi_filename, axis=1)

# ==========================================================
# 2️⃣ BUILD actual_filename
# Format => Finploy_<First8Chars>_<Phone>_<Timestamp>
# ==========================================================
def make_actual_filename(row):
    name_raw = str(row.get("name of candidate", "")).strip()
    name = re.sub(r"[^A-Za-z]", "", name_raw)

    phone = clean_phone(row.get("clean_phone", ""))
    ts = get_timestamp()

    if not name:
        return ""

    short_name = name[:8]
    return f"Finploy_{short_name}_{phone}_{ts}"

df["actual_filename"] = df.apply(make_actual_filename, axis=1)

# ==========================================================
# SAVE RESULT
# ==========================================================
df.to_excel(OUTPUT_FILE, index=False)
print("\n✅ Successfully updated:", OUTPUT_FILE)
print("🆕 Columns added → predi_filename, actual_filename\n")

# ==========================================================
# 3️⃣ CV CONVERSION: DOC / DOCX → PDF
# INPUT  : Naukri_cv
# OUTPUT : cv
# ==========================================================
os.makedirs(CV_OUTPUT_FOLDER, exist_ok=True)

print("\n▶️ Starting CV conversion (DOC/DOCX → PDF)")

word = win32com.client.DispatchEx("Word.Application")
word.Visible = False
time.sleep(2)

MAX_RETRIES = 3

try:
    for filename in os.listdir(NAUKRI_CV_FOLDER):
        input_path = os.path.join(NAUKRI_CV_FOLDER, filename)

        if not os.path.isfile(input_path):
            continue

        name, ext = os.path.splitext(filename)
        ext = ext.lower()
        output_pdf_path = os.path.join(CV_OUTPUT_FOLDER, name + ".pdf")

        if ext in [".doc", ".docx"]:
            print(f"Converting CV: {filename}")
            success = False

            for _ in range(MAX_RETRIES):
                try:
                    doc = word.Documents.Open(input_path, ReadOnly=True)
                    doc.SaveAs(output_pdf_path, FileFormat=17)
                    doc.Close(False)
                    success = True
                    break
                except Exception:
                    time.sleep(1)

            if not success:
                print(f"❌ Failed to convert: {filename}")

        elif ext == ".pdf":
            print(f"Copying CV PDF: {filename}")
            shutil.copy2(input_path, output_pdf_path)

finally:
    word.Quit()

print("✅ CV conversion completed.\n")

# ==========================================================
# RUN NEXT SCRIPT
# ==========================================================
try:
    print("▶️ Running main22.py ...")
    subprocess.run(
        ["python", r"D:\matching_harsh\Job_matching_unscreened\main22.py"],
        check=True
    )
    print("✅ main22.py executed successfully!")
except Exception as e:
    print(f"❌ Failed to run main22.py: {e}")
