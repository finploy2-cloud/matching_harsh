import tkinter as tk
from tkinter import messagebox
import pandas as pd
import mysql.connector
from datetime import datetime
import re
import random, string
import subprocess
import os
import requests

# =====================================================
# BACKEND EXCEL FILE
# =====================================================
EXCEL_FILE = r"D:\matching_harsh\Job_matching_unscreened\resume_output\output6.xlsx"
CV_OUTPUT_FOLDER = r"D:\matching_harsh\Job_matching_unscreened\cv_output"

# =====================================================
# SQL & UPLOAD CONFIG
# =====================================================
SQL_CONFIG = {
    "host": "65.0.211.89",
    "user": "harsh875_finploy_user1",
    "password": "Make*45@23+67",
    "database": "harsh875_finploy_com",
    "port": 3306
}

UPLOAD_URL = "https://www.finploy.com/upload_resume.php"
UPLOAD_TOKEN = "FinployUploadSecure2026"

TABLE_NAME = "candidate_details"

# =====================================================
# HELPERS
# =====================================================
def clean_phone(value):
    if pd.isna(value):
        return None
    digits = re.sub(r"\D", "", str(value))
    return digits[-10:] if len(digits) >= 10 else None

def generate_slug():
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=12))

def get_unique_slug(cursor):
    while True:
        slug = generate_slug()
        cursor.execute(
            "SELECT COUNT(*) FROM candidate_details WHERE candidate_getslug=%s",
            (slug,)
        )
        if cursor.fetchone()[0] == 0:
            return slug

def upload_file_to_cpanel(filename):
    """
    Uploads the resume file to the cPanel server via PHP endpoint.
    """
    file_path = os.path.join(CV_OUTPUT_FOLDER, filename)
    if not os.path.exists(file_path):
        print(f"⚠️  File not found locally: {filename}")
        # We return True here because technically the record can still be created, 
        # or you might want to stop. But let's assume we want to continue if it's missing but warn.
        # Actually, if the file is missing, the DB link will be broken. 
        # Better to return False if you want strict consistency.
        return False
    
    try:
        with open(file_path, "rb") as f:
            files = {"file": (filename, f)}
            payload = {"token": UPLOAD_TOKEN}
            # Add a timeout to prevent hanging the GUI
            response = requests.post(UPLOAD_URL, files=files, data=payload, timeout=30)
            
            if response.status_code == 200:
                print(f"✅  Uploaded to cPanel: {filename}")
                return True
            else:
                print(f"❌  Upload Failed for {filename}: {response.text}")
                return False
    except Exception as e:
        print(f"❌  Error during upload of {filename}: {e}")
        return False

# =====================================================
# MAIN PROCESS
# =====================================================
def start_process():

    try:
        conn = mysql.connector.connect(**SQL_CONFIG)
        cursor = conn.cursor(buffered=True)
    except Exception as e:
        messagebox.showerror("DB Error", f"Could not connect to database: {e}")
        return

    cursor.execute("SELECT COALESCE(MAX(id),0) FROM candidate_details")
    next_details_id = cursor.fetchone()[0] + 1

    try:
        df = pd.read_excel(EXCEL_FILE, engine="openpyxl")
    except Exception as e:
        messagebox.showerror("File Error", f"Could not read Excel file: {e}")
        conn.close()
        return

    column_mapping = {
        "name of candidate": "username",
        "clean_phone": "mobile_number",
        "experience": "work_experience",
        "education 2": "cv_highestqualification",
        "graduation_year": "cv_graduationyear",
        "company": "current_company",
        "designation": "destination",
        "clean_salary": "current_salary",
        "location": "current_location",
        "area": "area",
        "city": "city",
        "state": "state",
        "candidate_pincode": "pincode",
        "finploy_id": "location_code",
        "city_id": "city_wise_id",
        "Active /Inactive": "cv_parsingstatus",
        "actual_filename": "resume"
    }

    required_cols = [c for c in column_mapping if c in df.columns]
    mapped_df = df[required_cols].copy()
    mapped_df.rename(columns=column_mapping, inplace=True)

    products = product_var.get().strip()
    sub_products = sub_product_var.get().strip()
    departments = dept_var.get().strip()
    sub_departments = sub_dept_var.get().strip()
    specialization = spec_var.get().strip()
    category = category_var.get().strip()

    insert_count = 0
    update_count = 0
    upload_fail_count = 0

    print(f"🚀 Starting process for {len(mapped_df)} rows...")

    for _, row in mapped_df.iterrows():

        data = {}

        for col in mapped_df.columns:
            val = row.get(col)
            if pd.isna(val) or str(val).strip().lower() in ["", "nan", "none", "null"]:
                data[col] = None
            else:
                if col == "mobile_number":
                    data[col] = clean_phone(val)
                elif col == "resume":
                    fname = str(val).strip()
                    data[col] = fname if fname.lower().endswith(".pdf") else fname + ".pdf"
                else:
                    data[col] = str(val).strip()

        if not data.get("mobile_number") or not data.get("username"):
            continue

        # -------------------------------
        # UPLOAD RESUME TO CPANEL
        # -------------------------------
        if data.get("resume"):
            if not upload_file_to_cpanel(data["resume"]):
                upload_fail_count += 1
                # If upload fails, we skip this candidate to avoid broken links in DB
                print(f"⏩  Skipping DB sync for {data['username']} due to upload failure.")
                continue

        # -------------------------------
        # FETCH user_id FROM candidates
        # -------------------------------
        cursor.execute(
            "SELECT user_id FROM candidates WHERE mobile_number=%s",
            (data["mobile_number"],)
        )
        row_uid = cursor.fetchone()
        if not row_uid:
            print(f"ℹ️  Mobile {data['mobile_number']} not found in 'candidates' table. Skipping.")
            continue

        data["user_id"] = row_uid[0]

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        data.update({
            "modified": now,
            "cv_parsingtimestamp": now,
            "associate_id": 1,
            "associate_name": "Harsh",
            "associate_mobile": "8104748399",
            "products": products,
            "sub_products": sub_products,
            "departments": departments,
            "sub_departments": sub_departments,
            "specialization": specialization,
            "category": category
        })

        # -------------------------------
        # CHECK EXISTING
        # -------------------------------
        cursor.execute(
            "SELECT id, candidate_getslug FROM candidate_details WHERE mobile_number=%s",
            (data["mobile_number"],)
        )
        existing = cursor.fetchone()

        if existing:
            # ================= UPDATE =================
            details_id, slug = existing

            update_cols = []
            update_vals = []

            for k, v in data.items():
                if k == "mobile_number":
                    continue
                update_cols.append(f"{k}=%s")
                update_vals.append(v)

            update_vals.append(data["mobile_number"])

            sql = f"""
                UPDATE candidate_details
                SET {', '.join(update_cols)}
                WHERE mobile_number=%s
            """

            cursor.execute(sql, update_vals)
            conn.commit()
            update_count += 1

        else:
            # ================= INSERT =================
            slug = get_unique_slug(cursor)
            data["candidate_getslug"] = slug
            data["candidate_geturl"] = f"https://www.finploy.com/index.php?candidateaccessurl={slug}"
            data["id"] = next_details_id

            cols = ", ".join(data.keys())
            placeholders = ", ".join(["%s"] * len(data))
            sql = f"INSERT INTO candidate_details ({cols}) VALUES ({placeholders})"

            cursor.execute(sql, list(data.values()))
            conn.commit()

            next_details_id += 1
            insert_count += 1

    conn.close()

    status_msg = f"Inserted: {insert_count}\nUpdated: {update_count}"
    if upload_fail_count > 0:
        status_msg += f"\nUpload Failures (Skipped): {upload_fail_count}"
    
    messagebox.showinfo("Completed", status_msg)
    root.destroy()

# =====================================================
# GUI
# =====================================================
root = tk.Tk()
root.title("Finploy SQL & CV Sync Tool")
root.geometry("520x460")

labels = ["Products","Sub Products","Departments","Sub Departments","Specialization","Category"]
vars_map = {}

for i, lbl in enumerate(labels):
    tk.Label(root, text=lbl).grid(row=i, column=0, padx=10, pady=6)
    v = tk.StringVar()
    vars_map[lbl] = v
    tk.Entry(root, textvariable=v).grid(row=i, column=1)

product_var = vars_map["Products"]
sub_product_var = vars_map["Sub Products"]
dept_var = vars_map["Departments"]
sub_dept_var = vars_map["Sub Departments"]
spec_var = vars_map["Specialization"]
category_var = vars_map["Category"]

tk.Button(root, text="Start Sync & Upload", command=start_process, width=25, height=2, bg="#4CAF50", fg="white", font=("Arial", 10, "bold")).grid(row=7, column=0, columnspan=2, pady=25)

root.mainloop()

# =====================================================
# RUN NEXT SCRIPT
# =====================================================
try:
    subprocess.run(
        ["python", r"D:\matching_harsh\Job_matching_unscreened\main25.py"],
        check=False
    )
except Exception as e:
    print(f"❌ Failed to run next script: {e}")
