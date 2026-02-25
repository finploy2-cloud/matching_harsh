import os
import zipfile
import pandas as pd
import shutil
from multiprocessing import Pool, cpu_count
from PIL import Image
from playwright.sync_api import sync_playwright
from docx import Document   # NEW → for offline DOCX to text

# =====================================================
# CONFIGURATION
# =====================================================
EXCEL_FILE = r"D:\matching_harsh\Job_matching_unscreened\resume_output\output5.xlsx"
RESUME_FOLDER = r"D:\matching_harsh\Job_matching_unscreened\cv"
OUTPUT_FOLDER = r"D:\matching_harsh\Job_matching_unscreened\cv_output"

MATCHED_FOLDER = OUTPUT_FOLDER
UNMATCHED_FOLDER = os.path.join(OUTPUT_FOLDER, "unmatched")
LOG_FILE = os.path.join(OUTPUT_FOLDER, "not_found.txt")
OUTPUT_EXCEL = r"D:\matching_harsh\Job_matching_unscreened\resume_output\output6.xlsx"

os.makedirs(MATCHED_FOLDER, exist_ok=True)
os.makedirs(UNMATCHED_FOLDER, exist_ok=True)

# =====================================================
# LOAD EXCEL MAPPING (ALL CANDIDATES)
# =====================================================
df = pd.read_excel(EXCEL_FILE, engine="openpyxl")

# mapping: predi_filename → actual_filename
predi_map = {
    str(r["predi_filename"]).strip().lower(): str(r["actual_filename"]).strip()
    for _, r in df.iterrows()
}


# =====================================================
# DEBUG MISMATCH
# =====================================================
def debug_mismatches():
    print("\n============================")
    print("🔍 DEBUG — Checking filename mismatches")
    print("============================\n")

    fs = [os.path.splitext(f)[0].lower() for f in os.listdir(RESUME_FOLDER)]

    excel_keys = list(predi_map.keys())

    # files present but not in excel
    print("❌ FILES NOT FOUND IN EXCEL (will be unmatched):")
    for f in fs:
        if f not in excel_keys:
            print("   -", f)

    # excel keys but missing files
    print("\n❗ EXCEL KEYS THAT DON'T HAVE A FILE:")
    for k in excel_keys:
        if k not in fs:
            print("   -", k)

    print("\n============================")
    print("🔍 DEBUG COMPLETE")
    print("============================\n")


# Run debug before processing
debug_mismatches()


# =====================================================
# DETECT HTML CONTENT
# =====================================================
def is_html_file(path):
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            chunk = f.read(8000).lower()
            return "<html" in chunk or "<!doctype" in chunk or "<div" in chunk
    except:
        return False


# =====================================================
# VALID DOCX CHECK
# =====================================================
def is_valid_docx(path):
    return zipfile.is_zipfile(path)


# =====================================================
# DOCX → HTML (offline)
# =====================================================
def docx_to_html(in_path, out_html):

    try:
        doc = Document(in_path)
        html_lines = ["<html><body>"]

        for para in doc.paragraphs:
            html_lines.append(f"<p>{para.text}</p>")

        html_lines.append("</body></html>")

        with open(out_html, "w", encoding="utf-8") as f:
            f.write("\n".join(html_lines))

        return True

    except Exception as e:
        print("❌ DOCX->HTML FAILED:", in_path, e)
        return False


# =====================================================
# PLAYWRIGHT HTML → PDF
# =====================================================
def html_to_pdf(html_file, pdf_out):
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page()

            page.goto(f"file:///{html_file.replace(os.sep, '/')}", wait_until="load")

            page.pdf(path=pdf_out, format="A4", print_background=True)
            browser.close()

        return True

    except Exception as e:
        print("❌ HTML->PDF FAILED:", html_file, e)
        return False


# =====================================================
# UNIVERSAL CV CONVERTER
# =====================================================
def convert_file(filename):

    src = os.path.join(RESUME_FOLDER, filename)
    name_no_ext = os.path.splitext(filename)[0].lower().strip()
    ext = os.path.splitext(filename)[1].lower()

    # If file name not present in mapping → unmatched
    if name_no_ext not in predi_map:
        shutil.copy2(src, os.path.join(UNMATCHED_FOLDER, filename))
        return ("unmatched", filename)

    dest_name = predi_map[name_no_ext] + ".pdf"
    dest = os.path.join(MATCHED_FOLDER, dest_name)

    # CASE 1 — Already PDF
    if ext == ".pdf":
        shutil.copy2(src, dest)
        return ("matched", filename)

    # CASE 2 — Images
    if ext in [".jpg", ".jpeg", ".png"]:
        try:
            img = Image.open(src)
            img.convert("RGB").save(dest, "PDF")
            return ("matched", filename)
        except:
            pass

    # CASE 3 — HTML disguised as doc
    if is_html_file(src):
        try:
            html_path = src + ".html"
            shutil.copy2(src, html_path)

            if html_to_pdf(html_path, dest):
                os.remove(html_path)
                return ("matched", filename)
        except:
            pass

    # CASE 4 — DOCX
    if ext == ".docx" and is_valid_docx(src):
        html_file = src + "_tmp.html"

        if docx_to_html(src, html_file):
            if html_to_pdf(html_file, dest):
                os.remove(html_file)
                return ("matched", filename)

        # fallback
        shutil.copy2(src, os.path.join(UNMATCHED_FOLDER, filename))
        return ("unmatched", filename)

    # CASE 5 — DOC fallback
    if ext == ".doc":
        try:
            with open(src, "r", encoding="utf-8", errors="ignore") as f:
                text = f.read()

            html_file = src + "_tmp.html"
            with open(html_file, "w", encoding="utf-8") as hf:
                hf.write(f"<html><body><pre>{text}</pre></body></html>")

            if html_to_pdf(html_file, dest):
                os.remove(html_file)
                return ("matched", filename)
        except:
            pass

    # LAST fallback — unmatched
    shutil.copy2(src, os.path.join(UNMATCHED_FOLDER, filename))
    return ("unmatched", filename)


# =====================================================
# MAIN EXECUTION
# =====================================================
if __name__ == "__main__":

    files = [
        f for f in os.listdir(RESUME_FOLDER)
        if os.path.isfile(os.path.join(RESUME_FOLDER, f))
    ]

    print(f"\n🚀 Starting 100% conversion using {cpu_count()} CPU cores...\n")

    with Pool(cpu_count()) as pool:
        results = pool.map(convert_file, files)

    matched = [f for s, f in results if s == "matched"]
    unmatched = [f for s, f in results if s == "unmatched"]

    # Save unmatched filenames
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        for u in unmatched:
            f.write(u + "\n")

    # =====================================================
    # CREATE FINAL OUTPUT WITH MATCHED = YES / NO
    # =====================================================

    # Normalize matched file names
    matched_keys = set(os.path.splitext(m.lower())[0] for m in matched)

    # Add Matched column for ALL 30 candidates
    df["Matched"] = df["predi_filename"].str.strip().str.lower().apply(
        lambda x: "Yes" if x in matched_keys else "No"
    )

    # Save all rows to output6.xlsx
    df.to_excel(OUTPUT_EXCEL, index=False)

    print("\n========== SUMMARY ==========")
    print("✔ Total Candidates:", len(df))
    print("✔ CV Matched:", df["Matched"].eq("Yes").sum())
    print("❌ CV Unmatched:", df["Matched"].eq("No").sum())
    print("📂 Output Excel:", OUTPUT_EXCEL)
    print("================================")
# =====================================================
# RUN NEXT SCRIPT
# =====================================================
try:
    import subprocess
    print("▶️ Running main23.py ...")
    subprocess.run(["python", r"D:\matching_harsh\Job_matching_unscreened\main23.py"], check=True)
    print("✅ main23.py executed successfully!")
except Exception as e:
    print(f"❌ Failed to run main23.py: {e}")
