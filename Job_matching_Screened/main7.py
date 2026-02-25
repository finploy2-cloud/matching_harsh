import pandas as pd
import os

# ============================================================
# CONFIGURATION
# ============================================================
input_path = r"D:\matching_harsh\Job_matching_Screened\final_output\all_job_matches\all_job_matches_filtered.xlsx"
output_dir = r"D:\matching_harsh\Job_matching_Screened\final_output"

unique_path = os.path.join(output_dir, "dedup_unique.xlsx")
duplicate_path = os.path.join(output_dir, "dedup_duplicates.xlsx")

# ============================================================
# LOAD FILE
# ============================================================
if not os.path.exists(input_path):
    raise FileNotFoundError(f"❌ Input file not found at: {input_path}")

df = pd.read_excel(input_path)
df.columns = df.columns.str.strip().str.lower()

# ✅ Required columns for deduplication logic
required_cols = ["candidate_id", "company_code", "job_composit_key"]
for col in required_cols:
    if col not in df.columns:
        raise KeyError(f"❌ Missing column: {col}")

# ============================================================
# DEDUPLICATION LOGIC
# ============================================================
before = len(df)

# ✅ Mark duplicates → same candidate_id + same company_code + same job_composit_key
dup_subset = ["candidate_id", "company_code", "job_composit_key"]
df["is_duplicate"] = df.duplicated(subset=dup_subset, keep="last")

# Split into two dataframes
df_unique = df[df["is_duplicate"] == False].copy()
df_duplicates = df[df["is_duplicate"] == True].copy()

# ============================================================
# FALLBACK IF NO DUPLICATES FOUND
# ============================================================
if df_duplicates.empty:
    print("⚠️ No duplicates found based on (candidate_id, company_code, job_composit_key).")
    print("ℹ️ Using all rows as duplicates for WhatsApp messaging continuation.")
    df_duplicates = df.copy()  # fallback: use entire dataset

# Drop helper column
df_unique.drop(columns=["is_duplicate"], inplace=True)
df_duplicates.drop(columns=["is_duplicate"], inplace=True)

after = len(df_unique)
removed = before - after

# ============================================================
# PRINT SUMMARY
# ============================================================
print(f"✅ Total rows before: {before}")
print(f"✅ Unique rows kept: {after}")
print(f"✅ Duplicates removed: {removed}")

# ============================================================
# SAVE OUTPUT FILES
# ============================================================
os.makedirs(output_dir, exist_ok=True)
df_unique.to_excel(unique_path, index=False)
df_duplicates.to_excel(duplicate_path, index=False)

print(f"\n💾 Unique file saved to: {unique_path}")
print(f"💾 Duplicates file saved to: {duplicate_path}")
print("🎯 Deduplication complete based on (candidate_id, company_code, job_composit_key).")
import subprocess
try:
    import subprocess
    print("▶️ Running main8.py ...")
    subprocess.run(["python", r"D:\matching_harsh\Job_matching_Screened\main8.py"], check=True)
    print("✅ main8.py executed successfully!")
except Exception as e:
    print(f"❌ Failed to run main8.py: {e}")