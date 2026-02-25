import pandas as pd
import os

# ============================================================
# CONFIGURATION
# ============================================================
input_path = r"D:\matching_harsh\Screening_Followup\final_output\all_job_matches_duplicate.xlsx"
output_dir = os.path.dirname(input_path)
output_path = os.path.join(output_dir, "filtered_candidates.xlsx")

# ============================================================
# LOAD FILE
# ============================================================
df = pd.read_excel(input_path)

# Convert key columns to string and strip spaces (for safety)
for col in ["contact", "job_composit_key", "job_company", "job_designation"]:
    df[col] = df[col].astype(str).str.strip()

# ============================================================
# REMOVE DUPLICATES BASED ON COMBINATION
# ============================================================
df_unique = df.drop_duplicates(
    subset=["contact", "job_composit_key", "job_company", "job_designation"],
    keep="first"
)

# ============================================================
# SAVE CLEANED OUTPUT
# ============================================================
df_unique.to_excel(output_path, index=False)

print("✅ Duplicate removal complete!")
print(f"📁 Cleaned file saved at: {output_path}")
print(f"🧮 Original rows: {len(df)} | Unique rows: {len(df_unique)}")
