import pandas as pd
import os

# ============================================================
# CONFIGURATION
# ============================================================
input1_path = r"D:\matching_harsh\Job_matching_unscreened\output\All_job_match_sumit.xlsx"
input2_path = r"D:\matching_harsh\Job_matching_unscreened\final_output\all_job_matches_phone.xlsx"
output_path = r"D:\matching_harsh\Job_matching_unscreened\sumit\all_job_matches_sumit_final.xlsx"

# Ensure output folder exists
os.makedirs(os.path.dirname(output_path), exist_ok=True)

# ============================================================
# STEP 1: LOAD FILES
# ============================================================
df1 = pd.read_excel(input1_path)
df2 = pd.read_excel(input2_path)

# ============================================================
# STEP 2: SELECT ONLY candidate_id AND clean_phone FROM input2
# ============================================================
df_phone = df2[['candidate_id', 'clean_phone']].drop_duplicates(subset='candidate_id')

# ============================================================
# STEP 3: MERGE INTO INPUT1 USING candidate_id
# ============================================================
df_final = df1.merge(df_phone, on='candidate_id', how='left')

# ============================================================
# STEP 4: SAVE FINAL OUTPUT
# ============================================================
df_final.to_excel(output_path, index=False)

# ============================================================
# STEP 5: PRINT SUMMARY
# ============================================================
print("✅ Merge completed successfully!")
print(f"📁 Output file saved at: {output_path}")
print(f"📊 Input1 rows: {len(df1)} | Final file rows: {len(df_final)}")


try:
    import subprocess
    print("▶️ Running main18.py ...")
    subprocess.run(["python", r"D:\matching_harsh\Job_matching_unscreened\main18.py"], check=True)
    print("✅ main18.py executed successfully!")
except Exception as e:
    print(f"❌ Failed to run main18.py: {e}")