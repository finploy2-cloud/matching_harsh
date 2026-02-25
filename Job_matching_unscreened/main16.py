import pandas as pd
import re
import os

# ============================================================
# CONFIGURATION
# ============================================================
phones_file = r"D:\matching_harsh\Job_matching_unscreened\final_input\resdex_phone.xlsx"
matches_file = r"D:\matching_harsh\Job_matching_unscreened\output\All_job_match.xlsx"

# Output paths
output_file = r"D:\matching_harsh\Job_matching_unscreened\final_output\all_job_matches_phone.xlsx"
unique_file = r"D:\matching_harsh\Job_matching_unscreened\final_output\all_job_matches_phone_unique.xlsx"

# Ensure directories exist
os.makedirs(os.path.dirname(unique_file), exist_ok=True)

# ============================================================
# LOAD INPUT FILES
# ============================================================
df_phones = pd.read_excel(phones_file)
df_matches = pd.read_excel(matches_file)

# ============================================================
# STEP 1: RENAME 'link' COLUMN TO 'name of candidate'
# ============================================================
df_phones.rename(columns={'link': 'name of candidate'}, inplace=True)

# ============================================================
# STEP 2: CREATE 'name_location' COLUMN
# ============================================================
df_phones['name_location'] = (
    df_phones['name of candidate'].fillna('').astype(str).str.strip() + '_' +
    df_phones['location'].fillna('').astype(str).str.strip()
).str.replace('__', '_').str.strip('_')

df_matches['name_location'] = (
    df_matches['name of candidate'].fillna('').astype(str).str.strip() + '_' +
    df_matches['location'].fillna('').astype(str).str.strip()
).str.replace('__', '_').str.strip('_')

# ============================================================
# STEP 3: CLEAN PHONE NUMBERS
# ============================================================
def clean_phone_number(phone):
    if pd.isna(phone):
        return None
    digits = re.sub(r'\D', '', str(phone))
    if len(digits) >= 10:
        return digits[-10:]
    return None

df_phones['clean_phone'] = df_phones['NnYPh'].apply(clean_phone_number)

# ============================================================
# STEP 4: PREPARE PHONE MAPPING
# ============================================================
# Create two mapping dictionaries
# 1. Strict Map: (Name, Location) -> Phone
# 2. Relaxed Map: Name -> Phone (only for names that appear exactly ONCE in the phone list)

# Clean names for better matching
df_phones['name_clean'] = df_phones['name of candidate'].str.lower().str.strip()
df_matches['name_clean'] = df_matches['name of candidate'].str.lower().str.strip()

# 1. Strict Mapping (Name + Location)
strict_map = df_phones.set_index('name_location')['clean_phone'].to_dict()

# 2. Relaxed Mapping (Name only - Takes the FIRST phone found for this name)
relaxed_map = df_phones.drop_duplicates(subset=['name_clean']).set_index('name_clean')['clean_phone'].to_dict()

# ============================================================
# STEP 5: APPLY MAPPING
# ============================================================
def get_phone(row):
    # Try strict match first (Name + Location)
    phone = strict_map.get(row['name_location'])
    if pd.notna(phone) and str(phone).strip() != '':
        return phone
    
    # Fallback: Try name match only (In case location is different/wrong)
    phone = relaxed_map.get(row['name_clean'])
    return phone

df_merged = df_matches.copy()
df_merged['clean_phone'] = df_matches.apply(get_phone, axis=1)

# ============================================================
# STEP 6: REORDER COLUMNS (PLACE clean_phone AFTER 5th COLUMN)
# ============================================================
cols = df_merged.columns.tolist()
if 'clean_phone' in cols:
    cols.insert(5, cols.pop(cols.index('clean_phone')))
df_merged = df_merged[cols]

# ============================================================
# STEP 7: REMOVE DUPLICATES BASED ON name_location
# ============================================================
df_unique = df_merged.drop_duplicates(subset=['name_location'], keep='first')

# ============================================================
# STEP 8: SAVE OUTPUT FILES
# ============================================================
df_merged.to_excel(output_file, index=False)
df_unique.to_excel(unique_file, index=False)

# ============================================================
# STEP 9: SUMMARY
# ============================================================
missing_phones = df_unique['clean_phone'].isna().sum()
print(f"✅ Merged file saved to: {output_file}")
print(f"✅ Unique file saved to: {unique_file}")
print(f"📊 Total Matches: {len(df_merged)}")
print(f"👥 Unique Candidates: {len(df_unique)}")
print(f"📞 Candidates with Phone: {len(df_unique) - missing_phones}")
print(f"⚠️ Candidates still missing Phone: {missing_phones}")

if missing_phones > 0:
    print("\nMissing phones for:")
    print(df_unique[df_unique['clean_phone'].isna()][['name of candidate', 'location']])

try:
    import subprocess
    print("▶️ Running main17.py ...")
    subprocess.run(["python", r"D:\matching_harsh\Job_matching_unscreened\main17.py"], check=True)
    print("✅ main17.py executed successfully!")
except Exception as e:
    print(f"❌ Failed to run main17.py: {e}")