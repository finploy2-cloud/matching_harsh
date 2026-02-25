import pandas as pd
import os

# ============================================================
# CONFIGURATION
# ============================================================
INPUT1_FILE = r"D:\matching_harsh\Job_matching_unscreened\final_input\input2.xlsx"   # new resdex file
INPUT2_FILE = r"D:\matching_harsh\Job_matching_unscreened\output\split_candidate\unique_candidates_1.xlsx"     # existing file
OUTPUT_FILE = r"D:\matching_harsh\Job_matching_unscreened\output\split_candidate\unique_candidates_1.xlsx"     # final output path

# ============================================================
# STEP 1 — LOAD FILES
# ============================================================
try:
    df1 = pd.read_excel(INPUT1_FILE, dtype=str)
    df2 = pd.read_excel(INPUT2_FILE, dtype=str)
    print(f"✅ Loaded input files successfully.")
except Exception as e:
    print(f"❌ Error loading files: {e}")
    raise SystemExit

# Clean column names
df1.columns = df1.columns.str.strip().str.lower()
df2.columns = df2.columns.str.strip().str.lower()

# ============================================================
# STEP 2 — CREATE name_location IN FILE 1
# ============================================================
if 'link' not in df1.columns or 'location' not in df1.columns:
    raise KeyError("❌ Input file 1 must have 'link' and 'location' columns.")

df1['link'] = df1['link'].fillna('').astype(str).str.strip()
df1['location'] = df1['location'].fillna('').astype(str).str.strip()
df1['name_location'] = df1['link'].str.strip() + "_" + df1['location'].str.strip()



print(f"✅ Created 'name_location' column in Input 1 with {len(df1)} rows.")

# Keep only columns we need for mapping
mapping_df = df1[['name_location', 'link href']].dropna(subset=['name_location'])

# ============================================================
# STEP 3 — MAP link href FROM FILE 1 TO FILE 2
# ============================================================
if 'name_location' not in df2.columns:
    raise KeyError("❌ Input file 2 must have 'name_location' column.")

if 'link href' not in df2.columns:
    raise KeyError("❌ Input file 2 must have 'link href' column.")

# Create a dictionary for fast lookup
mapping_dict = dict(zip(mapping_df['name_location'], mapping_df['link href']))

# Count matches
matches = df2['name_location'].isin(mapping_dict.keys()).sum()
print(f"🔍 Found {matches} matching 'name_location' records between both files.")

# Update link href where matched
df2['link href'] = df2.apply(
    lambda x: mapping_dict.get(x['name_location'], x['link href']),
    axis=1
)

# ============================================================
# STEP 4 — SAVE OUTPUT FILE
# ============================================================
os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
df2.to_excel(OUTPUT_FILE, index=False)
print(f"✅ Updated file saved successfully: {OUTPUT_FILE}")
