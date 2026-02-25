# main12_corrected_city_first.py
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import string

# -------------------------------
# Phase 2.5 – Final Integration with Candidate ID and Composit Key
# -------------------------------
OUTPUT_DIR = r"D:\matching_harsh\Job_matching_unscreened\output"
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# Input and Output
input_file = r"D:\matching_harsh\Job_matching_unscreened\output\output3.xlsx"
output_file = os.path.join(OUTPUT_DIR, "output4.xlsx")  # Updated main dataset

# -------------------------------
# Google Sheets setup
# -------------------------------
scope = ["https://spreadsheets.google.com/feeds",
         "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)
client = gspread.authorize(creds)

# Open Google Sheet by Sheet ID
sheet_id = "11Yye2zMLOgb0J8wBjH0VJNuOV28AAERNPxr3RE2OO-E"
sheet = client.open_by_key(sheet_id).sheet1

# Read all data and handle duplicate headers
headers = sheet.row_values(1)
seen = {}
unique_headers = []
for h in headers:
    h_clean = h.strip()
    if h_clean in seen:
        seen[h_clean] += 1
        h_clean = f"{h_clean}_{seen[h_clean]}"
    else:
        seen[h_clean] = 0
    unique_headers.append(h_clean)

data_rows = sheet.get_all_values()[1:]  # skip header row
df_location = pd.DataFrame(data_rows, columns=unique_headers)

# Ensure required columns exist
required_cols = ['area', 'city', 'state', 'city_wise_id', 'pincode', 'id']
for col in required_cols:
    if col not in df_location.columns:
        df_location[col] = '0'

# Strip spaces and punctuation
for col in ['area', 'city', 'id', 'city_wise_id', 'pincode']:
    df_location[col] = df_location[col].astype(str).str.strip().str.rstrip(string.punctuation)

# -------------------------------
# Load main candidate dataset
# -------------------------------
df_main = pd.read_excel(input_file, engine='openpyxl')

# Rename columns for consistency
rename_map = {}
if 'link' in df_main.columns:
    rename_map['link'] = 'name of candidate'
if 'meta-data' in df_main.columns:
    rename_map['meta-data'] = 'experience'
if 'education 2' in df_main.columns:
    rename_map['education 2'] = 'education 2'
if 'year' in df_main.columns:
    rename_map['year'] = 'graduation_year'
df_main.rename(columns=rename_map, inplace=True)

# Track unmatched locations
unmatched = []

# -------------------------------
# Map finploy_id and location metadata
# -------------------------------
def map_location(loc):
    loc_clean = str(loc).strip().lower().rstrip(string.punctuation)
    
    # Match city first
    match_city = df_location[df_location['city'].str.lower().str.rstrip(string.punctuation) == loc_clean]
    if not match_city.empty:
        row = match_city.iloc[0]
        return pd.Series([
            row['id'], row['area'], row['city'], row['state'], row.get('city_wise_id', '0'), row.get('pincode', '0')
        ])
    
    # Match area second
    match_area = df_location[df_location['area'].str.lower().str.rstrip(string.punctuation) == loc_clean]
    if not match_area.empty:
        row = match_area.iloc[0]
        return pd.Series([
            row['id'], row['area'], row['city'], row['state'], row.get('city_wise_id', '0'), row.get('pincode', '0')
        ])
    
    unmatched.append(loc)
    return pd.Series(['0','0','0','0','0','0'])

df_main[['finploy_id','area','city','state','city_id','candidate_pincode']] = df_main['location'].apply(map_location)

# -------------------------------
# Fix department and product from input file only
# -------------------------------
def fix_dept_prod(x):
    dept = x['department'] if pd.notna(x['department']) and str(x['department']).strip() != '' else '0'
    prod = x['product'] if pd.notna(x['product']) and str(x['product']).strip() != '' else '0'
    
    # Convert floats like 3.0 -> 3
    try:
        dept = str(int(float(dept))) if str(dept).replace('.','',1).isdigit() else str(dept)
        prod = str(int(float(prod))) if str(prod).replace('.','',1).isdigit() else str(prod)
    except:
        pass
    return pd.Series([dept, prod])

df_main[['department', 'product']] = df_main.apply(fix_dept_prod, axis=1)

# -------------------------------
# Generate composit_key
# -------------------------------
# -------------------------------
# Generate composit_key (salary preserved exactly)
# -------------------------------
df_main['city_id'] = df_main['city_id'].replace('', '0').fillna('0')

# Format clean_salary so 5.0 → 5 AND 8.5 stays 8.5
df_main['clean_salary_str'] = (
    df_main['clean_salary']
    .apply(lambda x: str(x).rstrip('0').rstrip('.') if '.' in str(x) else str(x))
)

df_main['composit_key'] = (
    df_main['city_id'].astype(str) + '_' +
    df_main['department'].astype(str) + '_' +
    df_main['product'].astype(str) + '_' +
    df_main['clean_salary_str']
)


# -------------------------------
# Insert candidate_id column at the beginning
# -------------------------------
df_main.insert(0, 'candidate_id', range(1, len(df_main)+1))

# -------------------------------
# Keep only final required columns
# -------------------------------
final_columns = [
    'candidate_id', 'name of candidate', 'link href', 'experience', 'education 2','graduation_year','meta-data 2', 'location',
    'name_location', 'employment-detail', 'designation','company', 'clean_salary',  'Modification', 'Activity',
    'finploy_id', 'area', 'city', 'state', 'city_id', 'department', 'product', 'composit_key', 'candidate_pincode',
]
df_final = df_main[final_columns]

# -------------------------------
# Save final cleaned dataset
# -------------------------------
df_final.to_excel(output_file, index=False)
print(f"Phase 2.5 completed successfully. Final file saved as '{output_file}'.")
try:
    import subprocess
    print("▶️ Running main13.py ...")
    subprocess.run(["python", r"D:\matching_harsh\Job_matching_unscreened\main13.py"], check=True)
    print("✅ main13.py executed successfully!")
except Exception as e:
    print(f"❌ Failed to run main13.py: {e}")
