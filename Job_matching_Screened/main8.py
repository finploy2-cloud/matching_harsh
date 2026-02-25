import pandas as pd
from datetime import datetime, timedelta
import os

# -----------------------------------------
# Step 1. Configuration
# -----------------------------------------
OUTPUT_DIR = r"D:\matching_harsh\Job_matching_Screened\Densta_output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# -----------------------------------------
# Step 2. Take input file path dynamically
# -----------------------------------------
input_file = r'D:\matching_harsh\Job_matching_Screened\final_output\all_job_matches\all_job_matches_unique_phone.xlsx'

# -----------------------------------------
# Step 3. Generate output file name (01DDMMYY01)
# Example: 0107102501 for 07 Oct 2025
# -----------------------------------------
today = datetime.now()
file_prefix = "10"
file_suffix = "01"
file_date = today.strftime("%d%m%y")  # DDMMYY
file_name_no_ext = f"{file_prefix}{file_date}{file_suffix}"
output_file = os.path.join(OUTPUT_DIR, f"{file_name_no_ext}.xlsx")

# -----------------------------------------
# Step 4. Load input Excel
# -----------------------------------------
df = pd.read_excel(input_file)

# -----------------------------------------
# Step 5. Helper: Extract first name
# -----------------------------------------
def get_first_name(full_name):
    if isinstance(full_name, str):
        return full_name.strip().split(" ")[0]
    return "0"

# -----------------------------------------
# Step 6. Build the new DataFrame with required columns
# -----------------------------------------
df_output = pd.DataFrame({
    'vendor_lead_code': 0,
    'source_id': 0,
    'list_id': file_name_no_ext,
    'phone_code': 0,
    'Phone_number': df.get('clean_phone', '0'),
    'title': 0,
    'first_name': 0,
    'middle_initial': 0,
    'last_name': df['name of candidate'],
    'address1': df['composit_key'],
    'address2': df.get('company', '0'),
    'address3': df.get('designation', '0'),
    'city': df.get('location', '0'),
    'state': str(df.get('clean_salary', '0')),
    'province': 0,
    'postal_code': 0,
    'country': 0,
    'gender': 0,
    'birth_date': 0,
    'alt_phone': 0,
    'email': 0,
    'security_phrase': 0,
    'comments': 0
})

# -----------------------------------------
# Step 7. Save final output file
# -----------------------------------------
df_output.to_excel(output_file, index=False)

# -----------------------------------------
# Step 8. Print summary
# -----------------------------------------
print("✅ File generated successfully!")
print(f"📄 Output file path: {output_file}")
print(f"📊 Total records processed: {len(df_output)}")
print(f"🆔 List ID used: {file_name_no_ext}")
import subprocess
try:
    import subprocess
    print("▶️ Running main.py ...")
    subprocess.run(["python", r"D:\matching_harsh\Job_matching_Screened\candidate_jobs_formate\main.py"], check=True)
    print("✅ main.py executed successfully for blasting !")
except Exception as e:
    print(f"❌ Failed to run main.py for blasting : {e}")