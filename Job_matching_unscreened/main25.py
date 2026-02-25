import pandas as pd
import mysql.connector
import os
import re

def clean_name(name):
    if pd.isna(name):
        return ""
    # Remove any non-letter characters (keep only a-zA-Z)
    cleaned = re.sub(r'[^a-zA-Z]', '', str(name))
    return cleaned

def clean_experience(exp):
    if pd.isna(exp):
        return ""
    # Replace space with "_"
    cleaned = str(exp).replace(' ', '_')
    return cleaned

def process_data():
    # File paths
    input_file = 'D:\matching_harsh\Job_matching_unscreened\output\output4.xlsx'
    output_file = 'D:\matching_harsh\Job_matching_unscreened\output\output5.xlsx'
    if not os.path.exists(input_file):
        print(f"Error: {input_file} not found.")
        return

    # Read output4.xlsx
    print(f"Reading {input_file}...")
    df = pd.read_excel(input_file)
    
    # Build resume column: Naukri_{name of candidate}[{experience}]
    print("Building 'resume' column...")
    df['resume'] = df.apply(
        lambda row: f"Naukri_{clean_name(row['name of candidate'])}[{clean_experience(row['experience'])}]", 
        axis=1
    )
    
    # Build naukri_composit_key column: first 3 segments of composit_key
    print("Building 'naukri_composit_key' column...")
    def get_naukri_composit_key(key):
        if pd.isna(key):
            return ""
        parts = str(key).split('_')
        return "_".join(parts[:3])
    
    df['naukri_composit_key'] = df['composit_key'].apply(get_naukri_composit_key)
    
    # Save output5.xlsx
    df.to_excel(output_file, index=False)
    print(f"Saved {output_file}")
    
    # Database Configuration
    db_config = {
        "host": "65.0.211.89",
        "database": "finployza_whatsapp",
        "user": "finployza_finploy",
        "password": "Taker*458",
        "port": 3306
    }
    
    # Column Mapping (Excel: DB)
    mapping = {
        'name of candidate': 'name_of_candidate',
        'experience': 'experience',
        'education 2': 'education',
        'graduation_year': 'graduation_year',
        'meta-data 2': 'meta_data',
        'location': 'location',
        'name_location': 'name_location',
        'designation': 'designation',
        'company': 'company',
        'clean_salary': 'clean_salary',
        'finploy_id': 'finploy_id',
        'area': 'area',
        'city': 'city',
        'state': 'state',
        'city_id': 'city_id',
        'department': 'department',
        'product': 'product',
        'composit_key': 'composit_key',
        'naukri_composit_key': 'naukri_composit_key',
        'candidate_pincode': 'candidate_pincode',
        'resume': 'resume'
    }
    
    # Filter mapping to only include columns actually present in the Excel file
    final_mapping = {k: v for k, v in mapping.items() if k in df.columns}
    insert_cols = list(final_mapping.values())
    
    try:
        print("Connecting to database...")
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        
        placeholders = ", ".join(["%s"] * len(insert_cols))
        # Use backticks for column names to avoid issues with reserved words or special characters
        cols_str = ", ".join([f"`{c}`" for c in insert_cols])
        # Update everything except id (auto-inc) and possibly unique keys if we want to preserve them
        # Here we update all mapped columns on duplicate
        update_str = ", ".join([f"`{c}`=VALUES(`{c}`)" for c in insert_cols])
        
        sql = f"INSERT INTO Naukri_Db ({cols_str}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_str}"
        
        print("Checking for existing records to calculate breakdown...")
        cursor.execute("SELECT name_location FROM Naukri_Db")
        existing_locations = {row[0] for row in cursor.fetchall() if row[0]}
        
        new_records = 0
        updated_records = 0
        
        print("Preparing data for upsert...")
        data_to_upsert = []
        for _, row in df.iterrows():
            loc = row['name_location']
            if loc in existing_locations:
                updated_records += 1
            else:
                new_records += 1
                
            row_data = []
            for excel_col in final_mapping.keys():
                val = row[excel_col]
                # Convert NaN to None for MySQL
                if pd.isna(val):
                    val = None
                row_data.append(val)
            data_to_upsert.append(tuple(row_data))
            
        print(f"Total records in file: {len(data_to_upsert)}")
        print(f"Records to be Inserted (New): {new_records}")
        print(f"Records to be Updated (Existing): {updated_records}")
        
        print(f"Performing upsert...")
        cursor.executemany(sql, data_to_upsert)
        conn.commit()
        print(f"Successfully upserted data. Total rows affected value (DB reports 1 for insert, 2 for update): {cursor.rowcount}")
        
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error during database operation: {e}")

if __name__ == "__main__":
    process_data()
# =====================================================
# RUN NEXT SCRIPT
# =====================================================
# import subprocess
# try:
#     subprocess.run(
#         ["python", r"D:\matching_harsh\Job_matching_unscreened\candidate_jobs_formate\main.py"],
#         check=False
#     )
# except Exception as e:
#     print(f"❌ Failed to run next script: {e}")