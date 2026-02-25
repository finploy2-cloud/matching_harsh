import pandas as pd
import os

# =============================================================================
# CONFIGURATION
# =============================================================================
JOBS_FILE = r'D:\matching_harsh\Job_matching_unscreened\final_input\MASTER FILE LOCATIONS - Mapping.xlsx'
CANDIDATES_FILE = r'D:\matching_harsh\Job_matching_unscreened\output\output4.xlsx'
OUTPUT_DIR = r'D:\matching_harsh\Job_matching_unscreened\output'
SPLIT_DIR = os.path.join(OUTPUT_DIR, "split_candidate")  # ✅ new folder for split files

# Column names
JOBS_COLUMNS = {
    'job_id_col': 'job_id',
    'composite_key_col': 'composit_key',
    'date_col': 'Date',
    'company_col': 'Company',   
    'designation_col': 'Designation',
    'location_col': 'Client location',
    'hr_name_col': 'HR Name',
    'status_col': 'Active /Inactive',
    'company_code': 'company_code'
}
CANDIDATES_COLUMNS = {
    'candidate_id_col': 'candidate_id',
    'composite_key_col': 'composit_key'
}

# =============================================================================
# LOAD FILES
# =============================================================================
def load_excel_data():
    try:
        jobs_df = pd.read_excel(JOBS_FILE)
        candidates_df = pd.read_excel(CANDIDATES_FILE)
        print(f"✅ Loaded {len(jobs_df)} jobs and {len(candidates_df)} candidates.")
        return jobs_df, candidates_df
    except Exception as e:
        print(f"❌ Error loading Excel files: {e}")
        return None, None


# =============================================================================
# PARSE COMPOSITE KEYS
# =============================================================================
def parse_composite_key(key_str):
    if pd.isna(key_str) or '_' not in str(key_str) or str(key_str).count('_') != 3:
        return None, None, None
    parts = str(key_str).split('_')
    prefix = '_'.join(parts[:3])
    try:
        salary = float(parts[3])
    except:
        salary = None
    return prefix, salary, parts


# =============================================================================
# Hike Logic (10%–50%)
# =============================================================================
def is_salary_in_hike_range(candidate_salary, job_salary):
    """Return True if job salary gives 10–90% hike over candidate salary."""
    try:
        candidate_salary = float(candidate_salary)
        job_salary = float(job_salary)
    except (ValueError, TypeError):
        return False

    if candidate_salary <= 0:
        return False

    min_expected = candidate_salary * 1.05   # 10% hike
    max_expected = candidate_salary * 1.90   # 705 hike
    return min_expected <= job_salary <= max_expected


# =============================================================================
# FIND MATCHES
# =============================================================================
def find_matching_candidates_for_all_jobs(jobs_df, candidates_df):
    results = {}
    candidates_by_prefix = {}

    for _, row in candidates_df.iterrows():
        cand_key = row[CANDIDATES_COLUMNS['composite_key_col']]
        prefix, cand_salary, _ = parse_composite_key(cand_key)
        if prefix is None:
            continue
        candidates_by_prefix.setdefault(prefix, []).append({
            'candidate_id': row[CANDIDATES_COLUMNS['candidate_id_col']],
            'salary': cand_salary,
            'full_row': row
        })

    for _, job_row in jobs_df.iterrows():
        job_id = job_row[JOBS_COLUMNS['job_id_col']]
        job_key = job_row[JOBS_COLUMNS['composite_key_col']]
        prefix, target_salary, _ = parse_composite_key(job_key)
        if prefix is None:
            continue

        matches = []
        if prefix in candidates_by_prefix:
            for cand in candidates_by_prefix[prefix]:
                cand_salary = cand['salary']
                if (
                    cand_salary is not None
                    and target_salary is not None
                    and is_salary_in_hike_range(cand_salary, target_salary)
                ):
                    matches.append(cand['full_row'])

        results[job_id] = {
            'job_row': job_row,
            'matches': pd.DataFrame(matches)
        }

    return results


# =============================================================================
# EXPORT RESULTS
# =============================================================================
def export_to_single_excel(results):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    all_matches_dfs = []

    for job_id, data in results.items():
        match_df = data['matches']
        job_row = data['job_row']
        if match_df.empty:
            continue

        match_df = match_df.copy()
        match_df['job_id'] = job_id
        match_df['Active /Inactive'] = job_row[JOBS_COLUMNS['status_col']]
        match_df['job_date'] = job_row[JOBS_COLUMNS['date_col']]
        match_df['job_composit_key'] = job_row[JOBS_COLUMNS['composite_key_col']]
        match_df['job_company'] = job_row[JOBS_COLUMNS['company_col']]
        match_df['job_designation'] = job_row[JOBS_COLUMNS['designation_col']]
        match_df['job_location'] = job_row[JOBS_COLUMNS['location_col']]
        match_df['job_hr_name'] = job_row[JOBS_COLUMNS['hr_name_col']]
        match_df['company_code'] = job_row.get('company_code', 'NA')

        all_matches_dfs.append(match_df)

    if not all_matches_dfs:
        print("⚠️ No matches found.")
        return 0

    combined_df = pd.concat(all_matches_dfs, ignore_index=True)
    print(f"💾 Total combined matches: {len(combined_df)}")

    # ✅ Create new composite key
    combined_df['new_composite_key'] = (
        combined_df['company_code'].astype(str).str.strip() + '_' +
        combined_df['job_composit_key'].astype(str).str.strip()
    )

    # ✅ NEW LOGIC: Extract job_salary from job_composit_key (e.g., 518_3_3_4.2 → 4.2)
    def extract_job_salary(key):
        if pd.isna(key):
            return None
        parts = str(key).split('_')
        try:
            return float(parts[-1])
        except:
            return parts[-1] if len(parts) > 0 else None

    combined_df['job_salary'] = combined_df['job_composit_key'].apply(extract_job_salary)

    # =======================================================
    # 1️⃣ Save Full Job Match File
    # =======================================================
    file_all = os.path.join(OUTPUT_DIR, 'All_job_match.xlsx')
    combined_df.to_excel(file_all, index=False)
    print(f"✅ Saved full match file: {file_all}")

    # =======================================================
    # 2️⃣ Unique File (remove duplicates by name_location)
    # =======================================================
    if 'name_location' in combined_df.columns:
        unique_df = combined_df.drop_duplicates(subset=['name_location'], keep='first')
        file_unique = os.path.join(OUTPUT_DIR, 'All_job_match_unique.xlsx')
        unique_df.to_excel(file_unique, index=False)
        print(f"✅ Saved unique file (by name_location): {file_unique}")
    else:
        print("⚠️ 'name_location' column missing; skipping unique file.")
        unique_df = combined_df

    # =======================================================
    # ✅ NEW SECTION — Split the UNIQUE candidates into chunks of 30
    # =======================================================
    os.makedirs(SPLIT_DIR, exist_ok=True)
    chunk_size = 30
    total_rows = len(unique_df)
    num_chunks = (total_rows // chunk_size) + (1 if total_rows % chunk_size != 0 else 0)

    for i in range(num_chunks):
        start_idx = i * chunk_size
        end_idx = start_idx + chunk_size
        chunk_df = unique_df.iloc[start_idx:end_idx]
        chunk_file = os.path.join(SPLIT_DIR, f"unique_candidates_{i+1}.xlsx")
        chunk_df.to_excel(chunk_file, index=False)
        print(f"📄 Saved split file {i+1}/{num_chunks}: {chunk_file}")

    # =======================================================
    # 3️⃣ Strict Dedup (remove duplicates by new_composite_key per candidate)
    # =======================================================
    before = len(combined_df)
    dedup_df = combined_df.drop_duplicates(subset=['candidate_id', 'new_composite_key'], keep='first')
    after = len(dedup_df)
    print(f"🧹 Strict dedup removed {before - after} duplicates (based on new_composite_key).")

    file_sumit = os.path.join(OUTPUT_DIR, 'All_job_match_sumit.xlsx')
    dedup_df.to_excel(file_sumit, index=False)
    print(f"✅ Saved strict deduplicated file: {file_sumit}")

    print("\n📊 Summary:")
    print(f"   ➤ Total: {before}")
    print(f"   ➤ Unique (name_location): {len(unique_df)}")
    print(f"   ➤ Sumit Strict Dedup: {after}")
    print(f"   ➤ Split files created: {num_chunks} (each 30 candidates)")

    return after


# =============================================================================
# MAIN
# =============================================================================
def main():
    print("🚀 Finploy Matching Engine - 10–50% Hike Logic + Strict Dedup Version")
    print("=" * 80)

    jobs_df, candidates_df = load_excel_data()
    if jobs_df is None or candidates_df is None:
        return

    print("\n🔍 Matching candidates to jobs...")
    results = find_matching_candidates_for_all_jobs(jobs_df, candidates_df)

    print("\n💾 Exporting results...")
    export_to_single_excel(results)

    print("\n🎯 Process completed successfully.")


if __name__ == '__main__':
    main()
