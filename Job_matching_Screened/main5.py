import pandas as pd
import os

# =============================================================================
# CONFIGURATION - UPDATE THESE!
# =============================================================================
JOBS_FILE = r'D:\matching_harsh\Job_matching_Screened\final_input\MASTER FILE LOCATIONS - Mapping.xlsx'
CANDIDATES_FILE = r'D:\matching_harsh\Job_matching_Screened\output\output4.xlsx'
OUTPUT_DIR = r'D:\matching_harsh\Job_matching_Screened\final_output\all_job_matches'

JOBS_COLUMNS = {
    'job_id_col': 'job_id',
    'composite_key_col': 'composit_key',
    'date_col': 'Date',
    'company_col': 'Company',
    'designation_col': 'Designation',
    'location_col': 'Client location',
    'hr_name_col': 'HR Name',
    'status_col': 'Active /Inactive',
    'company_code': "company_code"
}

CANDIDATES_COLUMNS = {
    'candidate_id_col': 'candidate_id',
    'composite_key_col': 'composit_key'
}

# =============================================================================
def load_excel_data():
    """Load jobs and candidates from Excel files."""
    try:
        jobs_df = pd.read_excel(JOBS_FILE)
        print(f"✅ Loaded {len(jobs_df)} jobs from {JOBS_FILE}")
        candidates_df = pd.read_excel(CANDIDATES_FILE)
        print(f"✅ Loaded {len(candidates_df)} candidates from {CANDIDATES_FILE}")
        return jobs_df, candidates_df
    except FileNotFoundError as e:
        print(f"❌ File not found: {e}")
        return None, None
    except Exception as e:
        print(f"❌ Error loading Excel files: {e}")
        return None, None


def parse_composite_key(key_str):
    """Split key into parts; return prefix, salary, and full parts."""
    if pd.isna(key_str) or '_' not in str(key_str) or str(key_str).count('_') != 3:
        raise ValueError(f"Invalid key '{key_str}': Must be exactly 4 parts separated by '_' (e.g., '126_5_8_2.6').")
    parts = str(key_str).split('_')
    prefix = '_'.join(parts[:3])
    try:
        salary = float(parts[3])
    except ValueError:
        raise ValueError(f"Invalid salary '{parts[3]}' in key '{key_str}': Must be a number like 2.6.")
    return prefix, salary, parts


def find_matching_candidates_for_all_jobs(jobs_df, candidates_df):
    """Find matching candidates for all jobs based on composite_key and salary."""
    results = {}
    candidates_by_prefix = {}

    # Build prefix lookup for candidates
    for _, row in candidates_df.iterrows():
        cand_key = row[CANDIDATES_COLUMNS['composite_key_col']]
        try:
            prefix, cand_salary, _ = parse_composite_key(cand_key)
            if prefix not in candidates_by_prefix:
                candidates_by_prefix[prefix] = []
            candidates_by_prefix[prefix].append({
                'candidate_id': row[CANDIDATES_COLUMNS['candidate_id_col']],
                'salary': cand_salary,
                'full_row': row
            })
        except ValueError:
            continue

    # Process each job
    for _, job_row in jobs_df.iterrows():
        job_id = job_row[JOBS_COLUMNS['job_id_col']]
        job_key = job_row[JOBS_COLUMNS['composite_key_col']]
        try:
            prefix, target_salary, _ = parse_composite_key(job_key)
            if prefix in candidates_by_prefix:
                matches = [cand['full_row'] for cand in candidates_by_prefix[prefix] if cand['salary'] <= target_salary]
                match_df = pd.DataFrame(matches) if matches else pd.DataFrame()
                results[job_id] = {
                    'job_key': job_key,
                    'target_salary': target_salary,
                    'matches': match_df,
                    'count': len(match_df),
                    'job_row': job_row
                }
            else:
                results[job_id] = {
                    'job_key': job_key,
                    'target_salary': target_salary,
                    'matches': pd.DataFrame(),
                    'count': 0,
                    'job_row': job_row
                }
        except ValueError:
            continue

    return results


def export_to_single_excel(results):
    """Export all job-candidate matches to one Excel file."""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR, exist_ok=True)

    all_matches_dfs = []
    total_matches = 0

    for job_id, data in results.items():
        match_df = data['matches']
        job_row = data['job_row']

        if not match_df.empty:
            match_df_with_job = match_df.copy()

            # Add job info columns
            match_df_with_job['job_id'] = job_id
            match_df_with_job['Active /Inactive'] = job_row[JOBS_COLUMNS['status_col']]
            match_df_with_job['job_date'] = job_row[JOBS_COLUMNS['date_col']]
            match_df_with_job['job_composit_key'] = job_row[JOBS_COLUMNS['composite_key_col']]
            match_df_with_job['job_company'] = job_row[JOBS_COLUMNS['company_col']]
            match_df_with_job['job_designation'] = job_row[JOBS_COLUMNS['designation_col']]
            match_df_with_job['job_location'] = job_row[JOBS_COLUMNS['location_col']]
            match_df_with_job['job_hr_name'] = job_row[JOBS_COLUMNS['hr_name_col']]
            match_df_with_job['company_code'] = job_row[JOBS_COLUMNS['company_code']]

            # Job salary and hike%
            try:
                job_salary = float(str(job_row[JOBS_COLUMNS['composite_key_col']]).split('_')[-1])
            except Exception:
                job_salary = None

            match_df_with_job['Job_salary'] = job_salary

            if 'clean_salary' in match_df_with_job.columns:
                match_df_with_job['Hike'] = (
                    ((match_df_with_job['Job_salary'] - match_df_with_job['clean_salary'])
                     / match_df_with_job['clean_salary']) * 100
                ).round(1)
                match_df_with_job = match_df_with_job[
                    (match_df_with_job['Hike'] >= 10) & (match_df_with_job['Hike'] <= 90)
                ]
                match_df_with_job['Hike'] = match_df_with_job['Hike'].astype(str) + '%'

            if not match_df_with_job.empty:
                all_matches_dfs.append(match_df_with_job)
                total_matches += len(match_df_with_job)

    if all_matches_dfs:
        combined_df = pd.concat(all_matches_dfs, ignore_index=True)

        # ✅ Save single output file only
        output_file = os.path.join(OUTPUT_DIR, 'all_job_matches_duplicate.xlsx')
        combined_df.to_excel(output_file, index=False, engine='openpyxl')

        print(f"\n💾 Exported all matches to: {output_file}")
        print(f"📊 Total rows exported: {total_matches}")
    else:
        print("\n⚠️ No matches found; no output file created.")

    return total_matches


def main():
    print("🚀 Excel-based Job-Candidate Matcher (Single-file export only)")
    print("=" * 70)

    jobs_df, candidates_df = load_excel_data()
    if jobs_df is None or candidates_df is None:
        return

    print("\n🔍 Finding matches for all jobs...")
    results = find_matching_candidates_for_all_jobs(jobs_df, candidates_df)

    if not results:
        print("❌ No results to process.")
        return

    print("\n💾 Exporting results...")
    total_exported = export_to_single_excel(results)

    print(f"\n🎯 Completed successfully. Total rows exported: {total_exported}")


if __name__ == '__main__':
    main()
import subprocess
try:
    import subprocess
    print("▶️ Running main6.py ...")
    subprocess.run(["python", r"D:\matching_harsh\Job_matching_Screened\main6.py"], check=True)
    print("✅ main6.py executed successfully!")
except Exception as e:
    print(f"❌ Failed to run main6.py: {e}")