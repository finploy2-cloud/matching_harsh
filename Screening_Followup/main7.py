# ============================================================
# job_matcher_unique.py  – Finploy Job-Candidate Matcher
# Creates both duplicate & unique files (deduped by Contact)
# ============================================================

import pandas as pd
import os

# =============================================================================
# CONFIGURATION
# =============================================================================
JOBS_FILE = r"D:\matching_harsh\Screening_Followup\input\MASTER FILE LOCATIONS - Mapping.xlsx"
CANDIDATES_FILE = r"D:\matching_harsh\Screening_Followup\output\output4.xlsx"
OUTPUT_DIR = r"D:\matching_harsh\Screening_Followup\final_output\all_job_matches.xlsx"

JOBS_COLUMNS = {
    "job_id_col": "job_id",
    "composite_key_col": "composit_key",
    "date_col": "Date",
    "company_col": "Company",
    "designation_col": "Designation",
    "location_col": "Client location",
    "hr_name_col": "HR Name",
    "status_col": "Active /Inactive",
    "company_code": "company_code",
}

CANDIDATES_COLUMNS = {
    "candidate_id_col": "candidate_id",
    "composite_key_col": "composit_key",
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
    """Split key into parts; return prefix and salary."""
    if pd.isna(key_str) or "_" not in str(key_str) or str(key_str).count("_") != 3:
        return None, None
    parts = str(key_str).split("_")
    prefix = "_".join(parts[:3])
    try:
        salary = float(parts[3])
    except ValueError:
        salary = None
    return prefix, salary


# =============================================================================
def find_matching_candidates_for_all_jobs(jobs_df, candidates_df):
    """Find matching candidates for all jobs based on composite_key and salary."""
    results = {}
    candidates_by_prefix = {}

    # Build prefix lookup for candidates
    for _, row in candidates_df.iterrows():
        cand_key = row[CANDIDATES_COLUMNS["composite_key_col"]]
        prefix, cand_salary = parse_composite_key(cand_key)
        if prefix and cand_salary is not None:
            candidates_by_prefix.setdefault(prefix, []).append(
                {
                    "candidate_id": row[CANDIDATES_COLUMNS["candidate_id_col"]],
                    "salary": cand_salary,
                    "full_row": row,
                }
            )

    # Process each job
    for _, job_row in jobs_df.iterrows():
        job_id = job_row[JOBS_COLUMNS["job_id_col"]]
        job_key = job_row[JOBS_COLUMNS["composite_key_col"]]
        prefix, target_salary = parse_composite_key(job_key)

        if prefix and target_salary is not None:
            matches = [
                cand["full_row"]
                for cand in candidates_by_prefix.get(prefix, [])
                if cand["salary"] <= target_salary
            ]
            match_df = pd.DataFrame(matches) if matches else pd.DataFrame()
            results[job_id] = {
                "job_key": job_key,
                "target_salary": target_salary,
                "matches": match_df,
                "count": len(match_df),
                "job_row": job_row,
            }

    return results


# =============================================================================
def export_to_single_excel(results):
    """Export all job-candidate matches to duplicate + unique Excel files."""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR, exist_ok=True)

    all_matches_dfs = []
    total_matches = 0

    for job_id, data in results.items():
        match_df = data["matches"]
        job_row = data["job_row"]

        if not match_df.empty:
            df = match_df.copy()

            # Add job details
            df["job_id"] = job_id
            df["Active /Inactive"] = job_row[JOBS_COLUMNS["status_col"]]
            df["job_date"] = job_row[JOBS_COLUMNS["date_col"]]
            df["job_composit_key"] = job_row[JOBS_COLUMNS["composite_key_col"]]
            df["job_company"] = job_row[JOBS_COLUMNS["company_col"]]
            df["job_designation"] = job_row[JOBS_COLUMNS["designation_col"]]
            df["job_location"] = job_row[JOBS_COLUMNS["location_col"]]
            df["job_hr_name"] = job_row[JOBS_COLUMNS["hr_name_col"]]
            df["company_code"] = job_row[JOBS_COLUMNS["company_code"]]

            # Job salary and hike %
            try:
                job_salary = float(str(job_row[JOBS_COLUMNS["composite_key_col"]]).split("_")[-1])
            except Exception:
                job_salary = None

            df["Job_salary"] = job_salary
            if "clean_salary" in df.columns:
                df["Hike"] = ((df["Job_salary"] - df["clean_salary"]) / df["clean_salary"]) * 100
                df = df[(df["Hike"] >= 10) & (df["Hike"] <= 40)]
                df["Hike"] = df["Hike"].round(1).astype(str) + "%"

            if not df.empty:
                all_matches_dfs.append(df)
                total_matches += len(df)

    if not all_matches_dfs:
        print("\n⚠️ No matches found; no output file created.")
        return 0

    # Combine all results
    combined_df = pd.concat(all_matches_dfs, ignore_index=True)

    # =====================================================
    # ✅ STEP 1: Save full duplicate file
    # =====================================================
    dup_file = os.path.join(OUTPUT_DIR, "all_job_matches_duplicate.xlsx")
    combined_df.to_excel(dup_file, index=False, engine="openpyxl")
    print(f"\n💾 Exported all matches (duplicates kept) → {dup_file}")
    print(f"📊 Total rows exported (duplicates): {len(combined_df)}")

    # =====================================================
    # ✅ STEP 2: Create UNIQUE file (remove duplicates by contact)
    # =====================================================
    contact_col = next(
        (c for c in combined_df.columns if "contact" in c.lower() or "mobile" in c.lower() or "phone" in c.lower()),
        None,
    )

    if contact_col:
        before = len(combined_df)
        unique_df = combined_df.drop_duplicates(subset=[contact_col], keep="first").copy()
        after = len(unique_df)

        unique_file = os.path.join(OUTPUT_DIR, "all_job_matches_unique.xlsx")
        unique_df.to_excel(unique_file, index=False, engine="openpyxl")

        print(f"\n🧹 Removed duplicates by '{contact_col}': {before - after} duplicates removed.")
        print(f"💎 Saved unique matches → {unique_file}")
        print(f"📊 Total unique rows: {after}")
    else:
        print("\n⚠️ No contact column found — could not create unique version.")

    return total_matches


# =============================================================================
def main():
    print("=" * 70)
    print("🚀 Finploy Job Matcher – Duplicate + Unique Output Generator")
    print("=" * 70)

    jobs_df, candidates_df = load_excel_data()
    if jobs_df is None or candidates_df is None:
        return

    print("\n🔍 Matching candidates to jobs...")
    results = find_matching_candidates_for_all_jobs(jobs_df, candidates_df)

    print("\n💾 Exporting results...")
    total_exported = export_to_single_excel(results)

    print(f"\n🎯 Completed successfully. Total rows processed: {total_exported}")


if __name__ == "__main__":
    main()
try:
    import subprocess
    print("▶️ Running main8.py ...")
    subprocess.run(["python", r"D:\matching_harsh\Screening_Followup\main8.py"], check=True)
    print("✅ main8.py executed successfully!")
except Exception as e:
    print(f"❌ Failed to run main8.py: {e}")