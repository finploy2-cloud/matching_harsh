import mysql.connector
from mysql.connector import Error

# ==========================================================
# CONFIGURATION
# ==========================================================
SQL_DB_CONFIG = {
    "host": "65.0.211.89",
    "user": "harsh875_finploy_user1",
    "password": "Make*45@23+67",
    "database": "harsh875_finploy_com",
    "port": 3306,
}

TABLE_NAME = "candidate_jobs"

# ==========================================================
# MAIN FUNCTION
# ==========================================================
def update_finploy_hr():
    try:
        conn = mysql.connector.connect(**SQL_DB_CONFIG)
        cursor = conn.cursor()

        # ==================================================
        # STEP 1: Copy linup_finploy_hr → finploy_hr if empty
        # ==================================================
        step1_query = f"""
            UPDATE candidate_jobs
            SET finploy_hr = TRIM(lineup_finploy_hr)
            WHERE
                button_response_inst = 'Interested'
                AND lineup_finploy_hr IS NOT NULL
                AND TRIM(lineup_finploy_hr) <> ''
                AND (
                    finploy_hr IS NULL
                    OR TRIM(finploy_hr) = ''
                    OR finploy_hr = '0'
                );
        """
        cursor.execute(step1_query)
        conn.commit()
        print(f"[STEP 1] ✅ Copied linup_finploy_hr to finploy_hr for {cursor.rowcount} rows.")

        # ==================================================
        # STEP 2: Assign HR names alternately where both columns are empty/'0'
        # ==================================================
        step2_query = f"""
            UPDATE candidate_jobs AS cj
            JOIN (
                SELECT 
                    sr_no,
                    CASE ((ROW_NUMBER() OVER (ORDER BY sr_no DESC) - 1) % 3)
                        WHEN 0 THEN 'Soham'
                        WHEN 1 THEN 'Antara'
                        ELSE 'Shraddha'
                    END AS assigned_hr
                FROM (
                    SELECT sr_no
                    FROM candidate_jobs
                    WHERE
                        button_response_inst = 'Interested'
                        AND (lineup_finploy_hr IS NULL OR TRIM(lineup_finploy_hr) = '' OR lineup_finploy_hr = '0')
                        AND (finploy_hr IS NULL OR TRIM(finploy_hr) = '' OR finploy_hr = '0')
                    ORDER BY sr_no DESC
                    
                ) AS t
            ) AS ranked
            ON cj.sr_no = ranked.sr_no
            SET cj.finploy_hr = ranked.assigned_hr;
        """
        cursor.execute(step2_query)
        conn.commit()
        print(f"[STEP 2] ✅ Alternating HRs assigned for {cursor.rowcount} new rows (test mode).")

    except Error as e:
        print(f"❌ MySQL Error: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            print("✅ Connection closed.")


# ==========================================================
# RUN SCRIPT
# ==========================================================
if __name__ == "__main__":
    update_finploy_hr()

# ---------------------------------------------------------
# Run not_intrested.py after SQL insertion is done
# ---------------------------------------------------------
try:
    import subprocess
    print("🚀 Running sql_to_sheet.py ...")
    subprocess.run(
        ["python", r"D:\matching_harsh\Job_matching_unscreened\candidate_jobs_formate\sql_to_sheet.py"],
        check=True
    )
    print("✔ not_intrested.py executed successfully!")

except Exception as e:
    print(f"❌ Failed to run not_intrested.py: {e}")