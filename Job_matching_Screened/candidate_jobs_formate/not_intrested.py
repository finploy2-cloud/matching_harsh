import mysql.connector
import argparse

# MySQL configuration
db_config = {
    'host': '65.0.211.89',
    'port':3306,
    'user': 'harsh875_finploy_user1',
    'password': 'Make*45@23+67',
    'database': 'harsh875_finploy_com',
}

def build_history_where_clause():
    # Match rows where history contains the phrase NOT INTERESTED (and tolerate common typo)
    return (
        "(UPPER(CAST(ch.history AS CHAR)) LIKE '%NOT INTERESTED%' "
        " OR UPPER(CAST(ch.history AS CHAR)) LIKE '%NOT INTRESTED%')"
    )

def main():
    try:
        # Optional single phone filter
        parser = argparse.ArgumentParser(add_help=False)
        parser.add_argument('--phone', '-p', dest='phone', help='Test a single phone_no')
        args, _ = parser.parse_known_args()
        one_phone = args.phone

        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Exact value to set in candidate_jobs.	NOT_INTERESTED
        NOT_INTERESTED_VALUE = 'not interested'

        where_history = build_history_where_clause()

        # Count candidates eligible to update
        preview_sql = (
            "SELECT COUNT(DISTINCT cj.phone_no) "
            "FROM candidate_jobs cj "
            "JOIN conversation_history ch ON ch.contact_number = cj.phone_no "
            f"WHERE {where_history}"
        )
        preview_params = []
        if one_phone:
            preview_sql += " AND cj.phone_no = %s"
            preview_params.append(one_phone)
        cursor.execute(preview_sql, tuple(preview_params))
        (eligible_count,) = cursor.fetchone()
        print(
            "Eligible candidate_jobs rows to update (distinct phone_no)"
            + (f" for {one_phone}" if one_phone else "")
            + f": {eligible_count}"
        )

        # Perform the update using a single UPDATE ... JOIN across all matching rows
        update_sql = (
            "UPDATE candidate_jobs cj "
            "JOIN conversation_history ch ON ch.contact_number = cj.phone_no "
            "SET cj.NOT_INTERESTED = %s "
            f"WHERE {where_history} "
            "AND (cj.NOT_INTERESTED IS NULL OR cj.NOT_INTERESTED = '' "
            "OR LOWER(cj.NOT_INTERESTED) <> %s)"
        )
        update_params = [NOT_INTERESTED_VALUE, NOT_INTERESTED_VALUE]
        if one_phone:
            update_sql += " AND cj.phone_no = %s"
            update_params.append(one_phone)

        cursor.execute(update_sql, tuple(update_params))
        conn.commit()
        print(f"Updated rows in candidate_jobs.NOT_INTERESTED: {cursor.rowcount}")
        # If testing a single phone, show quick diagnostics and the final value
        if one_phone:
            cursor.execute(
                "SELECT COUNT(*) FROM conversation_history ch "
                "WHERE ch.contact_number = %s AND "
                "UPPER(CAST(ch.history AS CHAR)) LIKE '%NOT INTERESTED%'",
                (one_phone,),
            )
            (hist_matches,) = cursor.fetchone()
            print(f"History matches for {one_phone}: {hist_matches}")
            cursor.execute(
                "SELECT NOT_INTERESTED FROM candidate_jobs WHERE phone_no = %s",
                (one_phone,),
            )
            row = cursor.fetchone()
            if row is not None:
                print(f"candidate_jobs.NOT_INTERESTED for {one_phone}: {row[0]}")
            else:
                print(f"No candidate_jobs row found for phone_no {one_phone}")

    except mysql.connector.Error as err:
        print("Error:", err)
    finally:
        try:
            cursor.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

if __name__ == '__main__':
    main()
# ---------------------------------------------------------
# Run sheet_to_sql.py after SQL insertion is done
# ---------------------------------------------------------
try:
    import subprocess
    print("🚀 Running sheet_to_sql.py ...")
    subprocess.run(
        ["python", r"D:\matching_harsh\Job_matching_Screened\candidate_jobs_formate\sheet_to_sql.py"],
        check=True
    )
    print("✔ sheet_to_sql.py executed successfully!")

except Exception as e:
    print(f"❌ Failed to run sheet_to_sql.py: {e}")
