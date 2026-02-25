import sys
from pathlib import Path
from typing import Iterable, Tuple

import pandas as pd

try:
    import mysql.connector
    from mysql.connector import Error as MySQLError
except ImportError as exc:
    print(
        "[ERROR] mysql-connector-python is not installed. "
        "Install it with `pip install mysql-connector-python` and retry."
    )
    raise SystemExit(1) from exc


EXCEL_PATH = Path(r"D:\matching_harsh\Job_matching_unscreened\candidate_jobs_formate\output\finploy_template.xlsx")
#CSV_PATH = Path(r"D:\matching_harsh\Job_matching_unscreened\candidate_jobs_formate\output\finploy_template.csv")
SQL_DB_CONFIG = {
    "host": "65.0.211.89",
    "user": "harsh875_finploy_user1",
    "password": "Make*45@23+67",
    "database": "harsh875_finploy_com",
    "port": 3306,
}
SQL_TABLE_NAME = "candidate_jobs"


def load_workbook(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Input workbook not found: {path}")
    df = pd.read_excel(path, dtype=str).fillna("")
    
    # ✅ Added block: Convert Excel to CSV automatically
    csv_path = path.with_suffix(".csv")
    try:
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        print(f"[OK] Excel file converted to CSV successfully: {csv_path}")
        df = pd.read_csv(csv_path, dtype=str).fillna("")
   
    except Exception as exc:
        print(f"[ERROR] Could not convert Excel to CSV: {exc}")
        raise

    if df.empty:
        print("[WARN] Workbook is empty. No rows will be inserted.")
    return df


def to_db_rows(df: pd.DataFrame) -> Iterable[Tuple]:
    columns = list(df.columns)
    for _, row in df.iterrows():
        values = []
        for column, value in zip(columns, row):
            if value == "":
                values.append(None)
                continue
            if column.lower() == "sr_no":
                try:
                    values.append(int(value))
                except ValueError:
                    print(f"[WARN] Could not convert sr_no '{value}' to int. Storing as NULL.")
                    values.append(None)
                continue
            values.append(value)
        yield tuple(values)


def insert_rows(df: pd.DataFrame) -> None:
    if df.empty:
        return

    columns = [f"`{column}`" for column in df.columns]
    placeholders = ", ".join(["%s"] * len(columns))
    insert_sql = (
        f"INSERT INTO {SQL_TABLE_NAME} ({', '.join(columns)}) VALUES ({placeholders})"
    )

    try:
        connection = mysql.connector.connect(**SQL_DB_CONFIG)
    except MySQLError as exc:
        print(f"[ERROR] Could not connect to MySQL: {exc}")
        raise SystemExit(1) from exc

    cursor = None

    try:
        cursor = connection.cursor()
        cursor.executemany(insert_sql, list(to_db_rows(df)))
        connection.commit()
        print(f"[OK] Inserted {cursor.rowcount} rows into {SQL_TABLE_NAME}.")
    except MySQLError as exc:
        connection.rollback()
        print(f"[ERROR] Failed to insert rows: {exc}")
        raise SystemExit(1) from exc
    finally:
        if cursor is not None:
            cursor.close()
        connection.close()


def main() -> None:
    try:
        df = load_workbook(EXCEL_PATH)
    except Exception as exc:
        print(f"[ERROR] {exc}")
        raise SystemExit(1) from exc
    insert_rows(df)


if __name__ == "__main__":
    main()

# ---------------------------------------------------------
# Run blast_auto.py after SQL insertion is done
# ---------------------------------------------------------
try:
    import subprocess
    print("🚀 Running blast_auto.py ...")
    subprocess.run(
        ["python", r"D:\matching_harsh\Job_matching_unscreened\candidate_jobs_formate\blast_auto.py"],
        check=True
    )
    print("✔ blast_auto.py executed successfully!")

except Exception as e:
    print(f"❌ Failed to run blast_auto.py: {e}")