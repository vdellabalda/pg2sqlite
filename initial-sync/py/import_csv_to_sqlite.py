import sys
import subprocess
from pysqlite3 import dbapi2 as sqlite3
import pandas as pd
from sqlalchemy import create_engine


def import_csv_to_sqlite(csv_directory, sqlite_db_file, schema_sql_file=None):
    # Step 1: Load schema into the SQLite database
    engine = create_engine(f"sqlite:///{sqlite_db_file}")
    conn = sqlite3.connect(sqlite_db_file)
    cur = conn.cursor()

    if schema_sql_file:
        print(f"Loading schema from {schema_sql_file} into {sqlite_db_file}...")
        with open(schema_sql_file) as fp:
            cur.executescript(fp.read())  # or con.executescript

        #try:
        #    subprocess.run(f'sqlite3 "{sqlite_db_file}" < "{schema_sql_file}"', shell=True, check=True)
        #    print("✅ Schema applied successfully.")
        #except subprocess.CalledProcessError as e:
        #    print(f"❌ Failed to load schema: {e}")
        #    return

    # Step 2: Import all CSV files into the pre-created tables
    csv_files = [f for f in os.listdir(csv_directory) if f.endswith('.csv')]
    for csv_file in csv_files:
        table_name = os.path.splitext(csv_file)[0]
        csv_file_path = os.path.join(csv_directory, csv_file)
        df = pd.read_csv(csv_file_path, header=0)  # skips the first row

        print(f"📥 Importing {csv_file} into table {table_name}...")
        df.to_sql(table_name, con=engine, if_exists="append", index=False)

    print("🎉 All CSV files have been imported.")

if __name__ == "__main__":
    if len(sys.argv) not in (3, 4):
        print("Usage: python import_csv_to_sqlite.py <csv_directory> <sqlite_db_file> [schema.sql]")
        sys.exit(1)

    csv_directory = sys.argv[1]
    sqlite_db_file = sys.argv[2]
    schema_sql_file = sys.argv[3] if len(sys.argv) == 4 else None

    import_csv_to_sqlite(csv_directory, sqlite_db_file, schema_sql_file)


