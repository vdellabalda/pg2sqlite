import os
import sys
import subprocess
import pandas as pd
import sqlite3

def import_csv_to_sqlite(csv_directory, sqlite_db_file, schema_sql_file=None):
    # Step 1: Load schema into the SQLite database
    if schema_sql_file:
        print(f"Loading schema from {schema_sql_file} into {sqlite_db_file}...")
        try:
            subprocess.run(f'sqlite3 "{sqlite_db_file}" < "{schema_sql_file}"', shell=True, check=True)
            print("✅ Schema applied successfully.")
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to load schema: {e}")
            return

    # Step 2: Import all CSV files into the pre-created tables
    csv_files = [f for f in os.listdir(csv_directory) if f.endswith('.csv')]
    for csv_file in csv_files:
        table_name = os.path.splitext(csv_file)[0]
        df = pd.read_csv(csv_directory+"/"+csv_file, header=0)  # skips the first row
        print(df.head())
        conn = sqlite3.connect(sqlite_db_file)
        df.to_sql(table_name, conn, if_exists="append", index=False)
        #csv_file_path = os.path.join(csv_directory, csv_file)

        print(f"📥 Importing {csv_file} into table {table_name}...")
        #sqlite_command = f'''
        #sqlite3 "{sqlite_db_file}" <<EOF
        #.mode csv
        #.import "{csv_file_path}" "{table_name}"
        #EOF
        #'''
        #try:
        #    subprocess.run(sqlite_command, shell=True, check=True)
        #    print(f"✅ Imported {csv_file}")
        #except subprocess.CalledProcessError as e:
        #    print(f"❌ Error importing {csv_file}: {e}")

    print("🎉 All CSV files have been imported.")

if __name__ == "__main__":
    if len(sys.argv) not in (3, 4):
        print("Usage: python import_csv_to_sqlite.py <csv_directory> <sqlite_db_file> [schema.sql]")
        sys.exit(1)

    csv_directory = sys.argv[1]
    sqlite_db_file = sys.argv[2]
    schema_sql_file = sys.argv[3] if len(sys.argv) == 4 else None

    import_csv_to_sqlite(csv_directory, sqlite_db_file, schema_sql_file)

