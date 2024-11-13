import psycopg
import csv
import sys
from psycopg.errors import ForeignKeyViolation

def connect_db():
    """Establishes a database connection."""
    return psycopg.connect(
        host="pinniped.postgres.database.azure.com",
        dbname="abradsha",
        user="abradsha",
        password="hcVjAJ5zo3"
    )

def clean_data(row, columns):
    """Cleans and formats data from the row according to the required schema."""
    cleaned_data = {}
    for col in columns:
        value = row.get(col)
        if value in ['-999', '', 'NULL', None, 'PrivacySuppressed']:
            cleaned_data[col] = None
        else:
            cleaned_data[col] = value
    return cleaned_data

def check_unitid_exists(cursor, unitid):
    """Checks if a UNITID exists in the Institutions table."""
    cursor.execute("SELECT EXISTS(SELECT 1 FROM Institutions WHERE UNITID = %s)", (unitid,))
    return cursor.fetchone()[0]

def insert_data(cursor, table, data, columns):
    """Inserts data into the specified table."""
    placeholders = ', '.join(['%s'] * len(columns))
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
    print(f"Executing SQL: {sql}")  # Debugging: print SQL statement for review
    print(f"Data sample: {data[0]}")  # Debugging: print the first data row for review
    cursor.executemany(sql, data)

def load_ipeds_data(file_path):
    """Loads and inserts IPEDS-specific data into the database."""
    conn = connect_db()
    cursor = conn.cursor()

    try:
        with open(file_path, mode='r', encoding='ISO-8859-1') as file:
            reader = csv.DictReader(file)
            available_columns = reader.fieldnames

            ipeds_directory_columns = [
                "CBSA", "CBSATYPE", "CSA", "CCBASIC", "CCUGPROF", "CCSIZSET",
                "CCUGINST", "CCGIP", "CCENPROF", "LATITUDE", "LONGITUD"
            ]
            ipeds_data = []
            skipped_records = 0

            for row in reader:
                year = 2019  # Replace with actual year if available in the data
                unitid = row.get("UNITID")

                # Skip if UNITID is not found in Institutions
                if not check_unitid_exists(cursor, unitid):
                    print(f"Skipping record for UNITID {unitid} - does not exist in Institutions table.")
                    skipped_records += 1
                    continue

                ipeds_row = clean_data(row, ipeds_directory_columns)
                ipeds_data.append((year, unitid) + tuple(ipeds_row.values()))

            if ipeds_data:
                print(f"Inserting {len(ipeds_data)} rows into IPEDS_Directory table...")
                insert_data(cursor, "IPEDS_Directory", ipeds_data, ["YEAR", "UNITID"] + ipeds_directory_columns)

            conn.commit()
            print(f"IPEDS data loaded successfully.")
            print(f"Skipped {skipped_records} records due to missing UNITID references.")

    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python load_ipeds.py <csv_file>")
        sys.exit(1)

    load_ipeds_data(sys.argv[1])
