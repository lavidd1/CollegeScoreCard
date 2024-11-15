import psycopg
import csv
import sys
from psycopg.errors import ForeignKeyViolation
import credentials


def connect_db():
    """
    Establishes a database connection using the credentials module.

    Returns:
        psycopg.Connection: A connection object to interact with the
        PostgreSQL database.
    """
    return psycopg.connect(
        host=credentials.DB_HOST,
        dbname=credentials.DB_NAME,
        user=credentials.DB_USER,
        password=credentials.DB_PASSWORD
    )


def clean_data(row, columns):
    """
    Cleans and formats data from the row according to the required schema.

    Args:
        row (dict): A dictionary representing a row of data.
        columns (list): A list of columns to extract and clean data for.

    Returns:
        dict: A dictionary containing cleaned and formatted data for the
        specified columns.
    """
    cleaned_data = {}
    for col in columns:
        value = row.get(col)
        # Convert missing, empty, or redacted values to None
        if value in ['-999', '', 'NULL', None, 'PrivacySuppressed']:
            cleaned_data[col] = None
        else:
            cleaned_data[col] = value
    return cleaned_data


def check_unitid_exists(cursor, unitid):
    """
    Checks if a UNITID exists in the Institutions table.

    Args:
        cursor (psycopg.Cursor): The database cursor object.
        unitid (str): The UNITID to check for existence.

    Returns:
        bool: True if the UNITID exists in the Institutions
        table, False otherwise.
    """
    cursor.execute(
        "SELECT EXISTS(SELECT 1 FROM Institutions WHERE UNITID = %s)",
        (unitid,)
    )
    return cursor.fetchone()[0]


def insert_data(cursor, table, data, columns):
    """
    Inserts data into the specified table.

    Args:
        cursor (psycopg.Cursor): The database cursor object.
        table (str): The name of the table to insert data into.
        data (list of tuple): A list of tuples representing rows
        of data to insert.
        columns (list): A list of column names corresponding to the data.
    """
    placeholders = ', '.join(['%s'] * len(columns))  # Generate placeholders
    sql = (
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders}) "
        "ON CONFLICT DO NOTHING"
    )
    print(f"Executing SQL: {sql}")  # Debugging: print SQL statement
    print(f"Data sample: {data[0]}")  # Debugging: print the first
    # data row for review
    cursor.executemany(sql, data)


def load_ipeds_data(file_path):
    """
    Loads and inserts IPEDS-specific data into the database from a CSV file.

    Args:
        file_path (str): The path to the CSV file containing the IPEDS data.
    """
    conn = connect_db()
    cursor = conn.cursor()

    try:
        with open(file_path, mode='r', encoding='ISO-8859-1') as file:
            reader = csv.DictReader(file)
            available_columns = reader.fieldnames

            # Define columns for the IPEDS_Directory table
            ipeds_directory_columns = [
                "CBSA", "CBSATYPE", "CSA", "CCBASIC", "CCUGPROF", "CCSIZSET",
                "CCUGINST", "CCGIP", "CCENPROF", "LATITUDE", "LONGITUD"
            ]
            ipeds_data = []
            skipped_records = 0

            for row in reader:
                year = 2019  # Example year; replace with actual year
                # if available in the data
                unitid = row.get("UNITID")

                # Skip if UNITID is not found in Institutions table
                if not check_unitid_exists(cursor, unitid):
                    print(f"Skipping record for UNITID {unitid} - does \
                          not exist in Institutions table.")
                    skipped_records += 1
                    continue

                # Clean and prepare data for insertion
                ipeds_row = clean_data(row, ipeds_directory_columns)
                ipeds_data.append((year, unitid) + tuple(ipeds_row.values()))

            # Insert data in batch if available
            if ipeds_data:
                print(f"Inserting {len(ipeds_data)} rows into \
                      IPEDS_Directory table...")
                insert_data(cursor, "IPEDS_Directory", ipeds_data,
                            ["YEAR", "UNITID"] + ipeds_directory_columns)

            conn.commit()
            print("IPEDS data loaded successfully.")
            print(f"Skipped {skipped_records} records due to \
                  missing UNITID references.")

    except Exception as e:
        conn.rollback()  # Rollback transaction on error
        print(f"Error: {e}")
    finally:
        cursor.close()  # Close the cursor
        conn.close()  # Close the connection


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python load_ipeds_aki.py <csv_file>")
        sys.exit(1)

    load_ipeds_data(sys.argv[1])
