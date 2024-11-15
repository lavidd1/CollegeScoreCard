import psycopg
import csv
import sys
import re
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
        if value in ['-999', '', 'NULL', None, 'PrivacySuppressed']:
            cleaned_data[col] = None
        else:
            cleaned_data[col] = value
    return cleaned_data


def extract_year_from_filename(filename):
    """
    Extracts the year from filename.

    Parameters:
    filename (str): The filename from which to extract the year.

    Returns:
    int: The year extracted from the filename with format YYY.
    """
    match = re.search(r'hd(\d{4})\.csv$', filename)
    if not match:
        raise ValueError("Filename must have format hdYYYY.csv")
    return int(match.group(1))


def map_columns_by_year(columns, year):
    """
    Maps CSV column names to schema names based on a year-specific prefix.

    For example, if `year` is 2022, CSV columns with the prefix 'C21'
    (e.g., 'C21SZSET') are mapped to corresponding schema names
    (e.g., 'CCSIZSET').

    Parameters:
    columns (list of str): A list of CSV column names to map.
    year (int): The year used to determine the prefix for CSV columns.

    Returns:
    dict: A dictionary where keys are schema column names and values are the
          matched CSV column names, or None if the CSV column is not present
          in `columns`.
    """
    year_prefix = f"C{year % 100 - 1}"  # E.g. for 2022, prefix will be "C21"
    mappings = {
        f"{year_prefix}SZSET": "CCSIZSET",
        f"{year_prefix}UGPRF": "CCUGPROF",
        f"{year_prefix}IPGRD": "CCIPGRD",
        f"{year_prefix}ENPRF": "CCENPROF",
        f"{year_prefix}IPUG": "CCIPUG"
    }

    # Match CSV columns with schema column names
    mapped_columns = {}
    for csv_column, schema_column in mappings.items():
        if csv_column in columns:
            mapped_columns[schema_column] = csv_column
        else:
            mapped_columns[schema_column] = None  # Set missing columns to None

    return mapped_columns


def check_unitid_exists(cursor, unitid):
    """
    Checks if a specific UNITID exists in the Institutions table.

    Parameters:
    cursor (psycopg2.extensions.cursor or similar): A database cursor to
                                                    execute the SQL query.
    unitid (int or str): The UNITID to check for in the Institutions table.

    Returns:
    bool: True if the UNITID exists in the Institutions table, False otherwise.
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
        data (list of tuple): A list of tuples representing rows of data
            to insert.
        columns (list): A list of columns corresponding to the data.
    """
    placeholders = ', '.join(['%s'] * len(columns))
    sql = (
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders}) "
        "ON CONFLICT DO NOTHING"
    )
    print(f"Executing SQL: {sql}")
    print(f"Data sample: {data[0]}")
    cursor.executemany(sql, data)


def load_ipeds_data(file_path):
    conn = connect_db()
    cursor = conn.cursor()
    year = extract_year_from_filename(file_path)

    try:
        with open(file_path, mode='r', encoding='ISO-8859-1') as file:
            reader = csv.DictReader(file)
            available_columns = reader.fieldnames

            # Map year-specific columns to schema names
            mapped_columns = map_columns_by_year(available_columns, year)
            static_columns = ["CBSA",
                              "CBSATYPE",
                              "CSA",
                              "CCBASIC",
                              "LATITUDE",
                              "LONGITUD"]

            # Define final columns in IPEDS_Directory schema
            yr_id_cols = ["YEAR", "UNITID"]
            mc_keys = list(mapped_columns.keys())
            ipeds_directory_cols = yr_id_cols + static_columns + mc_keys

            ipeds_data = []
            skipped_records = 0

            for row in reader:
                unitid = row.get("UNITID")
                if not check_unitid_exists(cursor, unitid):
                    print(f"Skipping record for UNITID {unitid}"
                          "- does not exist in Institutions table.")
                    skipped_records += 1
                    continue

                # Clean and prepare data for insertion
                cleaned_row = clean_data(row, available_columns)

                # Populate row data with static columns
                row_data = [year, unitid] + [cleaned_row.get(col, None)
                                             for col in static_columns]

                # Add mapped columns, defaulting to None if they don't exist
                for schema_col, csv_col in mapped_columns.items():
                    row_data.append(cleaned_row.get(csv_col)
                                    if csv_col else None)

                ipeds_data.append(tuple(row_data))

            # Insert data in batch if available
            if ipeds_data:
                print(f"Inserting {len(ipeds_data)} rows into"
                      "IPEDS_Directory table...")
                insert_data(cursor, "IPEDS_Directory", ipeds_data,
                            ipeds_directory_cols)

            conn.commit()
            print("IPEDS data loaded successfully.")
            print(f"Skipped {skipped_records} records"
                  "due to missing UNITID references.")

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
