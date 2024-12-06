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
    """
    cleaned_data = {}
    for col in columns:
        value = row.get(col)
        if value is not None:
            value = value.strip()
        if value in ['-999', '', '-2', 'NULL', None, 'PrivacySuppressed']:
            cleaned_data[col] = None
        else:
            cleaned_data[col] = value
    return cleaned_data


def extract_year_from_filename(filename):
    """
    Extracts the year from the filename.

    Args:
        filename (str): The filename from which to extract the year.

    Returns:
        int: The year extracted from the filename with format YYYY.
    """
    match = re.search(r'hd(\d{4})\.csv$', filename)
    if not match:
        raise ValueError("Filename must have format hdYYYY.csv")
    return int(match.group(1))


def map_columns_by_year(columns, year):
    """
    Maps CSV column names to schema names based on the year.

    Args:
        columns (list): List of column names from the CSV file.
        year (int): The academic year to determine year-specific prefixes.

    Returns:
        dict: Mapping of schema column names to corresponding CSV columns.
    """
    if year >= 2020:
        target_prefix = "C21"
    elif year >= 2017:
        target_prefix = "C18"
    else:
        raise ValueError("Year not supported: Carnegie classifications "
                         "start from 2017.")

    year_prefixes = {col[:3] for col in columns if col[:3].startswith("C") and
                     col[1:3].isdigit()}

    if target_prefix not in year_prefixes:
        print(f"Warning: Expected prefix {target_prefix} not found. "
              f"Using available prefixes: {year_prefixes}")
        target_prefix = year_prefixes.pop() if year_prefixes else None

    if not target_prefix:
        raise ValueError("No valid year prefix found in columns.")

    mappings = {
        "CCBASIC": f"{target_prefix}BASIC",
        "CCUGPROF": f"{target_prefix}UGPRF",
        "CCSIZSET": f"{target_prefix}SZSET",
        "CCIPUG": f"{target_prefix}IPUG",
        "CCIPGRD": f"{target_prefix}IPGRD",
        "CCENPROF": f"{target_prefix}ENPRF",
    }

    return {schema: col for schema, col in mappings.items() if col in columns}


def preload_unitids(cursor):
    """
    Preloads all UNITID values from the Institutions table into memory.

    Args:
        cursor: The database cursor object.

    Returns:
        set: A set of UNITID values from the Institutions table.
    """
    cursor.execute("SELECT UNITID FROM Institutions")
    return set(row[0] for row in cursor.fetchall())


def batch_insert_location(cursor, addr_updates):
    """
    Batch inserts or updates the ADDR values in the Location table.

    Args:
        cursor: The database cursor object.
        addr_updates (list): A list of tuples containing UNITID and ADDR.
    """
    print(f"Processing {len(addr_updates)} address updates...")
    sql = """
        INSERT INTO Location (UNITID, ADDR)
        VALUES (%s, %s)
        ON CONFLICT (UNITID)
        DO UPDATE SET ADDR = EXCLUDED.ADDR
        WHERE Location.ADDR IS NULL OR Location.ADDR = '';
    """
    cursor.executemany(sql, addr_updates)
    print("Address updates complete.")


def batch_insert_ipeds(cursor, data, columns, batch_size=1000):
    """
    Inserts data into the IPEDS_Directory table in batches.

    Args:
        cursor: The database cursor object.
        data: A list of tuples representing rows of data to insert.
        columns: A list of column names corresponding to the data.
        batch_size: The size of each batch for insertion.
    """
    placeholders = ', '.join(['%s'] * len(columns))
    sql = f"INSERT INTO IPEDS_Directory ({', '.join(columns)}) VALUES " \
          f"({placeholders})"
    for i in range(0, len(data), batch_size):
        cursor.executemany(sql, data[i:i + batch_size])


def load_ipeds_data(file_path):
    """
    Loads IPEDS data and updates the Location and IPEDS_Directory tables.

    Args:
        file_path (str): The path to the CSV file to load.
    """
    conn = connect_db()
    cursor = conn.cursor()
    year = extract_year_from_filename(file_path)
    data_year = year

    try:
        existing_unitids = preload_unitids(cursor)

        with open(file_path, mode='r', encoding='ISO-8859-1') as file:
            reader = csv.DictReader(file)
            available_columns = reader.fieldnames

            if "ADDR" not in available_columns:
                raise ValueError("ADDR column missing from CSV file.")

            mapped_columns = map_columns_by_year(available_columns, year)
            static_columns = ["CBSA", "CBSATYPE", "CSA", "LATITUDE",
                              "LONGITUD"]
            addr_column = "ADDR"
            yr_id_cols = ["YEAR", "UNITID"]
            ipeds_directory_cols = yr_id_cols + static_columns + \
                list(mapped_columns.keys())

            ipeds_data = []
            addr_updates = []
            skipped_records = 0
            total_rows = 0
            null_addr_count = 0

            for row in reader:
                total_rows += 1
                unitid = row.get("UNITID")
                if not unitid or int(unitid) not in existing_unitids:
                    skipped_records += 1
                    continue

                cleaned_row = clean_data(row, available_columns)
                addr_value = cleaned_row.get(addr_column)
                if addr_value is None:
                    null_addr_count += 1

                row_data = [data_year, unitid] + \
                    [cleaned_row.get(col, None) for col in static_columns] + \
                    [cleaned_row.get(mapped_columns[col], None)
                     for col in mapped_columns.keys()]
                ipeds_data.append(tuple(row_data))

                if addr_value:
                    addr_updates.append((unitid, addr_value))

            print("\nSummary:")
            print(f"- Total rows read from CSV: {total_rows}")
            print(f"- Total rows skipped: {skipped_records}")
            print(f"- Total rows prepared for IPEDS_Directory: "
                  f"{len(ipeds_data)}")
            print(f"- Total ADDR updates for Location: {len(addr_updates)}")
            print(f"- Rows with NULL ADDR values: {null_addr_count}")

            if addr_updates:
                print(f"\nUpdating {len(addr_updates)} records in "
                      f"Location table...")
                batch_insert_location(cursor, addr_updates)

            if ipeds_data:
                print(f"\nInserting {len(ipeds_data)} records into "
                      f"IPEDS_Directory table...")
                batch_insert_ipeds(cursor, ipeds_data, ipeds_directory_cols)

            conn.commit()
            print("\nIPEDS data loaded successfully.")

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
