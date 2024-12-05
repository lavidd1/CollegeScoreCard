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
        original_value = value  # Preserve the original value for debugging
        if value is not None:
            value = value.strip()
        if value in ['-999', '', '-2', 'NULL', None, 'PrivacySuppressed']:
            cleaned_data[col] = None
        else:
            cleaned_data[col] = value
        # Debug: Log original and cleaned values for the ADDR column
        # if col == "ADDR":
        #     print(f"Column: {col}, Original: {original_value}, Cleaned: {cleaned_data[col]}")
    return cleaned_data


def extract_year_from_filename(filename):
    """
    Extracts the year from filename.

    Parameters:
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
    Dynamically maps CSV column names to schema names based on the year.

    Args:
        columns (list): List of column names from the CSV file.
        year (int): The academic year used to determine year-specific prefixes.

    Returns:
        dict: Mapping of schema column names to corresponding CSV column names.
    """
    # Identify potential year prefixes in the columns
    year_prefixes = {col[:3] for col in columns if col[:3].startswith("C") and col[1:3].isdigit()}

    # Try to find the prefix for the given year
    target_prefix = f"C{year % 100 - 1}"  # E.g., 2022 -> C21
    if target_prefix not in year_prefixes:
        print(f"Warning: Prefix {target_prefix} not found in columns. Using available prefixes: {year_prefixes}")
        target_prefix = year_prefixes.pop() if year_prefixes else None

    if not target_prefix:
        raise ValueError("No valid year prefix found in columns.")

    # Map schema columns to CSV columns
    mappings = {
        f"{target_prefix}BASIC": "CCBASIC",
        f"{target_prefix}UGPRF": "CCUGPROF",
        f"{target_prefix}SZSET": "CCSIZSET",
        f"{target_prefix}IPUG": "CCIPUG",
        f"{target_prefix}IPGRD": "CCIPGRD",
        f"{target_prefix}ENPRF": "CCENPROF",
    }

    # Return the mapping of schema to available CSV columns
    return {schema: col if col in columns else None for col, schema in mappings.items()}


def preload_unitids(cursor):
    """
    Preloads all UNITID values from the Institutions table into memory.
    """
    cursor.execute("SELECT UNITID FROM Institutions")
    return set(row[0] for row in cursor.fetchall())


def batch_insert_location(cursor, addr_updates):
    """
    Batch inserts or updates the ADDR values in the Location table.
    """
    print(f"Processing {len(addr_updates)} address updates...")
    sql = """
        INSERT INTO Location (UNITID, ADDR)
        VALUES (%s, %s)
        ON CONFLICT (UNITID)
        DO UPDATE SET ADDR = EXCLUDED.ADDR
        WHERE Location.ADDR IS NULL OR Location.ADDR = '';
    """
    print(f"Address updates to process: {addr_updates[:10]}")  # Log a sample of updates
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
    sql = f"INSERT INTO IPEDS_Directory ({', '.join(columns)}) VALUES ({placeholders})"
    for i in range(0, len(data), batch_size):
        cursor.executemany(sql, data[i:i + batch_size])


def load_ipeds_data(file_path):
    """
    Loads IPEDS data and updates the Location and IPEDS_Directory tables.
    """
    conn = connect_db()
    cursor = conn.cursor()
    year = extract_year_from_filename(file_path)
    data_year = year - 1

    try:
        # Preload UNITID values from the Institutions table
        existing_unitids = preload_unitids(cursor)

        with open(file_path, mode='r', encoding='ISO-8859-1') as file:
            reader = csv.DictReader(file)
            available_columns = reader.fieldnames

            # Validate that ADDR column exists in the CSV
            if "ADDR" not in available_columns:
                raise ValueError("ADDR column missing from CSV file.")

            # Dynamically map columns
            mapped_columns = map_columns_by_year(available_columns, year)
            static_columns = ["CBSA", "CBSATYPE", "CSA", "LATITUDE", "LONGITUD"]
            addr_column = "ADDR"
            yr_id_cols = ["YEAR", "UNITID"]
            ipeds_directory_cols = yr_id_cols + static_columns + list(mapped_columns.keys())

            ipeds_data = []
            addr_updates = []
            skipped_records = 0
            total_rows = 0
            null_addr_count = 0  # Counter for rows with NULL ADDR

            for row in reader:
                total_rows += 1
                unitid = row.get("UNITID")
                if not unitid or int(unitid) not in existing_unitids:
                    skipped_records += 1
                    continue

                try:
                    # Clean and prepare data for IPEDS_Directory table
                    cleaned_row = clean_data(row, available_columns)

                    # Check if ADDR is NULL or missing
                    addr_value = cleaned_row.get(addr_column)
                    if addr_value is None:
                        null_addr_count += 1  # Increment the counter for NULL ADDR

                    # Prepare row data for IPEDS_Directory
                    row_data = [data_year, unitid] + [
                        cleaned_row.get(col, None) for col in static_columns
                    ] + [
                        cleaned_row.get(mapped_columns[col], None) for col in mapped_columns.keys()
                    ]
                    ipeds_data.append(tuple(row_data))

                    # Collect ADDR updates if present
                    if addr_value:
                        addr_updates.append((unitid, addr_value))

                except Exception as e:
                    print(f"Error processing row {total_rows}: {row}")
                    print(f"Error details: {e}")
                    skipped_records += 1

            print(f"Total rows read from CSV: {total_rows}")
            print(f"Total rows skipped: {skipped_records}")
            print(f"Total rows prepared IPEDS_Directory: {len(ipeds_data)}")
            print(f"Total ADDR updates for Location: {len(addr_updates)}")
            print(f"Rows with NULL ADDR values: {null_addr_count}")  # Print NULL ADDR count

            # Batch insert into Location table
            if addr_updates:
                print(f"Updating {len(addr_updates)} records in Location table...")
                batch_insert_location(cursor, addr_updates)

            # Batch insert into IPEDS_Directory table
            if ipeds_data:
                print(f"Inserting {len(ipeds_data)} records into IPEDS_Directory table...")
                batch_insert_ipeds(cursor, ipeds_data, ipeds_directory_cols)

            conn.commit()
            print("IPEDS data loaded successfully.")

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
