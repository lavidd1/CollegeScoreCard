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
        # Convert missing, empty, or redacted values to None
        if value in ['-999', '', 'NULL', None, 'PrivacySuppressed']:
            cleaned_data[col] = None
        else:
            cleaned_data[col] = value
    return cleaned_data


def extract_year_from_filename(filename):
    """
    Extracts the academic end year from filename.

    Args:
        filename (str): The name of the file being processed.

    Returns:
        int: The academic end year in the format YYYY.
    """
    match = re.search(r'MERGED(\d{4})_(\d{2})_', filename)
    if not match:
        raise ValueError("Filename must have format MERGEDYYYY_YY_*.csv")

    # Extract starting year and increment by 1 to get the academic end year
    start_year = int(match.group(1))
    end_year = start_year + 1

    return end_year


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
    placeholders = ', '.join(['%s'] * len(columns))  # Generate placeholders
    sql = (
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders}) "
        "ON CONFLICT DO NOTHING"
    )
    print(f"Executing SQL: {sql}")  # Debugging: print SQL statement for review
    print(f"Data sample: {data[0]}")  # Debugging: print the
    # first data row for review
    cursor.executemany(sql, data)


def load_scorecard_data(file_path):
    """
    Loads and inserts College Scorecard data into the database.

    Args:
        file_path (str): The path to the CSV file containing the College
            Scorecard data.
    """
    conn = connect_db()
    cursor = conn.cursor()
    year = extract_year_from_filename(file_path)

    try:
        with open(file_path, mode='r', encoding='ISO-8859-1') as file:
            reader = csv.DictReader(file)

            institutions_columns = [
                "UNITID", "OPEID", "INSTNM", "CONTROL", "ACCREDAGENCY",
                "PREDDEG", "HIGHDEG"
            ]
            location_columns = [
                "UNITID", "REGION", "ST_FIPS", "ADDRESS", "CITY",
                "STABBR", "ZIP"
            ]
            financial_data_columns = [
                "UNITID", "TUITIONFEE_IN", "TUITIONFEE_OUT", "TUITIONFEE_PROG",
                "TUITFTE", "AVGFACSAL", "CDR2", "CDR3"
            ]
            admissions_data_columns = [
                "UNITID", "ADM_RATE", "GRAD_DEBT_MDN", "SATMTMID", "ACTMTMID"
            ]

            institutions_data = []
            location_data = []
            financial_data = []
            admissions_data = []

            for row in reader:
                # Prepare data for each table
                institution_row = clean_data(row, institutions_columns)
                institutions_data.append(tuple(institution_row.values()))

                location_row = clean_data(row, location_columns)
                location_data.append(tuple(location_row.values()))

                financial_row = clean_data(row, financial_data_columns)
                financial_data.append((year,) + tuple(financial_row.values()))
                # YEAR added here

                admissions_row = clean_data(row, admissions_data_columns)
                admissions_data.append((year,) +
                                       tuple(admissions_row.values()))
                # YEAR added here

            # Insert data in batch
            if institutions_data:
                print(f"Inserting {len(institutions_data)} \
                      rows into Institutions table...")
                insert_data(cursor, "Institutions",
                            institutions_data, institutions_columns)

            if location_data:
                print(f"Inserting {len(location_data)} \
                      rows into Location table...")
                insert_data(cursor, "Location",
                            location_data, location_columns)

            if financial_data:
                print(f"Inserting {len(financial_data)} \
                      rows into Financial_Data table...")
                insert_data(cursor, "Financial_Data", financial_data,
                            ["YEAR"] + financial_data_columns)

            if admissions_data:
                print(f"Inserting {len(admissions_data)} \
                      rows into Admissions_Data table...")
                insert_data(cursor, "Admissions_Data", admissions_data,
                            ["YEAR"] + admissions_data_columns)

            conn.commit()
            print("Data from College Scorecard loaded successfully.")

    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python load_scorecard.py <csv_file>")
        sys.exit(1)

    load_scorecard_data(sys.argv[1])
