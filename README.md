# College Scorecard Data Loader

This project contains a script for loading College Scorecard data into a PostgreSQL database using `psycopg`. The script supports data cleaning, transformation, and insertion into predefined database tables.

## Prerequisites

- Python 3.x
- PostgreSQL database
- `psycopg` library for database connections
- CSV file with College Scorecard data

## Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/yourusername/CollegeScoreCard.git
   cd CollegeScoreCard

## Setup
Create a credentials.py file in the project directory with the following format:

DB_HOST = "your_database_host"
DB_NAME = "your_database_name"
DB_USER = "your_database_username"
DB_PASSWORD = "your_database_password"

Ensure that credentials.py is not committed to version control for security reasons. Add it to your .gitignore file.

Set up your PostgreSQL database with the required tables to match the schema expected by the script.

## Function Description
connect_db()
Establishes a connection to the PostgreSQL database using credentials from credentials.py.

clean_data(row, columns)
Cleans and formats data from a row according to specified column requirements. Converts missing, empty, or redacted values to None.

insert_data(cursor, table, data, columns)
Inserts data into the specified database table. Uses ON CONFLICT DO NOTHING to handle potential conflicts gracefully.

load_scorecard_data(file_path)
Reads a CSV file containing College Scorecard data and inserts cleaned data into relevant tables in the database.

# IPEDS Data Loader

This project contains a script for loading IPEDS-specific data from a CSV file into a PostgreSQL database. The data is cleaned, validated, and inserted into relevant database tables while ensuring the integrity of relationships with the Institutions table.

## Prerequisites
- Python 3.x
- PostgreSQL database
- `psycopg` library for database connections
- A CSV file containing IPEDS-specific data

## Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/yourusername/CollegeScoreCard.git
   cd CollegeScoreCard

## Setup

Create a credentials.py file in the project directory with your database credentials:

DB_HOST = "your_database_host"
DB_NAME = "your_database_name"
DB_USER = "your_database_username"
DB_PASSWORD = "your_database_password"

Ensure that credentials.py is added to your .gitignore file to prevent sensitive information from being exposed in version control.

Make sure your PostgreSQL database is set up and includes the necessary tables (e.g., Institutions, IPEDS_Directory).

## Function Description

connect_db()
Establishes a connection to the PostgreSQL database using credentials from credentials.py.

clean_data(row, columns)
Cleans and formats data from a given row based on specified columns, converting empty or redacted values to None.

check_unitid_exists(cursor, unitid)
Checks whether a given UNITID exists in the Institutions table to ensure referential integrity.

insert_data(cursor, table, data, columns)
Inserts data into the specified table using a batch insert operation, while handling potential conflicts gracefully with ON CONFLICT DO NOTHING.

load_ipeds_data(file_path)
Reads a CSV file containing IPEDS data, cleans and validates the data, and inserts it into the IPEDS_Directory table, skipping records with missing UNITID references.