import streamlit as st
import pandas as pd
from load_ipeds import connect_db
import sys


def query_data(query, parameters=None):
    """
    Executes an SQL query and returns the result as a pandas DataFrame.

    Parameters:
    query (str): The SQL query to execute.

    Returns:
    pd.DataFrame: ...
    """
    try:
        connection = connect_db()
        df = pd.read_sql_query(query, connection, params=parameters)

        return df
    except Exception as e:
        print(f"Invalid query: {e}")
        return None
    finally:
        connection.close()


# -----------Get year from user----------------
if len(sys.argv) != 2:
    print("Usage: streamlit run app.py <year>")
    sys.exit(1)

year = sys.argv[1]

if not year.isdigit():
    print("The year must be a numeric value.")
    sys.exit(1)
# ----------------------------------------------

# ------------Set up queries here----------------
query1 = """
    SELECT loc.stabbr AS state, inst.control AS type, COUNT(*) AS
    num_institutions
    FROM institutions AS inst
    JOIN location AS loc ON inst.unitid = loc.unitid
    JOIN ipeds_directory AS ipeds ON loc.unitid = ipeds.unitid
    WHERE ipeds.year = %s
    GROUP BY loc.stabbr, inst.control
    ORDER BY loc.stabbr;
"""

query2 = """
    SELECT loc.stabbr AS state, COUNT(*) AS num_institutions
    FROM institutions AS inst
    JOIN location AS loc ON inst.unitid = loc.unitid
    JOIN ipeds_directory AS ipeds ON loc.unitid = ipeds.unitid
    WHERE ipeds.year = %s
    GROUP BY loc.stabbr
    ORDER BY loc.stabbr;
"""

query3 = """
    SELECT inst.control AS type, COUNT(*) AS num_institutions
    FROM institutions AS inst
    JOIN ipeds_directory AS ipeds ON inst.unitid = ipeds.unitid
    WHERE ipeds.year = %s
    GROUP BY inst.control;
"""

data1 = query_data(query1, parameters=(year,))
data2 = query_data(query2, parameters=(year,))
data3 = query_data(query3, parameters=(year,))
mapping = {'1': 'Public',
           '2': 'Private non-profit',
           '3': 'Private for-profit',
           '4': 'Foreign'}
data1['type'] = data1['type'].replace(mapping)
data3['type'] = data3['type'].replace(mapping)
# -----------------------------------------------

# -----------------------Dashboard Code------------------------
st.title("Team Olympia Dashboard")
st.write(f"Displaying data for the year: {year}")

st.subheader("Number of Institution by State and Type")

col1, col2, col3 = st.columns(3)
with col1:
    if data1 is not None and not data1.empty:
        st.dataframe(data1)
    else:
        st.warning("No data available for the selected year.")

with col2:
    if data2 is not None and not data1.empty:
        st.dataframe(data2)
    else:
        st.warning("No data available for the selected year.")

with col3:
    if data3 is not None and not data1.empty:
        st.dataframe(data3)
    else:
        st.warning("No data available for the selected year.")
# -------------------------------------------------------------
