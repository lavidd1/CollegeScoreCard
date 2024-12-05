import streamlit as st
import pandas as pd
from load_ipeds import connect_db
import sys
import base64
import plotly.express as px

#Changed page settings so that tables fit; no need to scroll left/right
st.set_page_config(layout="wide", page_icon='🎓')


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
        st.error(f"Invalid query: {e}")
        return None
    finally:
        connection.close()


# Set up queries
query1 = """
    SELECT loc.stabbr AS state, inst.control AS type, COUNT(*) AS num_institutions
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

# Streamlit Dashboard Code
st.title("Team Olympia Dashboard")
st.write("Explore data about institutions across the United States.")


# Analysis 1: Number of Institutions by State and Type
st.header("Number of Institutions by State and Type")
selected_year_institutions = st.selectbox("Select Year for Institution Analysis", ['2018', '2019', '2020', '2021'], key="institutions_year")
data1 = query_data(query1, parameters=(selected_year_institutions,))
data2 = query_data(query2, parameters=(selected_year_institutions,))
data3 = query_data(query3, parameters=(selected_year_institutions,))

# Map institution types
mapping = {
    '1': 'Public',
    '2': 'Private non-profit',
    '3': 'Private for-profit',
    '4': 'Foreign'
}
if data1 is not None:
    data1['type'] = data1['type'].replace(mapping)
if data3 is not None:
    data3['type'] = data3['type'].replace(mapping)


# Row 1: Tables and Pie Chart
st.subheader("Data Overview and Distribution")
row1_col1, row1_col2, row1_col3, row1_col4 = st.columns([2.7, 2, 2.3, 3])


with row1_col1:
    st.subheader("State & Type")
    if data1 is not None and not data1.empty:
        st.dataframe(data1)
    else:
        st.warning("No data available for this year.")

with row1_col2:
    st.subheader("State Summary")
    if data2 is not None and not data2.empty:
        st.dataframe(data2)
    else:
        st.warning("No data available for this year.")

with row1_col3:
    st.subheader("Institution Types")
    if data3 is not None and not data3.empty:
        st.dataframe(data3)
    else:
        st.warning("No data available for this year.")

with row1_col4:
    st.subheader("Institution Type Distribution")
    if data3 is not None and not data3.empty:
        pie_chart = px.pie(
            data3,
            values="num_institutions",
            names="type",
            title=f"Institution Type Distribution for {selected_year_institutions}",
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        st.plotly_chart(pie_chart, use_container_width=True)
    else:
        st.warning("No data available to generate pie chart.")


# Row 2: US Map by Institutions
st.subheader("Number of Institutions by State (Map)")
if data2 is not None and not data2.empty:
    map_chart = px.choropleth(
        data2,
        locations="state",  # State abbreviation
        locationmode="USA-states",  # Match state abbreviations
        color="num_institutions",  # Number of institutions
        hover_name="state",  # State name on hover
        title=f"Number of Institutions by State for {selected_year_institutions}",
        color_continuous_scale="Viridis",
        scope="usa"  # Focus on US map
    )

    # Update the layout to explicitly set the map size
    map_chart.update_layout(
        height=800,  # Set the height of the map
        width=1200,  # Set the width of the map
        margin={"r":0,"t":50,"l":0,"b":0}  # Adjust margins for better use of space
    )

    # Render the map
    st.plotly_chart(map_chart, use_container_width=False)  # Disable container width to use custom size
else:
    st.warning("No data available to generate the US map.")


# SQL query for tuition rates summary
query_tuition_summary = """
    SELECT
        loc.STABBR AS state,
        ipeds.CCBASIC AS carnegie_classification,
        AVG(fin.TUITIONFEE_IN) AS avg_in_state_tuition,
        AVG(fin.TUITIONFEE_OUT) AS avg_out_state_tuition
    FROM
        Financial_Data fin
    JOIN
        Institutions inst ON fin.UNITID = inst.UNITID
    JOIN
        Location loc ON inst.UNITID = loc.UNITID
    JOIN
        IPEDS_Directory ipeds ON fin.UNITID = ipeds.UNITID AND fin.YEAR = ipeds.YEAR
    WHERE
        fin.YEAR = %s
    GROUP BY
        loc.STABBR,
        ipeds.CCBASIC
    ORDER BY
        loc.STABBR,
        ipeds.CCBASIC;
"""


# Analysis 2: Tuition Rates by State and Carnegie Classification
st.header("Tuition Rates by State and Carnegie Classification")
selected_year_tuition = st.selectbox("Select Year for Tuition Analysis", ['2018', '2019', '2020', '2021', '2022'], key="tuition_year")
tuition_data = query_data(query_tuition_summary, parameters=(selected_year_tuition,))


# Display results
if tuition_data is not None and not tuition_data.empty:
    st.write(f"### Tuition Rate Summary for {selected_year_tuition}")
    st.dataframe(tuition_data, use_container_width=True)
else:
    st.warning(f"No tuition data available for the year {selected_year_tuition}.")

if tuition_data is not None and not tuition_data.empty:
    st.subheader("Average Tuition Rates by Carnegie Classification")
    bar_chart = px.bar(
        tuition_data,
        x="carnegie_classification",
        y=["avg_in_state_tuition", "avg_out_state_tuition"],
        title="Average Tuition Rates by Carnegie Classification",
        labels={"carnegie_classification": "Carnegie Classification", "value": "Tuition Rate"},
        barmode="group",
        height=500,
    )
    st.plotly_chart(bar_chart, use_container_width=True)
else:
    st.warning("No tuition data available to generate the bar chart.")


# Best- and worst-performing institutions by loan repayment rates
query_repayment = """
(
    SELECT 
        inst.INSTNM AS institution_name,
        loc.STABBR AS state,
        fin.CDR3 AS loan_repayment_rate,
        'Best' AS category
    FROM 
        Institutions inst
    JOIN 
        Location loc ON inst.UNITID = loc.UNITID
    JOIN 
        Financial_Data fin ON inst.UNITID = fin.UNITID
    WHERE 
        fin.YEAR = %s
    ORDER BY 
        fin.CDR3 ASC  -- Lowest default rates (best repayment rates)
    LIMIT 5
)
UNION
(
    SELECT 
        inst.INSTNM AS institution_name,
        loc.STABBR AS state,
        fin.CDR3 AS loan_repayment_rate,
        'Worst' AS category
    FROM 
        Institutions inst
    JOIN 
        Location loc ON inst.UNITID = loc.UNITID
    JOIN 
        Financial_Data fin ON inst.UNITID = fin.UNITID
    WHERE 
        fin.YEAR = %s
    ORDER BY 
        fin.CDR3 DESC  -- Highest default rates (worst repayment rates)
    LIMIT 5
)
ORDER BY 
    category, loan_repayment_rate;
"""

# Streamlit Analysis 3: Best and Worst Institutions by Loan Repayment Rates
st.header("Best- and Worst-Performing Institutions by Loan Repayment Rates")

selected_year_repayment = st.selectbox("Select Year for Loan Repayment Analysis", ['2018', '2019', '2020', '2021'], key="repayment_year")

# Execute the query with the year parameter passed twice
repayment_data = query_data(query_repayment, parameters=(selected_year_repayment, selected_year_repayment))

if repayment_data is not None and not repayment_data.empty:
    st.subheader("Loan Repayment Performance")
    st.dataframe(repayment_data, use_container_width=True)
else:
    st.warning(f"No repayment data available for the year {selected_year_repayment}.")

#Commenting this for now as the values for worst performing are None- hence considered worst by default
#if repayment_data is not None and not repayment_data.empty:
    #st.subheader("Best and Worst Loan Repayment Rates Comparison")
    #dual_bar_chart = px.bar(
        #repayment_data,
        #x="institution_name",
        #y="loan_repayment_rate",
        #color="category",
        #barmode="group",
        #title=f"Best vs Worst Loan Repayment Rates for {selected_year_repayment}",
        #labels={"loan_repayment_rate": "Repayment Rate", "institution_name": "Institution Name"},
        #height=500,
    #)
    #st.plotly_chart(dual_bar_chart, use_container_width=True)
#else:
    #st.warning("No data available to generate the dual bar chart.")

# Analysis 4: Tuition Rates and Loan Repayment Trends Over Time
st.header("Trends in Tuition Rates and Loan Repayment Rates Over Time")

# Dropdown to choose analysis type
analysis_type = st.radio(
    "Choose the type of analysis:",
    ["Aggregate Trends", "Selected Institutions"]
)

# Aggregate Trends
if analysis_type == "Aggregate Trends":
    query_aggregate_trends = """
        SELECT 
            fin.YEAR AS year,
            inst.CONTROL AS institution_type,
            AVG(fin.TUITIONFEE_IN) AS avg_in_state_tuition,
            AVG(fin.TUITIONFEE_OUT) AS avg_out_state_tuition,
            AVG(fin.CDR3) AS avg_loan_repayment_rate
        FROM 
            Financial_Data fin
        JOIN 
            Institutions inst ON fin.UNITID = inst.UNITID
        GROUP BY 
            fin.YEAR, inst.CONTROL
        ORDER BY 
            fin.YEAR, inst.CONTROL;
    """
    aggregate_data = query_data(query_aggregate_trends)

    if aggregate_data is not None and not aggregate_data.empty:
        # Ensure column names are lowercase for consistency
        aggregate_data.columns = aggregate_data.columns.str.lower()

        st.subheader("Average Tuition Rates and Loan Repayment Rates (Aggregate)")
        fig = px.line(
            aggregate_data,
            x="year",  # Ensure lowercase column name
            y=["avg_in_state_tuition", "avg_out_state_tuition", "avg_loan_repayment_rate"],
            color="institution_type",
            title="Tuition and Loan Repayment Trends (Aggregate)",
            labels={"year": "Year", "value": "Rate", "institution_type": "Institution Type"},
            height=500
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No data available for aggregate trends.")

# Selected Institutions
elif analysis_type == "Selected Institutions":
    query_selected_trends = """
        SELECT 
            fin.YEAR AS year,
            inst.INSTNM AS institution_name,
            fin.TUITIONFEE_IN AS in_state_tuition,
            fin.TUITIONFEE_OUT AS out_state_tuition,
            fin.CDR3 AS loan_repayment_rate
        FROM 
            Financial_Data fin
        JOIN 
            Institutions inst ON fin.UNITID = inst.UNITID
        WHERE 
            inst.INSTNM IN (
                SELECT INSTNM 
                FROM Institutions inst 
                JOIN Financial_Data fin ON inst.UNITID = fin.UNITID
                ORDER BY fin.TUITIONFEE_OUT DESC
                LIMIT 5
            )
        ORDER BY 
            fin.YEAR, inst.INSTNM;
    """
    selected_data = query_data(query_selected_trends)

    if selected_data is not None and not selected_data.empty:
        # Ensure column names are lowercase for consistency
        selected_data.columns = selected_data.columns.str.lower()

        st.subheader("Trends for Selected Institutions (Most Expensive)")
        fig = px.line(
            selected_data,
            x="year",  # Ensure lowercase column name
            y=["in_state_tuition", "out_state_tuition", "loan_repayment_rate"],
            color="institution_name",
            title="Tuition and Loan Repayment Trends (Selected Institutions)",
            labels={"year": "Year", "value": "Rate", "institution_name": "Institution Name"},
            height=500
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No data available for selected institutions.")


# Analysis 5: Correlation between Tuition, Loan Repayment Rates, and Faculty Salaries
st.header("Correlation Analysis: Tuition, Loan Repayment Rates, and Faculty Salaries")

# Year selection
selected_year_correlation = st.selectbox("Select Year for Correlation Analysis", ['2018', '2019', '2020', '2021'], key="correlation_year")

# Query data
query_correlation = """
    SELECT 
        inst.INSTNM AS institution_name,
        fin.TUITIONFEE_IN AS in_state_tuition,
        fin.TUITIONFEE_OUT AS out_state_tuition,
        fin.CDR3 AS loan_repayment_rate,
        fin.AVGFACSAL AS avg_faculty_salary
    FROM 
        Financial_Data fin
    JOIN 
        Institutions inst ON fin.UNITID = inst.UNITID
    WHERE 
        fin.YEAR = %s
    ORDER BY 
        inst.INSTNM;
"""
correlation_data = query_data(query_correlation, parameters=(selected_year_correlation,))

# Check if data is available
if correlation_data is not None and not correlation_data.empty:
    st.subheader("Data Preview")
    st.dataframe(correlation_data)

    # Convert data types to numeric for correlation analysis
    correlation_data.columns = correlation_data.columns.str.lower()
    numeric_data = correlation_data[["in_state_tuition", "out_state_tuition", "loan_repayment_rate", "avg_faculty_salary"]].apply(pd.to_numeric, errors="coerce")

    # Display Correlation Matrix
    st.subheader("Correlation Matrix")
    correlation_matrix = numeric_data.corr()
    st.write(correlation_matrix)

    # Heatmap for Correlation Matrix
    st.subheader("Heatmap of Correlations")
    fig_heatmap = px.imshow(
        correlation_matrix,
        text_auto=True,
        color_continuous_scale="RdBu",
        title="Correlation Heatmap",
        labels={"color": "Correlation Coefficient"},
        height=500,
    )
    st.plotly_chart(fig_heatmap, use_container_width=True)

    # Scatter Plots
    st.subheader("Scatter Plots to Visualize Correlations")
    scatter_columns = st.multiselect(
        "Select Columns for Scatter Plot",
        options=["in_state_tuition", "out_state_tuition", "loan_repayment_rate", "avg_faculty_salary"],
        default=["in_state_tuition", "loan_repayment_rate"]
    )
    if len(scatter_columns) == 2:
        fig_scatter = px.scatter(
            numeric_data,
            x=scatter_columns[0],
            y=scatter_columns[1],
            title=f"Scatter Plot: {scatter_columns[0]} vs {scatter_columns[1]}",
            labels={scatter_columns[0]: scatter_columns[0].replace("_", " ").title(),
                    scatter_columns[1]: scatter_columns[1].replace("_", " ").title()},
            height=500
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

else:
    st.warning(f"No data available for the year {selected_year_correlation}.")


# Analysis 6: Trends in Graduate Debt
st.header("Trends in Graduate Debt Over Time")

# Query to fetch graduate debt trends
query_grad_debt = """
    SELECT 
        YEAR AS year,
        AVG(GRAD_DEBT_MDN) AS avg_grad_debt
    FROM 
        Admissions_Data
    WHERE 
        GRAD_DEBT_MDN IS NOT NULL
    GROUP BY 
        YEAR
    ORDER BY 
        YEAR;
"""

# Fetch data
grad_debt_data = query_data(query_grad_debt)

# Check if data is available
if grad_debt_data is not None and not grad_debt_data.empty:
    st.subheader("Graduate Debt Trends")
    st.dataframe(grad_debt_data)

    # Line chart for graduate debt trends
    fig_grad_debt = px.line(
        grad_debt_data,
        x="year",
        y="avg_grad_debt",
        title="Trends in Graduate Debt Over Time",
        labels={"year": "Year", "avg_grad_debt": "Average Graduate Debt (USD)"},
        height=500
    )
    st.plotly_chart(fig_grad_debt, use_container_width=True)
else:
    st.warning("No data available for graduate debt trends.")

# Analysis 7: Top Institutions by Faculty Salaries
st.header("Top Institutions by Faculty Salaries")

# SQL query to fetch top institutions
# Enhanced SQL query to include institution type
query_top_salaries_enhanced = """
    SELECT 
        inst.INSTNM AS institution_name,
        fin.AVGFACSAL AS avg_faculty_salary,
        fin.TUITIONFEE_IN AS in_state_tuition,
        fin.TUITIONFEE_OUT AS out_state_tuition,
        inst.CONTROL AS institution_type
    FROM 
        Financial_Data fin
    JOIN 
        Institutions inst ON fin.UNITID = inst.UNITID
    WHERE 
        fin.AVGFACSAL IS NOT NULL
    ORDER BY 
        fin.AVGFACSAL DESC
    LIMIT 10;
"""


# Fetch enhanced data
faculty_salary_data_enhanced = query_data(query_top_salaries_enhanced)

if faculty_salary_data_enhanced is not None and not faculty_salary_data_enhanced.empty:
    st.subheader("Top Institutions by Faculty Salaries")

    # Display the dataframe
    st.dataframe(faculty_salary_data_enhanced)

    # Horizontal bar chart grouped by institution type
    fig_salaries_grouped = px.bar(
        faculty_salary_data_enhanced,
        x="avg_faculty_salary",
        y="institution_name",
        color="institution_type",  # Grouped by institution type
        orientation="h",
        title="Top Institutions by Faculty Salaries (Grouped by Type)",
        labels={
            "avg_faculty_salary": "Average Faculty Salary (USD)",
            "institution_name": "Institution Name",
            "institution_type": "Institution Type"
        },
        height=500
    )
    st.plotly_chart(fig_salaries_grouped, use_container_width=True)

    # Optional scatter plot for correlation
    fig_correlation = px.scatter(
        faculty_salary_data_enhanced,
        x="avg_faculty_salary",
        y="in_state_tuition",
        color="institution_name",
        title="Correlation between Faculty Salaries and Tuition Fees (In-State)",
        labels={
            "avg_faculty_salary": "Average Faculty Salary (USD)",
            "in_state_tuition": "In-State Tuition (USD)"
        },
        height=500
    )
    st.plotly_chart(fig_correlation, use_container_width=True)

else:
    st.warning("No data available for enhanced faculty salaries analysis.")



# Footer
st.write("**Note:** All data is sourced from the College Scorecard project.")