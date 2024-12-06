import streamlit as st
import pandas as pd
from load_ipeds import connect_db
import plotly.express as px

# Changed page settings so that tables fit; no need to scroll left/right
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

# Streamlit Dashboard Code
st.title("Team Olympia Dashboard")
st.write("Explore data about institutions across the United States.")


# Analysis 1: Number of Institutions by State and Type
st.header("Number of Institutions by State and Type")
s1 = "Select Year for Institution Analysis"
selected_year_institutions = st.selectbox(s1,
                                          ['2019', '2020', '2021', '2022'],
                                          key="institutions_year",
                                          index=2)
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
        plot_title1 = "Institution Type Distribution for " + \
                   f"{selected_year_institutions}"
        pie_chart = px.pie(
            data3,
            values="num_institutions",
            names="type",
            title=plot_title1,
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        st.plotly_chart(pie_chart, use_container_width=True)
    else:
        st.warning("No data available to generate pie chart.")


# Row 2: US Map by Institutions
if data2 is not None and not data2.empty:
    plot_title2 = "Map of Number of Institutions by State " + \
        f"for {selected_year_institutions}"
    map_chart = px.choropleth(
        data2,
        locations="state",  # State abbreviation
        locationmode="USA-states",  # Match state abbreviations
        color="num_institutions",  # Number of institutions
        hover_name="state",  # State name on hover
        title=plot_title2,
        labels={"num_institutions": "Number of Institutions"},
        color_continuous_scale="Viridis",
        scope="usa"  # Focus on US map
    )

    # Update the layout to explicitly set the map size
    map_chart.update_layout(
        height=800,  # Set the height of the map
        width=1200,  # Set the width of the map
        # Adjust margins for better use of space
        margin={"r": 0, "t": 50, "l": 0, "b": 0}
    )

    # Render the map
    # Disable container width to use custom size
    st.plotly_chart(map_chart, use_container_width=False)
else:
    st.warning("No data available to generate the US map.")


# SQL query for tuition rates summary
query_tuition_summary1 = """
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
        IPEDS_Directory ipeds ON
        fin.UNITID = ipeds.UNITID AND fin.YEAR = ipeds.YEAR
    WHERE
        fin.YEAR = %s
    GROUP BY
        loc.STABBR,
        ipeds.CCBASIC
    ORDER BY
        loc.STABBR,
        ipeds.CCBASIC;
"""

query_tuition_summary2 = """
    SELECT
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
        IPEDS_Directory ipeds ON
        fin.UNITID = ipeds.UNITID AND fin.YEAR = ipeds.YEAR
    WHERE
        fin.YEAR = %s
    GROUP BY
        ipeds.CCBASIC
    ORDER BY
        ipeds.CCBASIC;
"""

# Analysis 2: Tuition Rates by State and Carnegie Classification
st.header("Tuition Rates by State and Carnegie Classification")
selected_year_tuition = st.selectbox("Select Year for Tuition Analysis",
                                     ['2019', '2020', '2021', '2022'],
                                     key="tuition_year",
                                     index=2)
tuition_data1 = query_data(query_tuition_summary1,
                           parameters=(selected_year_tuition,))

tuition_data2 = query_data(query_tuition_summary2,
                           parameters=(selected_year_tuition,))
# Display results
if tuition_data1 is not None and not tuition_data1.empty:
    st.write(f"### Tuition Rate Summary for {selected_year_tuition}")
    st.dataframe(tuition_data1, use_container_width=True)
else:
    w1 = f"No tuition data available for the year {selected_year_tuition}."
    st.warning(w1)

if tuition_data2 is not None and not tuition_data2.empty:
    bar_chart = px.bar(
        tuition_data2,
        x="carnegie_classification",
        y=["avg_in_state_tuition", "avg_out_state_tuition"],
        title="Average Tuition Rates by Carnegie Classification",
        labels={"carnegie_classification": "Carnegie Classification",
                "value": "Tuition Rate (USD)",
                "variable": "Metric",
                "avg_in_state_tuition": "Average In-state Tuition",
                "avg_out_state_tuition": "Average Out-of-state Tuition"},
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
s2 = "Select Year for Loan Repayment Analysis"
selected_year_repayment = st.selectbox(s2,
                                       ['2019', '2020', '2021', '2022'],
                                       key="repayment_year",
                                       index=2)

# Execute the query with the year parameter passed twice
repayment_data = query_data(query_repayment,
                            parameters=(selected_year_repayment,
                                        selected_year_repayment))

if repayment_data is not None and not repayment_data.empty:
    st.subheader("Loan Repayment Performance")
    st.dataframe(repayment_data, use_container_width=True)
else:
    w2 = f"No repayment data available for the year {selected_year_repayment}."
    st.warning(w2)


# Analysis 4: Tuition Rates and Loan Repayment Trends Over Time ----
st.header("Trends in Tuition Rates and Loan Repayment Rates Over Time")

# Select metric to plot
s4 = "Select Aggregate Metric"
selected_metric = st.selectbox(s4,
                               ['In-state Tuition',
                                'Out-of-state Tuition',
                                'Loan Repayment Rate'],
                               key="aggregate_metric")
# Aggregate Trends
if selected_metric == 'In-state Tuition':
    query_aggregate_trends = """
        SELECT
            fin.YEAR AS year,
            inst.CONTROL AS type,
            AVG(fin.TUITIONFEE_IN) AS avg_in_state_tuition
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
    if aggregate_data is not None:
        aggregate_data['type'] = aggregate_data['type'].replace(mapping)

    if aggregate_data is not None and not aggregate_data.empty:
        # Ensure column names are lowercase for consistency
        aggregate_data.columns = aggregate_data.columns.str.lower()
        fig = px.line(
            aggregate_data,
            x="year",  # Ensure lowercase column name
            y="avg_in_state_tuition",
            color="type",
            title="In-state Tuition Rates Over Time (Aggregate)",
            labels={"year": "Year",
                    "avg_in_state_tuition": "Rate (USD)",
                    "type": "Institution Type"},
            height=500
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No data available for aggregate trends.")

elif selected_metric == 'Out-of-state Tuition':
    query_aggregate_trends = """
        SELECT
            fin.YEAR AS year,
            inst.CONTROL AS type,
            AVG(fin.TUITIONFEE_OUT) AS avg_out_state_tuition
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
    if aggregate_data is not None:
        aggregate_data['type'] = aggregate_data['type'].replace(mapping)

    if aggregate_data is not None and not aggregate_data.empty:
        # Ensure column names are lowercase for consistency
        aggregate_data.columns = aggregate_data.columns.str.lower()
        fig = px.line(
            aggregate_data,
            x="year",  # Ensure lowercase column name
            y="avg_out_state_tuition",
            color="type",
            title="Out-of-state Tuition Rates Over Time (Aggregate)",
            labels={"year": "Year",
                    "avg_out_state_tuition": "Rate (USD)",
                    "type": "Institution Type"},
            height=500
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No data available for aggregate trends.")

else:
    query_aggregate_trends = """
        SELECT
            fin.YEAR AS year,
            inst.CONTROL AS type,
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
    if aggregate_data is not None:
        aggregate_data['type'] = aggregate_data['type'].replace(mapping)

    if aggregate_data is not None and not aggregate_data.empty:
        # Ensure column names are lowercase for consistency
        aggregate_data.columns = aggregate_data.columns.str.lower()
        fig = px.line(
            aggregate_data,
            x="year",  # Ensure lowercase column name
            y="avg_loan_repayment_rate",
            color="type",
            title="Loan Repayment Rates Over Time (Aggregate)",
            labels={"year": "Year",
                    "avg_loan_repayment_rate": "Rate",
                    "type": "Institution Type"},
            height=500
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No data available for aggregate trends.")

# Analysis 5: Correlation between Tuition, Loan Repayment Rates,
# and Faculty Salaries
h1 = "Correlation Analysis: Tuition, Loan Repayment Rates, " + \
    "and Faculty Salaries"
st.header(h1)

# Year selection
s5 = "Select Year for Correlation Analysis"
selected_year_correlation = st.selectbox(s4,
                                         ['2019', '2020', '2021', '2022'],
                                         key="correlation_year",
                                         index=2)

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
correlation_data = query_data(query_correlation,
                              parameters=(selected_year_correlation,))

# Check if data is available
if correlation_data is not None and not correlation_data.empty:
    st.subheader("Data Preview")
    st.dataframe(correlation_data)

    # Convert data types to numeric for correlation analysis
    correlation_data.columns = correlation_data.columns.str.lower()
    correlation_cols = ["in_state_tuition", "out_state_tuition",
                        "loan_repayment_rate", "avg_faculty_salary"]
    numeric_data = correlation_data[correlation_cols].apply(pd.to_numeric,
                                                            errors="coerce")

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
        options=["in_state_tuition", "out_state_tuition",
                 "loan_repayment_rate", "avg_faculty_salary"],
        default=["in_state_tuition", "loan_repayment_rate"]
    )
    if len(scatter_columns) == 2:
        plot_title3 = f"Scatter Plot: {scatter_columns[0]} vs " + \
            f"{scatter_columns[1]}"
        fig_scatter = px.scatter(
            numeric_data,
            x=scatter_columns[0],
            y=scatter_columns[1],
            title=plot_title3,
            labels={scatter_columns[0]:
                    scatter_columns[0].replace("_", " ").title(),
                    scatter_columns[1]:
                    scatter_columns[1].replace("_", " ").title()},
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
        labels={"year": "Year",
                "avg_grad_debt": "Average Graduate Debt (USD)"},
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
        inst.CONTROL AS type
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
if faculty_salary_data_enhanced is not None:
    faculty_salary_data_enhanced['type'] = \
        faculty_salary_data_enhanced['type'].replace(mapping)
if (faculty_salary_data_enhanced is not None and
        not faculty_salary_data_enhanced.empty):
    st.subheader("Top Institutions by Faculty Salaries")

    # Display the dataframe
    st.dataframe(faculty_salary_data_enhanced)

    # Horizontal bar chart grouped by institution type
    fig_salaries_grouped = px.bar(
        faculty_salary_data_enhanced,
        x="avg_faculty_salary",
        y="institution_name",
        color="type",  # Grouped by institution type
        orientation="h",
        title="Top Institutions by Faculty Salaries (Grouped by Type)",
        labels={
            "avg_faculty_salary": "Average Faculty Salary (USD)",
            "institution_name": "Institution Name",
            "type": "Institution Type"
        },
        height=500
    )
    st.plotly_chart(fig_salaries_grouped, use_container_width=True)

    # Optional scatter plot for correlation
    plot_title4 = "Correlation between Faculty Salaries and " + \
        "Tuition Fees (In-State)"
    fig_correlation = px.scatter(
        faculty_salary_data_enhanced,
        x="avg_faculty_salary",
        y="in_state_tuition",
        color="institution_name",
        title=plot_title4,
        labels={
            "avg_faculty_salary": "Average Faculty Salary (USD)",
            "in_state_tuition": "In-State Tuition (USD)",
            "institution_name": "Name of Instiution"
        },
        height=500
    )
    st.plotly_chart(fig_correlation, use_container_width=True)

else:
    st.warning("No data available for enhanced faculty salaries analysis.")


# Footer
st.write("**Note:** All data is sourced from the College Scorecard project.")
