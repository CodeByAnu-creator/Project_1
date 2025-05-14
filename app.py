import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import mysql.connector # for connecting MYSQL
import json # To load GeoJSON data
import os # Import os module to check file existence
from urllib.parse import quote_plus # Import quote_plus for password encoding

# Import credentials from the separate file
try:
    from credentials import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_DATABASE#, ENCODED_PASSWORD # ENCODED_PASSWORD is not used by mysql.connector
except ImportError:
    st.error("Error: credentials.py not found or incomplete. Please create credentials.py with your database details.")
    st.stop() # App stops if credentials are not available

# --- Database Configuration ---
def get_db_connection():
    # Establishing database connection using MySQL.
    try:
        # Use credentials imported from credentials.py
        conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_DATABASE,
            port=DB_PORT
        )
        return conn
    except mysql.connector.Error as e:
        st.error(f"Error connecting to the MySQL database: {e}")
        st.error(f"Please check your credentials in credentials.py and ensure the MySQL server is running.")
        return None

# --- SQL Queries ---
# Query for overall transaction trends by type
SQL_QUERY_AGGREGATED_TRANSACTION = """
SELECT
    year,
    quarter,
    transactiontype, -- Using column name from provided base code
    SUM(transactioncount) AS total_transaction_volume, -- Using column name from provided base code
    SUM(transactionamount) AS total_transaction_value -- Using column name from provided base code
FROM
    aggregated_transaction
GROUP BY
    year,
    quarter,
    transactiontype -- Using column name from provided base code
ORDER BY
    year,
    quarter,
    transactiontype; -- Using column name from provided base code
"""

# Queries for states with highest/lowest transaction volume and value for a specific period
SQL_QUERY_HIGHEST_VOLUME = """
SELECT
    state,
    SUM(transactioncount) AS total_volume -- Using column name from provided base code
FROM
    aggregated_transaction
WHERE
    year = {year}
    AND quarter = {quarter}
GROUP BY
    state
ORDER BY
    total_volume DESC
LIMIT 1;
"""

SQL_QUERY_LOWEST_VOLUME = """
SELECT
    state,
    SUM(transactioncount) AS total_volume -- Using column name from provided base code
FROM
    aggregated_transaction
WHERE
    year = {year}
    AND quarter = {quarter}
GROUP BY
    state
ORDER BY
    total_volume ASC
LIMIT 1;
"""

SQL_QUERY_HIGHEST_VALUE = """
SELECT
    state,
    SUM(transactionamount) AS total_value -- Using column name from provided base code
FROM
    aggregated_transaction
WHERE
    year = {year}
    AND quarter = {quarter}
GROUP BY
    state
ORDER BY
    total_value DESC
LIMIT 1;
"""

SQL_QUERY_LOWEST_VALUE = """
SELECT
    state,
    SUM(transactionamount) AS total_value -- Using column name from provided base code
FROM
    aggregated_transaction
WHERE
    year = {year}
    AND quarter = {quarter}
GROUP BY
    state
ORDER BY
    total_value ASC
LIMIT 1;
"""

# Query for district vs state comparison
SQL_QUERY_DISTRICT_VS_STATE = """
SELECT
    mt.state,
    mt.district,
    SUM(mt.transactioncount) AS district_total_volume,
    SUM(mt.transactionamount) AS district_total_value,
    (SELECT
        SUM(at.transactioncount)
      FROM
        aggregated_transaction at
      WHERE
        at.state = mt.state
        AND at.year = mt.year
        AND at.quarter = mt.quarter
    ) AS state_total_volume,
    (SELECT
        SUM(at.transactionamount)
      FROM
        aggregated_transaction at
      WHERE
        at.state = mt.state
        AND at.year = mt.year
        AND at.quarter = mt.quarter
    ) AS state_total_value
FROM
    map_transactions mt
WHERE
    mt.year = {year}
    AND mt.quarter = {quarter}
    AND mt.state = '{state}'
    AND mt.district = '{district}'
GROUP BY
    mt.state,
    mt.district;
"""

# Query for top 10 states by registered users
SQL_QUERY_TOP_10_USERS = """
SELECT
    state,
    SUM(count) AS total_registered_users
FROM
    aggregated_user
GROUP BY
    state
ORDER BY
    total_registered_users DESC
LIMIT 10;
"""

# Query for variations in transaction behavior across states (aggregated across all years/quarters)
SQL_QUERY_STATE_VARIATIONS = """
SELECT
    state,
    SUM(transactioncount) AS sumOfTransCount,
    SUM(transactionamount) AS sumOfTransAmount
FROM
    map_transactions -- Using table name from provided base code
GROUP BY
    state;
"""

# Query to get distinct years, quarters, and states for dropdowns
SQL_QUERY_YEARS_QUARTERS_STATES = """
SELECT DISTINCT year FROM aggregated_transaction ORDER BY year;
SELECT DISTINCT quarter FROM aggregated_transaction ORDER BY quarter;
SELECT DISTINCT state FROM aggregated_transaction ORDER BY state;
"""

# Query to get districts for a selected state
SQL_QUERY_DISTRICTS_BY_STATE = """
SELECT DISTINCT district
FROM map_transactions
WHERE state = '{state}'
ORDER BY district;
"""

# SQL Query for Growth Potential Analysis (from the user's selection)
SQL_QUERY_GROWTH_POTENTIAL = """
SELECT
    year,
    quarter,
    state,
    transactiontype,
    SUM(transactioncount) AS total_volume,
    SUM(transactionamount) AS total_value
FROM
    aggregated_transaction
GROUP BY
    year,
    quarter,
    state,
    transactiontype
ORDER BY
    state,
    transactiontype,
    year,
    quarter;
"""

# SQL Query for Registered Users by State and Brand
SQL_QUERY_REGISTERED_USERS_BY_BRAND = """
SELECT
    state,
    brand,
    SUM(registeredusers) AS total_registered_users
FROM
    aggregated_user -- Assuming 'aggregated_user' table contains state, brand, and registeredusers
GROUP BY
    state, brand
ORDER BY
    state, total_registered_users DESC;
"""

# SQL Query for Total Registered Users by Brand (for new bar chart)
SQL_QUERY_TOTAL_REGISTERED_USERS_BY_BRAND = """
SELECT
    brand,
    SUM(registeredusers) AS total_registered_users
FROM
    aggregated_user
GROUP BY
    brand
ORDER BY
    total_registered_users DESC;
"""

# SQL Query for PIN codes having the highest insurance transaction (using top_insurance_pincode)
SQL_QUERY_TOP_INSURANCE_PINCODE = """
SELECT
    pincode,
    SUM(InsuranceCount) AS total_insurance_volume, -- Using column name from provided schema
    SUM(InsuranceAmount) AS total_insurance_value -- Using column name from provided schema
FROM
    top_insurance_pincode -- Using top_insurance_pincode table from schema
GROUP BY
    pincode
ORDER BY
    total_insurance_volume DESC;
"""
#states recorded the highest number of insurance transactions in the selected year-quarter
SQL_QUERY_TOP_INSURANCE_STATES_BY_YEAR_QUARTER="""
SELECT
    state,
    SUM(Insurancecount) AS total_insurance_transactions
FROM
    aggregated_insurance
WHERE
    year = {selected_year}
    AND quarter = {selected_quarter}
GROUP BY
    state
ORDER BY
    total_insurance_transactions DESC;
"""


#states where insurance transactions 

SQL_QUERY_YEARLY_INSURANCE_COUNT_BY_STATE="""
SELECT
    state,
    year,
    SUM(InsuranceCount) AS total_year_volume 
FROM
    aggregated_insurance
GROUP BY
    state, year
ORDER BY
    state, year;
"""
# SQL Query for Top 10 States by Quarterly Transaction Volume
SQL_QUERY_TOP_10_STATES_QUARTERLY_VOLUME = """
SELECT
    state,
    year,
    quarter,
    SUM(transactioncount) AS quarterly_transaction_volume -- Summing transaction_count for each state, year, and quarter
FROM
    aggregated_transaction
GROUP BY
    state, year, quarter
ORDER BY
    quarterly_transaction_volume desc
    limit 30;
"""
SQL_QUERY_LOWEST_BRANDS = """
select distinct brand, sum(registeredUsers) as TotalUsers
from aggregated_user
group by brand
order by totalUsers asc
limit 10;
"""
SQL_QUERY_AppOpen_Highest_Rate = """
(
    SELECT 
        state, 
        SUM(registeredusers) AS total_registered_users, 
        SUM(appopens) AS total_app_opens, 
        CAST(SUM(appopens) AS DECIMAL) / NULLIF(SUM(registeredusers), 0) AS app_open_rate_per_user
    FROM map_users
    WHERE registeredusers IS NOT NULL AND appopens IS NOT NULL
    GROUP BY state
    HAVING SUM(registeredusers) > 0
    ORDER BY app_open_rate_per_user DESC
    LIMIT 5
);
"""
SQL_QUERY_AppOpen_Lowest_Rate = """
(
    SELECT 
        state, 
        SUM(registeredusers) AS total_registered_users, 
        SUM(appopens) AS total_app_opens, 
        CAST(SUM(appopens) AS DECIMAL) / NULLIF(SUM(registeredusers), 0) AS app_open_rate_per_user
    FROM map_users
    WHERE registeredusers IS NOT NULL AND appopens IS NOT NULL
    GROUP BY state
    HAVING SUM(registeredusers) > 0
    ORDER BY app_open_rate_per_user ASC
    LIMIT 5
);

"""
SQL_QUERY_LOW_USER_RATIO = """
SELECT
    au.state, -- State name
    SUM(au.registeredusers) AS total_registered_users, -- Total registered users for the state
    SUM(at.transactioncount) AS total_transaction_count, -- Total transaction count for the state
    CAST(SUM(at.transactioncount) AS DECIMAL) / NULLIF(SUM(au.registeredusers), 0) AS transaction_to_user_ratio
FROM
    aggregated_user au -- Alias for the aggregated_user table
JOIN
    aggregated_transaction at ON au.state = at.state -- Join on state
                               AND au.year = at.year -- Join on year
                               AND au.quarter = at.quarter -- Join on quarter
WHERE
    au.registeredusers IS NOT NULL -- Ensure user data is present
    AND at.transactioncount IS NOT NULL -- Ensure transaction data is present
GROUP BY
    au.state -- Group by state to aggregate data for each state
HAVING
    SUM(au.registeredusers) > 0
    order by transaction_to_user_ratio Asc
    limit 5;
"""

SQL_QUERY_HIGH_USER_RATIO = """
SELECT
    au.state, -- State name
    SUM(au.registeredusers) AS total_registered_users, -- Total registered users for the state
    SUM(at.transactioncount) AS total_transaction_count, -- Total transaction count for the state
    CAST(SUM(at.transactioncount) AS DECIMAL) / NULLIF(SUM(au.registeredusers), 0) AS transaction_to_user_ratio
FROM
    aggregated_user au -- Alias for the aggregated_user table
JOIN
    aggregated_transaction at ON au.state = at.state -- Join on state
                               AND au.year = at.year -- Join on year
                               AND au.quarter = at.quarter -- Join on quarter
WHERE
    au.registeredusers IS NOT NULL -- Ensure user data is present
    AND at.transactioncount IS NOT NULL -- Ensure transaction data is present
GROUP BY
    au.state -- Group by state to aggregate data for each state
HAVING
    SUM(au.registeredusers) > 0
    order by transaction_to_user_ratio DESC
    limit 5;
"""

# --- GeoJSON Data for India States ---
# Using a local file path for the GeoJSON data
INDIA_STATES_GEOJSON_PATH = r"C:\Users\abhij\OneDrive\Documents\Anu_Guvi\Project_1\PhonePe\states_india.geojson" # Use raw string for path

@st.cache_data # provides cached result and do not re-reads the file every time.
def load_geojson(filepath):
    """Loading GeoJSON data from a local file path."""
    if not os.path.exists(filepath):
        st.error(f"GeoJSON file not found at: {filepath}")
        return None
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            geojson_data = json.load(f)
        return geojson_data
    except Exception as e:
        st.error(f"Error loading GeoJSON data from {filepath}: {e}")
        return None

# Loading GeoJSON data to a variable
india_states_geojson = load_geojson(INDIA_STATES_GEOJSON_PATH)

# --- Data Loading Functions ---
@st.cache_data
def load_aggregated_transaction_data(query):
    conn = get_db_connection()
    if conn is None:
        st.error("Database connection failed.")
        return pd.DataFrame()
    try:
        df = pd.read_sql(query, conn)
        conn.close()
    except Exception as e: # Added specific exception handling
        st.error(f"Something went wrong while loading aggregated transaction data: {e}")
        return pd.DataFrame()
    df['period'] = df['year'].astype(str) + '-Q' + df['quarter'].astype(str)
    # Ensure correct order for plotting
    period_order = sorted(df['period'].unique())
    df['period'] = pd.Categorical(df['period'], categories=period_order, ordered=True)
    return df

@st.cache_data # Cache the data
def load_data_from_query(query):
    """General function to load data from SQL query."""
    conn = get_db_connection()
    if conn is not None:
        try:
            df = pd.read_sql(query, conn)
            conn.close()
            return df
        except Exception as e:
            st.error(f"Error executing SQL query or loading data: {e}")
            return pd.DataFrame() # Return empty DataFrame on error
    return pd.DataFrame() # Return empty DataFrame if connection failed


@st.cache_data # Cache the data
def get_dropdown_options():
    conn = get_db_connection()
    years = []
    quarters = []
    states = []
    if conn is None:
        st.error("Database connection failed for dropdown options.") # Added specific error message
        return [], [], []
    try:
        years = pd.read_sql("SELECT DISTINCT year FROM aggregated_transaction ORDER BY year;", conn)['year'].tolist() #to convert into python list
        quarters = pd.read_sql("SELECT DISTINCT quarter FROM aggregated_transaction ORDER BY quarter;", conn)['quarter'].tolist()
        states = pd.read_sql("SELECT DISTINCT state FROM aggregated_transaction ORDER BY state;", conn)['state'].tolist()
        conn.close()
        return years, quarters, states
    except Exception as e: # Added specific exception handling
        st.error(f"Error loading dropdown values: {e}")
        return [], [], []

@st.cache_data # Cache the data
def get_districts_for_state(state):
    """Fetches distinct districts for a given state."""
    conn = get_db_connection()
    districts = []
    if conn is not None and state:
        try:
            query = SQL_QUERY_DISTRICTS_BY_STATE.format(state=state)
            df_districts = pd.read_sql(query, conn)
            districts = df_districts['district'].tolist()
            conn.close()
        except Exception as e:
            st.error(f"Error fetching districts for state {state}: {e}")
        return districts
    return []

@st.cache_data # Cache the data
def get_transaction_types():
    """Fetches distinct transaction types."""
    conn = get_db_connection()
    transaction_types = []
    if conn is not None:
        try:
            df_types = pd.read_sql("SELECT DISTINCT transactiontype FROM aggregated_transaction ORDER BY transactiontype;", conn)
            transaction_types = df_types['transactiontype'].tolist()
            conn.close()
        except Exception as e:
            st.error(f"Error fetching transaction types: {e}")
        return transaction_types
    return []


# --- State Name Mapping ---
# This dictionary maps state names from your data (if they are in a different format from json file)
state_name_map = {
    'andaman-&-nicobar-islands': 'Andaman & Nicobar Island',
    'andhra-pradesh': 'Andhra Pradesh',
    'arunachal-pradesh': 'Arunanchal Pradesh',
    'assam': 'Assam',
    'bihar': 'Bihar',
    'chandigarh': 'Chandigarh',
    'chhattisgarh': 'Chhattisgarh',
    'dadra-&-nagar-haveli-&-daman-&-diu': 'Dadara & Nagar Havelli', # Note: This might need adjustment based on your GeoJSON
    'delhi': 'NCT of Delhi',
    'goa': 'Goa',
    'gujarat': 'Gujarat',
    'haryana': 'Haryana',
    'himachal-pradesh': 'Himachal Pradesh',
    'jammu-&-kashmir': 'Jammu & Kashmir',
    'jharkhand': 'Jharkhand',
    'karnataka': 'Karnataka',
    'kerala': 'Kerala',
    'lakshadweep': 'Lakshadweep',
    'madhya-pradesh': 'Madhya Pradesh',
    'maharashtra': 'Maharashtra',
    'manipur': 'Manipur',
    'meghalaya': 'Meghalaya',
    'mizoram': 'Mizoram',
    'nagaland': 'Nagaland',
    'odisha': 'Odisha',
    'puducherry': 'Puducherry',
    'punjab': 'Punjab',
    'rajasthan': 'Rajasthan',
    'sikkim': 'Sikkim',
    'tamil-nadu': 'Tamil Nadu',
    'telangana': 'Telangana',
    'tripura': 'Tripura',
    'uttar-pradesh': 'Uttar Pradesh',
    'uttarakhand': 'Uttarakhand',
    'west-bengal': 'West Bengal',
}


# --- Helper function to map state names and filter out missing ones ---
def prepare_state_data_for_map(df, geojson_features, data_col='state', geojson_prop='st_nm', states_to_filter=[]):
    """
    Applies state name mapping, filters out specified states.
    Does NOT display warnings or info messages about mismatches in the Streamlit app.
    """
    # Check if geojson_features is None or empty
    if not geojson_features:
        # Optionally log this for debugging in the terminal, but not in the app
        # print("Warning: GeoJSON features are not loaded or empty. Cannot prepare state data for mapping.")
        return df # Return original DataFrame if features are not available

    # Use .get() with a default value to avoid KeyError if 'properties' or geojson_prop is missing
    geojson_states = {feature.get('properties', {}).get(geojson_prop) for feature in geojson_features if feature.get('properties', {}).get(geojson_prop) is not None}

    # Apply the provided state_name_map to the DataFrame's state column
    # This assumes your DataFrame's state column uses the keys from state_name_map
    # If your DataFrame's state names already match the GeoJSON names, you can skip this mapping.
    # Use .copy() to avoid SettingWithCopyWarning
    df_prepared = df.copy()
    if data_col in df_prepared.columns:
        df_prepared[data_col] = df_prepared[data_col].replace(state_name_map)
    else:
        # Optionally log this for debugging if the state column is missing
        # print(f"Warning: '{data_col}' column not found in DataFrame.")
        return df # Return original DataFrame if state column is missing

    # --- Filter out specified states (like Ladakh) ---
    if states_to_filter and not df_prepared.empty and data_col in df_prepared.columns:
         df_prepared = df_prepared[~df_prepared[data_col].isin(states_to_filter)].copy() # Filter out states in the list

    # The following checks are kept for internal logic but messages are removed
    if data_col in df_prepared.columns:
        data_states_after_map_and_filter = set(df_prepared[data_col].unique())
        # Find states in data (after mapping and filtering) not in geojson
        mismatched_data_states = data_states_after_map_and_filter - geojson_states
        # Find states in geojson not in data (optional, but good for completeness)
        mismatched_geojson_states = geojson_states - data_states_after_map_and_filter

        # --- Removed Streamlit warning/info messages ---
        # if mismatched_data_states:
        #     pass # Messages removed

        # if mismatched_geojson_states:
        #     pass # Messages removed
        # --- End removed messages ---

    return df_prepared



# --- Main Navigation Bar (using sidebar radio buttons) ---
st.sidebar.title("Case studies") # Changed sidebar title to "Case studies"
# Added new main navigation option 'Decoding Transaction Dynamics on PhonePe'
# Added new case study option
page_selection = st.sidebar.radio("Go to", ["Home","Transaction Data Analysis", "Decoding Transaction Dynamics on PhonePe", "Device Dominance and User Engagement Analysis", "Insurance Transactions Analysis", "Transaction Analysis for Market Expansion"])
# --- Content based on Main Navigation Selection ---

if page_selection == "Home":
    st.subheader("Transaction Trends Over Time of PhonePe - Overview")

    st.markdown("""
    The Indian digital payments story has truly captured the world’s imagination. From the largest towns to the remotest villages, there is a payments revolution being driven by the penetration of mobile phones, mobile internet and state-of-art payments infrastructure built as Public Goods championed by the central bank and the government. PhonePe started in 2016 and has been a strong beneficiary of the API driven digitisation of payments in India.
    PhonePe is a leading digital payment platform in India, offering a range of financial services including mobile payments, banking, and online money transfers. Founded in 2015, it operates on the Unified Payments Interface (UPI) developed by the National Payments Corporation of India (NPCI). PhonePe is known for its user-friendly interface and widespread acceptance across merchants and businesses in India.

    This dashboard provides insights derived from transaction data available in the PhonePe Pulse repository. Here, you can explore transaction trends, geographical insights, and other key metrics.
    
    This view shows the overall transaction volume and value trends
    for all transaction types combined over time.
    """)

    # Load the data
    df = load_aggregated_transaction_data(SQL_QUERY_AGGREGATED_TRANSACTION)

    if df.empty:
        st.warning("Could not load data. Please check database connection details in credentials.py and ensure the table schema is correct.")
    else:
            
            # Aggregate data for the overview (sum across transaction types)
        overview_df = df.groupby('period')[['total_transaction_volume', 'total_transaction_value']].sum().reset_index()
        overview_df['period'] = pd.Categorical(overview_df['period'], categories=sorted(overview_df['period'].unique()), ordered=True)

            # Ensure 'period' is sorted and set as categorical for proper ordering (already done above)
        overview_df = overview_df.sort_values('period')
        st.subheader("Total Transaction Volume Over Time (Bar Chart)")
            # Plot using matplotlib
        fig, ax = plt.subplots(figsize=(12, 6))

        ax.bar(overview_df['period'], overview_df['total_transaction_volume'], color='skyblue')
        ax.set_title('Total Transaction Volume Over Time (All Types)')

        ax.set_xlabel('Time Period')
        ax.set_ylabel('Volume')
        ax.tick_params(axis='x', rotation=45)
        ax.grid(axis='y', linestyle='--', alpha=0.5)
        st.pyplot(fig)

            # Value Trend Chart (Overview)
        st.subheader("Total Transaction Value Over Time")
        fig_value_overview = px.line(
            overview_df,
            x='period',
            y='total_transaction_value',
            color_discrete_sequence=['indianred'], # Use a different color for differentiation
            title='Total Transaction Value Over Time (All Types)',
            labels={'total_transaction_value': 'Value', 'period': 'Time Period'}
        )
        fig_value_overview.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig_value_overview, use_container_width=True)

            # Optional: Display raw data
        if st.checkbox("Show Raw Data (Aggregated Overview)"):
            st.subheader("Raw Data (Aggregated Overview)")
            st.dataframe(overview_df)


elif page_selection == "Transaction Data Analysis":
    st.header("Transaction Data Analysis") # Added a sub-header for this section

    st.markdown("""
    This section visualizes transaction data trends.
    """)


    # --- Sub-navigation for Transaction Data Analysis (moved to main body) ---
    transaction_analysis_selection = st.selectbox(
        "Select Analysis Type",
        ["Most Popular Transaction Types", "States with Extreme Transactions", "District vs. State Performance", "Top 10 States by Registered Users"] # Sub-navigation options
    )

    

    if transaction_analysis_selection == "Most Popular Transaction Types":
        st.subheader("Most Popular Transaction Types Analysis")

        st.markdown("""
        This view allows you to analyze the volume and value trends for
        individual transaction types to see which are the most popular.
        """)

        # Load the data
        df = load_aggregated_transaction_data(SQL_QUERY_AGGREGATED_TRANSACTION)

        if df.empty:
            st.warning("Could not load data. Please check database connection details in credentials.py and ensure the table schema is correct.")
        else:
            # --- Visualizations for Individual Transaction Types ---

# Plot Volume Trend
# Aggregating total volume by transaction type
            volume_by_type = df.groupby('transactiontype')['total_transaction_volume'].sum()

# Create pie chart
            fig_pie, ax_pie = plt.subplots()
            ax_pie.pie(volume_by_type, labels=volume_by_type.index, autopct='%1.1f%%', startangle=90)
            ax_pie.set_title("Total Transaction Volume by Type")

#pie is drawn as circle
            ax_pie.axis('equal')
            st.pyplot(fig_pie)

# Grouping of data for Value by period and transactiontype
            st.subheader("Transaction Value Trend by Type")
            value_pivot = df.pivot_table(
                index='period',
                columns='transactiontype',
                values='total_transaction_value',
                aggfunc='sum'
            ).fillna(0).sort_index()

# Plot Value Trend
            fig_val, ax_val = plt.subplots(figsize=(12, 5))
            for col in value_pivot.columns:
                ax_val.plot(value_pivot.index, value_pivot[col], marker='o', label=col)

            ax_val.set_title("Total Transaction Value by Type Over Time")
            ax_val.set_xlabel("Time Period")
            ax_val.set_ylabel("Value")
            ax_val.legend(title="Transaction Type")
            ax_val.tick_params(axis='x', rotation=45)
            st.pyplot(fig_val)

            # Optional: Display raw data
            if st.checkbox("Show Raw Data (By Transaction Type)"):
                st.subheader("Raw Data (By Transaction Type)")
                st.dataframe(df) # Display the full dataframe

    elif transaction_analysis_selection == "States with Extreme Transactions":
        st.subheader("States with Highest/Lowest Transaction Volume and Value")

        st.markdown("""
        Select a year and quarter to find the states with the highest and lowest
        total transaction volume and value during that period.
        """)

        # --- Year and Quarter Selection ---
        years, quarters, states = get_dropdown_options()

        if not years or not quarters or not states:
             st.warning("Could not load years, quarters, or states from the database. Please check your connection and data.")
        else:
            selected_year = st.selectbox("Select Year", years, key='extreme_year') # Added unique key
            selected_quarter = st.selectbox("Select Quarter", quarters, key='extreme_quarter') # Added unique key


            # --- Fetch and Display Extreme States ---
            if st.button(f"Analyze for {selected_year} Q{selected_quarter}", key='analyze_extreme'): # Added unique key
                st.write(f"Analyzing data for {selected_year} Quarter {selected_quarter}...")

                # Fetch data for highest volume
                query_highest_volume = SQL_QUERY_HIGHEST_VOLUME.format(year=selected_year, quarter=selected_quarter)
                df_highest_volume = load_data_from_query(query_highest_volume) # Using generic loader

                # Fetch data for lowest volume
                query_lowest_volume = SQL_QUERY_LOWEST_VOLUME.format(year=selected_year, quarter=selected_quarter)
                df_lowest_volume = load_data_from_query(query_lowest_volume) # Using generic loader

                # Fetch data for highest value
                query_highest_value = SQL_QUERY_HIGHEST_VALUE.format(year=selected_year, quarter=selected_quarter)
                df_highest_value = load_data_from_query(query_highest_value) # Using generic loader

                # Fetch data for lowest value
                query_lowest_value = SQL_QUERY_LOWEST_VALUE.format(year=selected_year, quarter=selected_quarter)
                df_lowest_value = load_data_from_query(query_lowest_value) # Using generic loader

                # --- Display Results ---
                if not df_highest_volume.empty:
                    st.subheader("Highest Transaction Volume")
                    st.dataframe(df_highest_volume)
                else:
                    st.info("Could not retrieve data for highest transaction volume for the selected period.")

                if not df_lowest_volume.empty:
                    st.subheader("Lowest Transaction Volume")
                    st.dataframe(df_lowest_volume)
                else:
                    st.info("Could not retrieve data for lowest transaction volume for the selected period.")

                if not df_highest_value.empty:
                    st.subheader("Highest Transaction Value")
                    st.dataframe(df_highest_value)
                else:
                    st.info("Could not retrieve data for highest transaction value for the selected period.")

                if not df_lowest_value.empty:
                    st.subheader("Lowest Transaction Value")
                    st.dataframe(df_lowest_value)
                else:
                    st.info("Could not retrieve data for lowest transaction value for the selected period.")

    elif transaction_analysis_selection == "District vs. State Performance":
        st.subheader("District Performance Compared to State Total")

        st.markdown("""
        Select a year, quarter, state, and district to compare the district's
        total transaction volume and value against the total for its state.
        """)

        # --- Year, Quarter, State, and District Selection ---
        years, quarters, states = get_dropdown_options()

        if not years or not quarters or not states:
             st.warning("Could not load years, quarters, or states from the database. Please check your connection and data.")
        else:
            selected_year = st.selectbox("Select Year", years, key='district_year') # Added unique key
            selected_quarter = st.selectbox("Select Quarter", quarters, key='district_quarter') # Added unique key
            selected_state = st.selectbox("Select State", states, key='district_state') # Added unique key

            # Dynamically load districts based on selected state
            districts = get_districts_for_state(selected_state)

            if not districts:
                st.warning(f"Could not load districts for {selected_state}. Please check your data in map_transactions.") # Using table name from provided base code
            else:
                selected_district = st.selectbox("Select District", districts, key='district_district') # Added unique key


                # --- Fetch and Display Comparison Data ---
                if st.button(f"Compare {selected_district} ({selected_state}) Performance", key='compare_district'): # Added unique key
                    st.write(f"Comparing {selected_district} ({selected_state}) performance for {selected_year} Q{selected_quarter}...")

                    # Format and fetch data
                    query_district_vs_state = SQL_QUERY_DISTRICT_VS_STATE.format(
                        year=selected_year,
                        quarter=selected_quarter,
                        state=selected_state,
                        district=selected_district
                    )
                    df_comparison = load_data_from_query(query_district_vs_state) # Using generic loader

                    # --- Display Results and Visualization ---
                    if not df_comparison.empty:
                        st.subheader("Comparison Results")
                        st.dataframe(df_comparison)

                        # Prepare data for plotting
                        # Reshape data for easier plotting (e.g., using melt)
                        df_melted_volume = df_comparison[['district', 'state', 'district_total_volume', 'state_total_volume']].melt(
                            id_vars=['district', 'state'],
                            var_name='Scope',
                            value_name='Total Volume'
                        )
                        df_melted_volume['Scope'] = df_melted_volume['Scope'].replace({
                            'district_total_volume': f'{selected_district} (District)',
                            'state_total_volume': f'{selected_state} (State Total)'
                        })


                        df_melted_value = df_comparison[['district', 'state', 'district_total_value', 'state_total_value']].melt(
                            id_vars=['district', 'state'],
                            var_name='Scope',
                            value_name='Total Value'
                        )
                        df_melted_value['Scope'] = df_melted_value['Scope'].replace({
                            'district_total_value': f'{selected_district} (District)',
                            'state_total_value': f'{selected_state} (State Total)'
                        })


                        # Volume Comparison Bar Chart
                        st.subheader("Volume Comparison: District vs. State Total")
                        fig_volume_comparison = px.bar(
                            df_melted_volume,
                            x='Scope',
                            y='Total Volume',
                            title=f"Transaction Volume: {selected_district} vs. {selected_state} Total ({selected_year} Q{selected_quarter})",
                            labels={'Total Volume': 'Transaction Volume'},
                            color='Scope' # Color bars by Scope (District/State)
                        )
                        fig_volume_comparison.update_layout(xaxis_tickangle=-45)
                        st.plotly_chart(fig_volume_comparison, use_container_width=True)

                        # Value Comparison Bar Chart
                        st.subheader("Value Comparison: District vs. State Total")
                        fig_value_comparison = px.bar(
                            df_melted_value,
                            x='Scope',
                            y='Total Value',
                             title=f"Transaction Value: {selected_district} vs. {selected_state} Total ({selected_year} Q{selected_quarter})",
                            labels={'Total Value': 'Transaction Value'},
                             color='Scope' # Color bars by Scope (District/State)
                        )
                        fig_value_comparison.update_layout(xaxis_tickangle=-45)
                        st.plotly_chart(fig_value_comparison, use_container_width=True)


                    else:
                         st.info(f"Could not retrieve comparison data for {selected_district} ({selected_state}) for {selected_year} Q{selected_quarter}. Please check if data exists for this period and location.")

    elif transaction_analysis_selection == "Top 10 States by Registered Users":
        st.subheader("Top 10 States by Total Registered Users")

        st.markdown("""
        This visualization shows the top 10 states with the highest total number of registered PhonePe users.
        """)

        # Load the data for top users
        df_top_users = load_data_from_query(SQL_QUERY_TOP_10_USERS) # Using generic loader

        if df_top_users.empty:
            st.warning("Could not load data for top 10 states by registered users. Please check your database connection and the 'aggregated_user' table.")
        else:
            # --- Visualization for Top 10 Users ---

            st.subheader("Top 10 States by Registered User Count")
            fig_top_users = px.bar(
                df_top_users,
                x='state',
                y='total_registered_users',
                title='Top 10 States by Total Registered Users',
                labels={'state': 'State', 'total_registered_users': 'Total Registered Users'},
                color='state' # Color bars by state
            )
            fig_top_users.update_layout(xaxis_tickangle=-45) # Angle x-axis labels for readability
            st.plotly_chart(fig_top_users, use_container_width=True)

            # Optional: Display raw data
            if st.checkbox("Show Raw Data (Top 10 States by Users)"):
                st.subheader("Raw Data (Top 10 States by Users)")
                st.dataframe(df_top_users)


# --- Decoding Transaction Dynamics on PhonePe Case Study ---
elif page_selection == "Decoding Transaction Dynamics on PhonePe":
    st.header("Decoding Transaction Dynamics on PhonePe")

    st.markdown("""
    This section explores various aspects of transaction behavior on the PhonePe platform.
    """)

    # --- Sub-navigation for Decoding Transaction Dynamics ---
    decoding_dynamics_selection = st.selectbox(
        "Select Analysis Type",
        ["Variations in transaction behavior across states", "Potential for Growth of payment categories"] # Added new option here
    )

    # --- Content based on Decoding Transaction Dynamics Sub-navigation ---
    if decoding_dynamics_selection == "Variations in transaction behavior across states":
        st.subheader("Variations in Transaction Behavior Across States")

        st.markdown("""
        This section visualizes the total transaction volume and value aggregated across all years and quarters for each state,
        using either bar charts or an interactive India map.
        """)

        # --- Visualization Type Selection ---
        viz_type = st.radio("Select Visualization Type", ["Bar Charts", "India Map"]) # Renamed for clarity

        # Load the data for state variations
        df_state_variations = load_data_from_query(SQL_QUERY_STATE_VARIATIONS)

        if df_state_variations.empty:
             st.warning("Could not load data for state variations. Please check your database connection and the 'map_transactions' table.") # Using table name from provided base code
        elif india_states_geojson is None:
             st.warning("Could not load GeoJSON data for India states. Map visualization is not available. Please check the file path.")
        else:
            # --- State Name Mapping and Filtering ---
            # Use 'st_nm' as the key for state names from your GeoJSON
            geojson_state_key = 'st_nm' # !!! VERIFY THIS KEY IN YOUR india.json !!!

            # Filter out 'Ladakh' as it might not be in your GeoJSON or data consistently
            # Also filter out 'Dadra and Nagar Haveli' if your GeoJSON handles it separately from Daman and Diu
            states_to_exclude = ['Ladakh']
             # You might need to add 'Dadra and Nagar Haveli' here depending on your GeoJSON and data

            df_state_variations_prepared = prepare_state_data_for_map(
                df_state_variations.copy(),
                india_states_geojson.get('features', []), # Use .get() for safety
                data_col='state',
                geojson_prop=geojson_state_key,
                states_to_filter=states_to_exclude
            )

            # Check if preparation was successful enough to proceed with mapping
            # Check if the 'state' column exists in the prepared DataFrame before proceeding
            if not df_state_variations_prepared.empty and 'state' in df_state_variations_prepared.columns:

                 # Get the set of state names from the prepared data for the final check
                 data_states_for_plotting = set(df_state_variations_prepared['state'].unique())
                 # Get the set of state names from the GeoJSON using the correct key
                 geojson_states = {feature.get('properties', {}).get(geojson_state_key) for feature in india_states_geojson.get('features', []) if feature.get('properties', {}).get(geojson_state_key) is not None}

                 # Final check for mismatches before plotting
                 mismatched_for_plotting = data_states_for_plotting - geojson_states

                 # Provide feedback on mismatches even if not stopping plotting
                 if mismatched_for_plotting:
                      st.warning(f"The following states in your data (after mapping and filtering) were not found in the GeoJSON features using key '{geojson_state_key}': {mismatched_for_plotting}")
                      st.info("These states will not be colored on the map. Please check your data and GeoJSON.")


                 if viz_type == "Bar Charts":
                    # --- Bar Charts for State Variations ---

                    # Volume Variation Bar Chart
                    st.subheader("Total Transaction Volume by State (All Periods)")

                    fig_state_volume, ax_state_volume = plt.subplots(figsize=(12, 7))

# Plotting bar chart
                    ax_state_volume.bar(
                        df_state_variations_prepared['state'],
                        df_state_variations_prepared['sumOfTransCount'],
                        color='skyblue',
                        width=0.6
                    )

# Add titles and labels
                    ax_state_volume.set_title('Total Transaction Volume by State (Aggregated)')
                    ax_state_volume.set_xlabel('State')
                    ax_state_volume.set_ylabel('Total Transaction Volume')
                    ax_state_volume.tick_params(axis='x', rotation=90)
                    plt.tight_layout()
                    

# Annotate bar values
                    for i, v in enumerate(df_state_variations_prepared['sumOfTransCount']):
                        ax_state_volume.text(i, v + max(df_state_variations_prepared['sumOfTransCount']) * 0.01, f'{v:,.0f}', ha='center', fontsize=8)

# Display in Streamlit
                    st.pyplot(fig_state_volume)


                    # Value Variation Bar Chart
                    st.subheader("Total Transaction Value by State (All Periods)")
                    fig_value_variations = px.bar(
                        df_state_variations_prepared, # Use prepared data
                        x='state',
                        y='sumOfTransAmount',
                        title='Total Transaction Value by State (Aggregated)',
                        labels={'state': 'State', 'sumOfTransAmount': 'Total Transaction Value'},
                        color='state' # Color bars by state
                    )
                    fig_value_variations.update_layout(xaxis_tickangle=-45) # Angle x-axis labels for readability
                    st.plotly_chart(fig_value_variations, use_container_width=True)

                 elif viz_type == "India Map": # Updated name
                     # --- India Map for State Variations ---

                     # Volume Map
                     st.subheader("Total Transaction Volume Across States (Map)")
                     fig_volume_map = px.choropleth(
                         df_state_variations_prepared, # Use prepared data
                         geojson=india_states_geojson,
                         locations='state', # Column in dataframe with state names (after mapping and filtering)
                         featureidkey=f"properties.{geojson_state_key}", # Path to state name in GeoJSON features
                         color='sumOfTransCount', # Column with the value to color the map
                         hover_name='state', # Column for tooltip
                         hover_data={'sumOfTransCount': True, 'sumOfTransAmount': ':,.2f'}, # Additional data in tooltip
                         title='Total Transaction Volume by State (Map)',
                         labels={'sumOfTransCount': 'Total Volume'},
                         # Adjust map projection and center for India
                         # scope="asia", # Commenting out scope as fitbounds is used
                         # center={"lat": 23.4, "lon": 80.9}, # Commenting out center as fitbounds is used
                         # projection="mercator" # Default projection is often Mercator, explicitly setting is fine
                     )
                     # Use fitbounds="geojson" to automatically zoom to the GeoJSON layer
                     # Corrected: Replaced showstates=True with showsubunits=True
                     fig_volume_map.update_geos(showcountries=False, showsubunits=True, showcoastlines=False, fitbounds="geojson")
                     fig_volume_map.update_layout(margin={"r":0,"t":40,"l":0,"b":0})
                     st.plotly_chart(fig_volume_map, use_container_width=True)

                     # Value Map
                     st.subheader("Total Transaction Value Across States (Map)")
                     fig_value_map = px.choropleth(
                         df_state_variations_prepared, # Use prepared data
                         geojson=india_states_geojson,
                         locations='state', # Column in dataframe with state names (after mapping and filtering)
                         featureidkey=f"properties.{geojson_state_key}", # Path to state name in GeoJSON features
                         color='sumOfTransAmount', # Column with the value to color the map
                         hover_name='state', # Column for tooltip
                         hover_data={'sumOfTransCount': True, 'sumOfTransAmount': ':,.2f'}, # Corrected hover_data key
                         title='Total Transaction Value by State (Map)',
                         labels={'sumOfTransAmount': 'Total Value'},
                         # Adjust map projection and center for India
                         # scope="asia", # Commenting out scope as fitbounds is used
                         # center={"lat": 23.4, "lon": 80.9}, # Commenting out center as fitbounds is used
                         # projection="mercator" # Default projection is often Mercator, explicitly setting is fine
                     )
                     # Use fitbounds="geojson" to automatically zoom to the GeoJSON layer
                     # Corrected: Replaced showstates=True with showsubunits=True
                     fig_value_map.update_geos(showcountries=False, showsubunits=True, showcoastlines=False, fitbounds="geojson")
                     fig_value_map.update_layout(margin={"r":0,"t":40,"l":0,"b":0})
                     st.plotly_chart(fig_value_map, use_container_width=True)


                 # Optional: Display raw data
                 if st.checkbox("Show Raw Data (State Variations)"):
                     st.subheader("Raw Data (State Variations)")
                     st.dataframe(df_state_variations_prepared) # Display the prepared data
            else:
                 st.warning("State data could not be prepared for visualization. Please check data loading, GeoJSON, and state name mapping.")

    # --- New Section: Potential for Growth of Payment Categories ---
    elif decoding_dynamics_selection == "Potential for Growth of payment categories":
        st.header("Potential for Growth of Payment Categories") # Changed to header for consistency

        st.markdown("""
        Analyze transaction volume and value trends across states and payment categories
        to identify areas and categories with high growth potential.
        """)

        # Load the data using the selected SQL query
        df_growth_potential = load_data_from_query(SQL_QUERY_GROWTH_POTENTIAL)

        if df_growth_potential.empty:
            st.warning("Could not load data for growth potential analysis. Please check your database connection and the 'aggregated_transaction' table.")
        else:
            # Combine year and quarter for a time-based axis
            df_growth_potential['period'] = df_growth_potential['year'].astype(str) + '-Q' + df_growth_potential['quarter'].astype(str)
            # Ensure correct order for plotting
            period_order = sorted(df_growth_potential['period'].unique())
            df_growth_potential['period'] = pd.Categorical(df_growth_potential['period'], categories=period_order, ordered=True)

            # --- Visualization Type Selection for Growth Potential Section ---
            # Removed "India Map with Bubbles" option
            growth_viz_type = st.radio("Select Visualization Type", ["Trend Charts", "Pie Charts by State/Category"], key='growth_viz_type') # Updated option list

            # --- Content based on Growth Potential Visualization Type ---
            if growth_viz_type == "Trend Charts":

                st.markdown("""
                View transaction volume and value trends over time for different payment categories,
                with optional filtering by state.
                """)

                # --- Interactive Filters for Trend Charts ---
                st.sidebar.subheader("Filter Trend Data")
                all_states = ['All'] + sorted(df_growth_potential['state'].unique())
                selected_state_filter_trend = st.sidebar.selectbox("Select State for Trend", all_states, key='growth_state_filter_trend')

                # Apply state filter
                filtered_df_trend = df_growth_potential.copy()
                if selected_state_filter_trend != 'All':
                    filtered_df_trend = filtered_df_trend[filtered_df_trend['state'] == selected_state_filter_trend]

                if filtered_df_trend.empty:
                    st.info("No data available for the selected state for trend charts.")
                else:
                    # Volume Trend by Category (filtered)
                    st.subheader("Transaction Volume Trend by Category")
                    if selected_state_filter_trend != 'All':
                         title_volume = f"Transaction Volume Trend by Category in {selected_state_filter_trend}"
                    else:
                         title_volume = "Transaction Volume Trend by Category (All States)"

                    # Pivot data for volume trend
                    volume_pivot = filtered_df_trend.pivot_table(
                        index='period',
                        columns='transactiontype',
                        values='total_volume',
                        aggfunc='sum'
                    ).fillna(0).sort_index()

                    fig_vol, ax_vol = plt.subplots(figsize=(12, 5))
                    for col in volume_pivot.columns:
                       ax_vol.plot(volume_pivot.index, volume_pivot[col], marker='o', label=col)

                    ax_vol.set_title(title_volume)
                    ax_vol.set_xlabel("Time Period")
                    ax_vol.set_ylabel("Total Volume")
                    ax_vol.legend(title="Transaction Type")
                    ax_vol.tick_params(axis='x', rotation=45)
                    st.pyplot(fig_vol)

                    # --- Transaction Value Trend by Category ---
                    st.subheader("Transaction Value Trend by Category")
                    if selected_state_filter_trend != 'All':
                        title_value = f"Transaction Value Trend by Category in {selected_state_filter_trend}"
                    else:
                        title_value = "Transaction Value Trend by Category (All States)"

# Pivot data for value trend
                    value_pivot = filtered_df_trend.pivot_table(
                        index='period',
                        columns='transactiontype',
                        values='total_value',
                        aggfunc='sum'
                    ).fillna(0).sort_index()

                    fig_val, ax_val = plt.subplots(figsize=(12, 5))
                    for col in value_pivot.columns:
                        ax_val.plot(value_pivot.index, value_pivot[col], marker='o', label=col)

                    ax_val.set_title(title_value)
                    ax_val.set_xlabel("Time Period")
                    ax_val.set_ylabel("Total Value")
                    ax_val.legend(title="Transaction Type")
                    ax_val.tick_params(axis='x', rotation=45)
                    st.pyplot(fig_val)
               
            elif growth_viz_type == "Pie Charts by State/Category": # Updated option name

                 st.markdown("""
                 View the distribution of total transaction volume and value by State (when 'All' States is selected)
                 or by Payment Category (when a specific State is selected) using pie charts.
                 """)

                 # --- Interactive Filters for Pie Charts ---
                 st.sidebar.subheader("Filter Pie Chart Data")
                 all_states = ['All'] + sorted(df_growth_potential['state'].unique())
                 selected_state_filter_pie = st.sidebar.selectbox("Select State for Pie Chart", all_states, key='growth_state_filter_pie') # Updated key

                 # Removed the transaction type filter selectbox for pie charts
                 # all_transaction_types = ['All'] + sorted(df_growth_potential['transactiontype'].unique())
                 # selected_type_filter_pie = st.sidebar.selectbox("Select Transaction Type for Pie Chart", all_transaction_types, key='growth_type_filter_pie') # Updated key

                 # Apply filters (only state filter remains)
                 filtered_df_pie = df_growth_potential.copy()
                 if selected_state_filter_pie != 'All':
                     filtered_df_pie = filtered_df_pie[filtered_df_pie['state'] == selected_state_filter_pie]
                 # Removed the transaction type filter condition
                 # if selected_type_filter_pie != 'All':
                 #     filtered_df_pie = filtered_df_pie[filtered_df_pie['transactiontype'] == selected_type_filter_pie]

                 if filtered_df_pie.empty:
                     st.info("No data available for the selected filters for pie charts.")
                 else:
                     # Determine whether to group by state or transaction type for the pie chart
                     if selected_state_filter_pie == 'All':
                         # Group by state when 'All' states are selected (shows distribution across states)
                         st.subheader("Total Volume Distribution by State (All Transaction Types)")
                         df_volume_agg = filtered_df_pie.groupby('state')['total_volume'].sum().reset_index()
                         fig_volume_pie = px.pie(
                             df_volume_agg,
                             values='total_volume',
                             names='state',
                             title="Total Transaction Volume Distribution by State (All Transaction Types)",
                             hole=.3
                         )
                         st.plotly_chart(fig_volume_pie, use_container_width=True)

                         st.subheader("Total Value Distribution by State (All Transaction Types)")
                         df_value_agg = filtered_df_pie.groupby('state')['total_value'].sum().reset_index()
                         fig_value_pie = px.pie(
                             df_value_agg,
                             values='total_value',
                             names='state',
                             title="Total Transaction Value Distribution by State (All Transaction Types)",
                             hole=.3
                         )
                         st.plotly_chart(fig_value_pie, use_container_width=True)

                     else: # A specific state is selected
                         # Group by transaction type when a specific state is selected (shows distribution across categories in that state)
                         st.subheader(f"Total Volume Distribution by Category in '{selected_state_filter_pie}'")
                         df_volume_agg = filtered_df_pie.groupby('transactiontype')['total_volume'].sum().reset_index()
                         fig_volume_pie = px.pie(
                             df_volume_agg,
                             values='total_volume',
                             names='transactiontype',
                             title=f"Total Transaction Volume Distribution in '{selected_state_filter_pie}' by Category",
                             hole=.3
                         )
                         st.plotly_chart(fig_volume_pie, use_container_width=True)

                         st.subheader(f"Total Value Distribution by Category in '{selected_state_filter_pie}'")
                         df_value_agg = filtered_df_pie.groupby('transactiontype')['total_value'].sum().reset_index()
                         fig_value_pie = px.pie(
                             df_value_agg,
                             values='total_value',
                             names='transactiontype',
                             title=f"Total Transaction Value Distribution in '{selected_state_filter_pie}' by Category",
                             hole=.3
                         )
                         st.plotly_chart(fig_value_pie, use_container_width=True)


# --- New Case Study: Device Dominance and User Engagement Analysis ---
elif page_selection == "Device Dominance and User Engagement Analysis":
    st.header("Device Dominance and User Engagement Analysis")

    st.markdown("""
    This section analyzes user engagement patterns, including device usage and registered user distribution.
    """)

    # --- Sub-navigation for Device Dominance ---
    device_analysis_selection = st.selectbox(
        "Select Analysis Type",
        ["Highest Number of Registered Users","Total Registered Users by Brand", "Lowest Users by Brand", "App open highest rate per registered user", "App open lowest rate per registered user"] # Added new option here
    )

    # --- Content based on Device Dominance Sub-navigation ---
    if device_analysis_selection == "Highest Number of Registered Users":
        st.subheader("Registered Users by State and Brand")

        st.markdown("""
        This heatmap visualizes the total number of registered users across different states and mobile brands.
        """)

        # Load the data using the new SQL query
        df_registered_users = load_data_from_query(SQL_QUERY_REGISTERED_USERS_BY_BRAND)

        if df_registered_users.empty:
            st.warning("Could not load data for registered users by brand. Please check your database connection and the 'aggregated_user' table.")
        else:
            # --- Data Preparation for Heatmap ---
            # Pivot the data to get states as rows, brands as columns, and total_registered_users as values
            heatmap_data = df_registered_users.pivot_table(
                index='state',
                columns='brand',
                values='total_registered_users',
                fill_value=0 # Fill missing values with 0
            )

            # --- Create Heatmap Visualization ---
            st.subheader("Total Registered Users Heatmap by State and Brand")

            plt.figure(figsize=(12, 8))
            heatmap_fig = sns.heatmap(
                heatmap_data,
                fmt=".0f", cmap="viridis",  # Show values and use Viridis colormap
                linewidths=0.5, linecolor='gray',
                cbar_kws={'label': 'Total Registered Users'}
            )

            # Set axis labels and title
            plt.title("Total Registered Users by State and Brand", fontsize=16)
            plt.xlabel("Brand")
            plt.ylabel("State")
            plt.xticks(rotation=45)
            plt.yticks(rotation=0)

            # Display in Streamlit
            st.pyplot(plt)

            # Optional: Display raw data
            if st.checkbox("Show Raw Data (Registered Users by Brand)"):
                st.subheader("Raw Data (Registered Users by Brand)")
                st.dataframe(df_registered_users)


    elif device_analysis_selection == "Total Registered Users by Brand": # New option
        st.subheader("Total Registered Users Distribution by Brand")

        st.markdown("""
        This pie chart shows the distribution of total registered users across different mobile brands.
        """)

        # Load the data for total registered users by brand
        df_total_users_by_brand = load_data_from_query(SQL_QUERY_TOTAL_REGISTERED_USERS_BY_BRAND) # Using the specific query

        if df_total_users_by_brand.empty:
            st.warning("Could not load data for total registered users by brand. Please check your database connection and the 'aggregated_user' table.")
        else:
            # --- Create Matplotlib Pie Chart ---
            st.subheader("Total Registered Users by Brand")
            fig, ax = plt.subplots(figsize=(8, 8))  # Create a figure and axes for the pie chart

            # Extract data
            sizes = df_total_users_by_brand['total_registered_users']
            labels = df_total_users_by_brand['brand']

            # Create the pie chart and store the wedges (for legend colors)
            wedges, texts, autotexts = ax.pie(
                sizes,
                labels=None,  # Don't use labels on the pie slices
                autopct='%1.1f%%',  # Show percentages with one decimal place
                startangle=140
            )

            # Create custom legend labels (e.g., "Samsung: 1,000,000")
            legend_labels = [
                f"{brand}: {value:,}" for brand, value in zip(labels, sizes)
            ]

            # Add the legend with brand names and values
            ax.legend(wedges, legend_labels, title="Brand (Registered Users)", loc="center left", bbox_to_anchor=(1, 0.5))

            ax.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
            ax.set_title("Total Registered Users by Brand Distribution")  # Set title

            # Display the plot in Streamlit
            st.pyplot(fig)

    elif device_analysis_selection == "Lowest Users by Brand":
        st.subheader("Lowest Users by Brand")

        st.markdown("""
        This bar chart shows the distribution of least users using brands.
        """)
        df_lowest_users_by_brand = load_data_from_query(SQL_QUERY_LOWEST_BRANDS)

        if df_lowest_users_by_brand.empty:
            st.warning("Could not load data for total registered users by brand. Please check your database connection and the 'aggregated_user' table.")
        else:
            st.subheader("Lowest Registered Users by Brand")
            fig, ax = plt.subplots(figsize=(8, 8))  # Create a figure and axes for the pie chart

            # Extract data
            sizes = df_lowest_users_by_brand['TotalUsers']
            labels = df_lowest_users_by_brand['brand']

            # Create the pie chart and store the wedges (for legend colors)
            wedges, texts, autotexts = ax.pie(
                sizes,
                labels=None,  # Don't use labels on the pie slices
                autopct='%1.1f%%',  # Show percentages with one decimal place
                startangle=100,
                wedgeprops=dict(width=0.5) 
            )

            # Create custom legend labels (e.g., "Samsung: 1,000,000")
            legend_labels = [
                f"{brand}: {value:,}" for brand, value in zip(labels, sizes)
            ]

            # Add the legend with brand names and values
            ax.legend(wedges, legend_labels, title="Brand (Registered Users)", loc="center left", bbox_to_anchor=(1, 0.5))

            ax.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
            ax.set_title("Lowest Registered Users by Brand Distribution")  # Set title

            # Display the plot in Streamlit
            st.pyplot(fig)

            # Optional: Display raw data checkbox
            if st.checkbox("Show Raw Data (Least usered Brands)"):
                st.subheader("Raw Data (Least usered Brands)")
                st.dataframe(df_lowest_users_by_brand)

    elif device_analysis_selection == "App open highest rate per registered user":
        st.subheader("app open highest rate per registered user")

        st.markdown("""
        This bar chart shows the distribution of App open rate.
        """)
        df_AppOpen_Rate = load_data_from_query(SQL_QUERY_AppOpen_Highest_Rate)

        if df_AppOpen_Rate.empty:
            st.warning("Could not load data for total registered users. Please check your database connection and the 'map_users' table.")
        else:
            st.subheader("App open rate by states")



            fig, ax = plt.subplots(figsize=(10, 6)) # Create a figure and an axes.

            ax.barh(df_AppOpen_Rate['state'], df_AppOpen_Rate['app_open_rate_per_user'], color='lightgreen')

            ax.set_title('App Open Rate Per Registered User by State (Top 5)')
            ax.set_xlabel('App Open Rate Per Registered User')
            ax.set_ylabel('State')

            for index, value in enumerate(df_AppOpen_Rate['app_open_rate_per_user']):
                ax.text(value, index, f'{value:.2f}', va='center') # Place text at the end of each bar

            plt.tight_layout()

            st.pyplot(fig)

            st.write("""
            **Note:** The data displayed is sample data. Replace the `data` dictionary
            with the actual results fetched from your database query.
            """)

    elif device_analysis_selection == "App open lowest rate per registered user":
        st.subheader("app open lowest rate per registered user")

        st.markdown("""
        This bar chart shows the distribution of App open rate.
        """)
        df_AppOpen_Lowest_Rate = load_data_from_query(SQL_QUERY_AppOpen_Lowest_Rate)

        if df_AppOpen_Lowest_Rate.empty:
            st.warning("Could not load data for total registered users. Please check your database connection and the 'map_users' table.")
        else:
            st.subheader("App open rate by states")



            fig, ax = plt.subplots(figsize=(10, 6)) # Create a figure and an axes.

            ax.barh(df_AppOpen_Lowest_Rate['state'], df_AppOpen_Lowest_Rate['app_open_rate_per_user'], color='lightblue')

            ax.set_title('App Open Rate Per Registered User by State (Least 5)')
            ax.set_xlabel('App Open Rate Per Registered User')
            ax.set_ylabel('State')

            for index, value in enumerate(df_AppOpen_Lowest_Rate['app_open_rate_per_user']):
                ax.text(value, index, f'{value:.2f}', va='center') # Place text at the end of each bar

            plt.tight_layout()

            st.pyplot(fig)

            st.write("""
            **Note:** The data displayed is sample data. Replace the `data` dictionary
            with the actual results fetched from your database query.
            """)





# --- New Case Study: Insurance Transactions Analysis ---
elif page_selection == "Insurance Transactions Analysis":
    st.header("Insurance Transactions Analysis") # New Header

    st.markdown("""
    This section focuses on analyzing insurance transaction data.
    """)

    # --- Sub-navigation for Insurance Transactions Analysis ---
    insurance_analysis_options = ["PIN codes having the highest insurance transaction", "States recorded the highest number of insurance transactions", "States where insurance transactions declined"]
    insurance_analysis_selection = st.selectbox(
        "Select Analysis Type",
        insurance_analysis_options
    )

    # --- Content based on Insurance Transactions Analysis Sub-navigation ---
    if insurance_analysis_selection == "PIN codes having the highest insurance transaction":
        st.subheader("PIN Codes with Highest Insurance Transaction Volume")

        st.markdown("""
        This section shows the PIN codes with the highest insurance transaction volumes.
        """)

        # Load the data for top insurance pincodes
        df_top_insurance_pincode = load_data_from_query(SQL_QUERY_TOP_INSURANCE_PINCODE) # Using the specific query

        if df_top_insurance_pincode.empty:
            st.warning("Could not load data for top insurance pincodes. Please check your database connection and the 'top_insurance_pincode' table.")
        else:
            st.subheader("Top PIN Codes by Insurance Transaction Volume")
            st.dataframe(df_top_insurance_pincode) # Display raw data for now

            

            # Optional: Add code here for an alternative visualization (e.g., a bar chart for top N pincodes)
            if not df_top_insurance_pincode.empty:
                st.subheader("Top 10 PIN Codes by Insurance Transaction Volume (Bar Chart)")
                fig, ax = plt.subplots(figsize=(10, 6))
                top_n_pincodes = df_top_insurance_pincode.head(10) # Get top 10
                ax.bar(top_n_pincodes['pincode'].astype(str), top_n_pincodes['total_insurance_volume'])
                ax.set_title("Top 10 PIN Codes by Insurance Transaction Volume")
                ax.set_xlabel("PIN Code")
                ax.set_ylabel("Total Insurance Transaction Volume")
                plt.xticks(rotation=45, ha='right')
                plt.tight_layout()
                st.pyplot(fig)


            # Optional: Display raw data checkbox
            if st.checkbox("Show Raw Data (Top Insurance Pincodes)"):
                st.subheader("Raw Data (Top Insurance Pincodes)")
                st.dataframe(df_top_insurance_pincode)

    elif insurance_analysis_selection == "States recorded the highest number of insurance transactions":
        st.subheader("States with Highest Insurance Transaction Volume")

        st.markdown("""
        This section visualizes the states with the highest number of insurance transactions.
        """)

        # Load the data using the specific SQL query from the Canvas
        # Note: This query requires year and quarter to be selected.
        # We need to add year and quarter selection for this analysis.
        st.sidebar.subheader("Filter Insurance Data")
        years, quarters, states = get_dropdown_options() # Reuse existing function

        if not years or not quarters:
             st.warning("Could not load years or quarters from the database. Please check your connection and data.")
        else:
            selected_year_insurance = st.sidebar.selectbox("Select Year", years, key='insurance_year')
            selected_quarter_insurance = st.sidebar.selectbox("Select Quarter", quarters, key='insurance_quarter')

            if st.button(f"Analyze for {selected_year_insurance} Q{selected_quarter_insurance}", key='analyze_insurance_states'):
                 st.write(f"Analyzing states with highest insurance transactions for {selected_year_insurance} Quarter {selected_quarter_insurance}...")

                 # Format the SQL query with selected year and quarter
                 sql_query_formatted = SQL_QUERY_TOP_INSURANCE_STATES_BY_YEAR_QUARTER.format(
                     selected_year=selected_year_insurance,
                     selected_quarter=selected_quarter_insurance
                 )

                 df_top_insurance_states = load_data_from_query(sql_query_formatted) # Load data

                 if df_top_insurance_states.empty:
                     st.info(f"No data available for states with insurance transactions for {selected_year_insurance} Q{selected_quarter_insurance}. Please check your database.")
                 else:
                     # --- Create Matplotlib Bar Chart ---
                     st.subheader(f"Top States by Insurance Transactions ({selected_year_insurance} Q{selected_quarter_insurance})")
                     fig, ax = plt.subplots(figsize=(10, 6)) # Create a figure and axes

                     # Create the bar chart
                     ax.bar(df_top_insurance_states['state'], df_top_insurance_states['total_insurance_transactions'])

                     ax.set_title("States by Total Insurance Transactions") # Set title
                     ax.set_xlabel("State") # Set x-axis label
                     ax.set_ylabel("Total Insurance Transactions(lakhs)") # Set y-axis label
                     plt.xticks(rotation=45, ha='right') # Rotate x-axis labels for readability
                     plt.tight_layout() # Adjust layout
                     st.pyplot(fig) # Display the Matplotlib figure in Streamlit

                     # Optional: Display raw data
                     if st.checkbox("Show Raw Data (Top Insurance States)"):
                         st.subheader("Raw Data (Top Insurance States)")
                         st.dataframe(df_top_insurance_states)

    elif insurance_analysis_selection == "States where insurance transactions declined": # New option name
        st.subheader("Total Yearly Insurance Transaction Count by State")

        st.markdown("""
        This section visualizes the total number of insurance transactions per state over the years.
        """)

        # Load the data using the simple yearly query
        df_yearly_insurance_total = load_data_from_query(SQL_QUERY_YEARLY_INSURANCE_COUNT_BY_STATE)

        # --- Temporary Debugging Line ---
        st.write("Debugging: Data loaded for Yearly Insurance Totals:")
        st.dataframe(df_yearly_insurance_total)
        # --- End Debugging Line ---


        if df_yearly_insurance_total.empty:
            st.warning("Could not load yearly insurance data by state. Please check your database connection and the 'aggregated_insurance' table.")
        else:
            # --- Create Matplotlib Line Chart for Yearly Totals ---
            st.subheader("Total Yearly Insurance Transaction Count Over Time by State")

            fig, ax = plt.subplots(figsize=(12, 7)) # Create a figure and axes

            # Plot a line for each state
            for state in df_yearly_insurance_total['state'].unique():
                state_data = df_yearly_insurance_total[df_yearly_insurance_total['state'] == state].sort_values(by='year')
                # Check if 'year' and 'total_year_volume' columns exist before plotting
                if 'year' in state_data.columns and 'total_year_volume' in state_data.columns:
                     ax.plot(state_data['year'], state_data['total_year_volume'], marker='o', linestyle='-', label=state)
                else:
                     st.warning(f"Could not find 'year' or 'total_year_volume' column in data for state {state}. Cannot plot trend.")
                     # No need to break here, just skip plotting for this state if columns are missing
                     continue

            # --- Create Heatmap Visualization using Matplotlib and Seaborn ---
            st.subheader("Total Yearly Insurance Transaction Count Heatmap by State and Year")

# Data Preparation for Heatmap: Pivot the data
# Ensure 'year' is treated as a category or string for pivoting if needed,
# but for heatmap index/columns, numerical or object types work.
# Using 'year' as columns and 'state' as index.
            heatmap_data = df_yearly_insurance_total.pivot_table(
                index='state',
                columns='year',
                values='total_year_volume',
                fill_value=0 # Fill years/states with no data with 0
            )

# Create a figure and axes for the heatmap
            fig, ax = plt.subplots(figsize=(12, 8)) # Adjust figsize as needed

# Create the heatmap using Seaborn
            sns.heatmap(
                heatmap_data,
                annot=True, # Annotate cells with the count values
                fmt=".0f", # Format annotations as integers
                cmap="viridis", # Use a color map (viridis, plasma, etc.)
                ax=ax # Draw the heatmap on the created axes
            )

            ax.set_title("Total Yearly Insurance Transaction Count by State and Year") # Set title
            ax.set_xlabel("Year") # Set x-axis label
            ax.set_ylabel("State") # Set y-axis label
            plt.xticks(rotation=45, ha='right') # Rotate x-axis labels for readability
            plt.yticks(rotation=0) # Ensure y-axis labels are horizontal
            plt.tight_layout() # Adjust layout to prevent labels overlapping

# Display the Matplotlib figure in Streamlit
            st.pyplot(fig)

            st.info("""
            This heatmap visualizes the total insurance transaction count for each state across different years.
            Darker colors indicate a higher transaction count.
            """)
# --- New Case Study: Transaction Analysis for Market Expansion ---
elif page_selection == "Transaction Analysis for Market Expansion":
    st.header("Transaction Analysis for Market Expansion")

    st.markdown("""
    This section analyzes transaction data to identify opportunities for market expansion.
    """)

    # --- Sub-navigation for Market Expansion Analysis ---
    market_expansion_analysis_options = ["Top-performing states","Transaction to Low user Ratio","Transaction to High user Ratio"] # New option
    market_expansion_analysis_selection = st.selectbox(
        "Select Analysis Type",
        market_expansion_analysis_options
    )

    # --- Content based on Market Expansion Analysis Sub-navigation ---
    if market_expansion_analysis_selection == "Top-performing states":
        st.subheader("Top 10 Performing States by Quarterly Transaction Volume")

        st.markdown("""
        This section shows the top 10 states with the highest total transaction volume in the latest quarter available in the data.
        """)

        # Load the data using the specific query
        df_top_states_quarterly = load_data_from_query(SQL_QUERY_TOP_10_STATES_QUARTERLY_VOLUME)

        if df_top_states_quarterly.empty:
            st.warning("Could not load data for top-performing states by quarterly volume. Please check your database connection and the 'aggregated_transaction' table.")
        else:
            # --- Data Visualization: Bar Chart for Top States ---
            st.subheader("Top States by Quarterly Transaction Volume")

            # Create a combined period string for better labeling if needed,
            # but for top 10 in the latest quarter, just showing state and volume is sufficient.
            # If you want to show the specific quarter, you might need to adjust the query
            # to get the latest quarter first, then the top 10 states for that quarter.
            # The current query gets the top 10 overall across all quarters.
            # Let's assume for this visualization we want the top 10 overall quarterly volumes.

            fig_top_states = px.bar(
                df_top_states_quarterly,
                x='state',
                y='quarterly_transaction_volume',
                title='Top 10 States by Quarterly Transaction Volume',
                labels={'state': 'State', 'quarterly_transaction_volume': 'Total Quarterly Transaction Volume'},
                color='state' # Color bars by state
            )
            fig_top_states.update_layout(xaxis_tickangle=-45) # Angle x-axis labels for readability
            st.plotly_chart(fig_top_states, use_container_width=True)

            st.info("""
            This bar chart shows the states with the highest quarterly transaction volumes based on the available data.
            Note: This query ranks the top 10 quarterly volumes across all quarters, not necessarily the top 10 states in the *latest* quarter.
            """)


            # Optional: Display raw data
            if st.checkbox("Show Raw Data (Top Quarterly States)"):
                st.subheader("Raw Data (Top Quarterly States)")
                st.dataframe(df_top_states_quarterly)

    if market_expansion_analysis_selection == "Transaction to Low user Ratio":
        st.subheader("Least 5 States with low user ratio")

        st.markdown("""
        This section shows the Least 5 States with low user ratio.
        """)

        # Load the data using the specific query
        df_user_ratio = load_data_from_query(SQL_QUERY_LOW_USER_RATIO)

        if df_user_ratio.empty:
            st.warning("Could not load data for top-performing states by quarterly volume. Please check your database connection and the 'aggregated_transaction' table.")
        else:
            # --- Data Visualization: Bar Chart for Top States ---
            st.subheader("States with Low user ratio")
            fig, ax = plt.subplots(figsize=(10, 7)) # Create a figure and an axes.

            scatter = ax.scatter(
                df_user_ratio['state'], # X-axis: Total Registered Users
                df_user_ratio['transaction_to_user_ratio'], # Y-axis: Transaction-to-User Ratio
                alpha=0.8, # Transparency of points
                #s=df_user_ratio['total_registered_users']/500 # Size of points based on user count (adjust scaling as needed)
            )


            ax.set_title('Total Registered Users vs. Transaction-to-User Ratio by State')
            ax.set_xlabel('state')
            ax.set_ylabel('Transaction-to-User Ratio')
            plt.tight_layout()

            st.pyplot(fig)

            st.write("""
            **Interpreting the plot:**
            - States in the lower-right area (high on the X-axis, low on the Y-axis)
              have a large number of registered users but a relatively low transaction ratio.
              These are potential candidates for marketing efforts aimed at increasing transaction activity.
            """)
            if st.checkbox("Show Raw Data "):
                st.subheader("Raw Data")
                st.dataframe(df_user_ratio)

    if market_expansion_analysis_selection == "Transaction to High user Ratio":
        st.subheader("Top 5 States with high user ratio")

        st.markdown("""
        This section shows the Top 5 States with high user ratio.
        """)

        # Load the data using the specific query
        df_high_user_ratio = load_data_from_query(SQL_QUERY_HIGH_USER_RATIO)

        if df_high_user_ratio.empty:
            st.warning("Could not load data for top-performing states by quarterly volume. Please check your database connection and the 'aggregated_transaction' table.")
        else:
            # --- Data Visualization: Bar Chart for Top States ---
            st.subheader("States with High user ratio")
            fig, ax = plt.subplots(figsize=(10, 7)) # Create a figure and an axes.

            scatter = ax.scatter(
                df_high_user_ratio['state'], # X-axis: Total Registered Users
                df_high_user_ratio['transaction_to_user_ratio'], # Y-axis: Transaction-to-User Ratio
                alpha=0.8, # Transparency of points
                #s=df_high_user_ratio['total_registered_users']/500 # Size of points based on user count (adjust scaling as needed)
            )


            ax.set_title('Total Registered Users vs. Transaction-to-User Ratio by State')
            ax.set_xlabel('state')
            ax.set_ylabel('Transaction-to-User Ratio')
            plt.tight_layout()

            st.pyplot(fig)

            st.write("""
            **Interpreting the plot:**
            - States in the lower-right area (high on the X-axis, low on the Y-axis)
              have a large number of registered users but a relatively low transaction ratio.
              These are potential candidates for marketing efforts aimed at increasing transaction activity.
            """)
            if st.checkbox("Show Raw Data "):
                st.subheader("Raw Data")
                st.dataframe(df_high_user_ratio)


