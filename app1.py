
import streamlit as st
import time
import pandas as pd
from dotenv import load_dotenv
import os
import psycopg2
import bcrypt
from google import genai
import re

# --- Configuration and Initialization --- #

# Page configuration for a wider, cleaner layout
st.set_page_config(
    layout="wide"
)


# Initialize session state for history and current input
if 'history' not in st.session_state:
    st.session_state.history = []
if 'user_input_key' not in st.session_state:
    st.session_state.user_input_key = ""
# Initialize placeholders for execution results and data
if 'execution_message' not in st.session_state:
    st.session_state.execution_message = None
if 'execution_error' not in st.session_state:
    st.session_state.execution_error = None
if 'query_results_df' not in st.session_state:
    st.session_state.query_results_df = None
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False


# -- Login Screen -- #

HASHED_PASSWORD = st.secrets["HASHED_PASSWORD"]

def login_screen():
    """Display login screen and authenticate user."""
    st.title("🔒 Secure Login")
    st.markdown("---")
    st.write("Enter your password to access the AI SQL Query Assistant.")

    password = st.text_input("Password", type="password", key="login_password")

    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        login_btn = st.button("🔑 Login", type="primary", use_container_width=True)

    if login_btn:
        if password:
            try:
                if bcrypt.checkpw(password.encode('utf-8'), HASHED_PASSWORD.encode('utf-8')):
                    st.session_state.logged_in = True
                    st.success("✅ Authentication successful! Redirecting...")
                    st.rerun()
                else:
                    st.error("❌ Incorrect password")
            except Exception as e:
                st.error(f"❌ Authentication error: {e}")
        else:
            st.warning("⚠️ Please enter a password")

    st.markdown("---")
    st.info("""
**Security Notice:**
**Passwords are protected using bcrypt hashing**
""")
    
def require_login():
    if not st.session_state.logged_in:
        login_screen()
        st.stop()

# -- Database Schema for LLM refrence -- #
DATABASE_SCHEMA = """
    Database Schema:

    Lookup tables - 
    Region(
        RegionID SERIAL not null primary key,
        Region TEXT not null
    )
    Country(
        CountryID SERIAL not null Primary key,
        Country Text not null,
        RegionID integer not null,
        FOREIGN KEY(RegionID) REFERENCES Region(RegionID)
    )
    ProductCategory(
        ProductCategoryID SERIAL not null Primary Key,
        ProductCategory Text not null,
        ProductCategoryDescription Text not null,
        UNIQUE(ProductCategory)
    )
    Product(
        ProductID SERIAL not null Primary key,
        ProductName Text not null,
        ProductUnitPrice Real not null,
        ProductCategoryID integer not null,
        UNIQUE (ProductName),
        FOREIGN KEY(ProductCategoryID) REFERENCES ProductCategory(ProductCategoryID)
    )

    Core tables -

    Customer(
        CustomerID SERIAL not null Primary Key,
        FirstName Text not null,
        LastName Text not null,
        Address Text not null,
        City Text not null,
        CountryID integer not null,
        UNIQUE (FirstName, LastName, Address),
        FOREIGN KEY(CountryID) REFERENCES Country(CountryID)
    )
    OrderDetail(
        OrderID SERIAL not null Primary Key,
        CustomerID integer not null,
        ProductID integer not null,
        OrderDate TIMESTAMP not null,
        QuantityOrdered integer not null,
        UNIQUE (CustomerID, ProductID),
        FOREIGN KEY(CustomerID) REFERENCES Customer(CustomerID),
        FOREIGN KEY(ProductID) REFERENCES Product(ProductID)
    )

    Important Notes - 
    Use Joins to get descriptive values from lookup tables
    OrderDate is a TIMESTAMP type
    Always use proper joins for foreign key relationships
"""

# --- Database connection --- #
load_dotenv()

def generate_url():
    DATABASE_USERNAME = st.secrets["DATABASE_USERNAME"]
    DATABASE_PASSWORD = st.secrets["DATABASE_PASSWORD"] 
    DATABASE_SERVER = st.secrets["DATABASE_SERVER"] 
    DATABASE_NAME = st.secrets["DATABASE_NAME"] 

    return f"postgresql://{DATABASE_USERNAME}:{DATABASE_PASSWORD}@{DATABASE_SERVER}/{DATABASE_NAME}"

DATABASE_URL = generate_url()

def db_connection(DB_URL):
    try:
        conn = psycopg2.connect(DB_URL)
        return conn
    except Error as e:
        print(e)
        return None
    

def execute_sql(sql):
    conn = db_connection(DATABASE_URL)
    with conn:
        df = pd.read_sql_query(sql,conn)
        return df


# --- LLM connection --- # 

def generate_sql_query_llm(prompt):

    MY_API_KEY = st.secrets["GEMINI_KEY"]
    client = genai.Client(api_key=MY_API_KEY)
    response = client.models.generate_content(
        model="gemini-2.0-flash-lite",
        # model="gemini-2.5-flash",
        contents=f"{prompt}",
    )
    return response.text


def sql_extraction(text_block):
    pattern = r"```sql\n([\s\S]*?)\n```"

    match = re.search(pattern, text_block)

    if match:
        sql_query = match.group(1).strip()
        return sql_query
    else:
        print("SQL block not found in the text.")


# --- Button Handlers ---

def handle_generate_sql():
    """Handles click of 'Generate SQL' button."""
    prompt = st.session_state.user_input_key
    
    # Reset execution messages and results when a new generation starts
    st.session_state.execution_message = None
    st.session_state.execution_error = None
    st.session_state.query_results_df = None

    prompt_formatted = f"""You are a PostgreSQL expert. Given the following database schema and a user's question, generate a valid PostgreSQL query.

    {DATABASE_SCHEMA}

    User Question: {prompt}

    Requirements:
    1. Generate ONLY the SQL query, wrapped in ```sql``` code blocks
    2. Use proper JOINs to get descriptive names from lookup tables
    3. Use appropriate aggregations (COUNT, AVG, SUM, etc.) when needed
    4. Add LIMIT clauses for queries that might return many rows (default LIMIT 100)
    5. Use proper date/time functions for TIMESTAMP columns
    6. Make sure the query is syntactically correct for PostgreSQL
    7. Add helpful column aliases using AS

    Generate the SQL query: """

    if prompt:
        with st.spinner('Generating SQL query...'):
            result = generate_sql_query_llm(prompt_formatted)
        
        result_formatted = sql_extraction(result)
        
        st.session_state.history.append({
            "prompt" : st.session_state.user_input_key,
            "sql" : result_formatted
        })
        
        st.session_state.user_input_key = ""

def handle_clear_history():
    st.session_state.history = []
    st.session_state.user_input_key = ""
    st.session_state.execution_message = None
    st.session_state.execution_error = None
    st.session_state.query_results_df = None

def load_example(example_text):
    """Loads an example question into the input area."""
    st.session_state.user_input_key = example_text


def handle_run_query(latest_index):
    """Run the query against the database and displays results."""
    
    sql_to_execute = st.session_state.get(f'editable_sql_{latest_index}')
    
    # Clear previous execution results/messages
    st.session_state.execution_message = None
    st.session_state.execution_error = None
    st.session_state.query_results_df = None
    
    if not sql_to_execute:
        st.session_state.execution_error = "Cannot run an empty query."
        return

    with st.spinner('Executing query against database...'):
        time.sleep(2) # Simulate execution time
        
        try:
            results_df = execute_sql(sql_to_execute)
            
            st.session_state.query_results_df = results_df
            
            st.session_state.execution_message = (
                f"✅ Query returned {len(results_df)} rows successfully at {time.strftime('%H:%M:%S')}!"
            )
            
        except Exception as e:
            st.session_state.execution_error = f"❌ Database Error: Could not execute query. {e}"


# --- Sidebar Content ---

def render_sidebar():
    """Renders the sidebar"""
    st.sidebar.markdown(
        """
        <div>
            <h3>💡 Example Questions</h3>
            <p>
                Try asking questions like:
            </p>
        </div>
        """,
        unsafe_allow_html=True
    )
    
    st.sidebar.markdown("---")
    
    st.sidebar.markdown("**Demographics:**")
    st.sidebar.button(
        "Find the total value by region. Total is defined as multiplication of product price and ordered quantity.", 
        on_click=load_example, 
        args=["Find the total value by region. Total is defined as multiplication of product price and ordered quantity."], 
        key="ex_region"
    )
    st.sidebar.button(
        "List of all the countries with total order value greater than 100000 dollars.", 
        on_click=load_example, 
        args=["List of all the countries with total order value greater than 100000 dollars."], 
        key="ex_country"
    )

    st.sidebar.markdown("**Statistics**")
    st.sidebar.button(
        "What is the average order value?", 
        on_click=load_example, 
        args=["What is the average order value?"], 
        key="ex_average_order"
    )
    st.sidebar.button(
        "List the top 5 most ordered productnames.", 
        on_click=load_example, 
        args=["List the top 5 most ordered productnames."], 
        key="ex_common_product"
    )

    # Add "How it works" section
    st.sidebar.markdown("---")
    st.sidebar.markdown(
        """
        <div>
            <h3 style="color: #4CAF50;">⚙️ How it works</h3>
            <ol >
                <li>Enter your question in plain English</li>
                <li>AI generates SQL query</li>
                <li>Review and click 'Run Query' to execute against your connected database</li>
            </ol>
        </div>
        """,
        unsafe_allow_html=True
    )

    st.sidebar.markdown("---")

    if st.sidebar.button('Logout'):
        st.session_state.logged_in = False
        st.rerun()


# --- Main Application Logic ---

def main():

    require_login()
    
    # 1. Render the sidebar
    render_sidebar()
    
    # 2. Main Title
    st.markdown(
        """
        <div style="text-align: center;">
            <h1 style="color: #FF6A4D;">🤖 AI-Powered SQL Query Assistant</h1>
            <p style="font-size: 1.1em; color: #4b5563;">
                Ask questions in natural language, and I will generate SQL queries for you to review and run!
            </p>
        </div>
        """,
        unsafe_allow_html=True
    )
    
    # 3. User Input Area
    st.subheader("What would you like to know?")
    st.text_area(
        label="Enter your question here:",
        value=st.session_state.user_input_key if 'user_input_key' in st.session_state else "",
        height=100,
        key="user_input_key", # Key for session state management
        label_visibility="collapsed",
        placeholder="e.g., what is the average order value?"
    )

    # 4. Action Buttons
    col1, col2, _ = st.columns([1, 1, 5])
    
    with col1:
        st.button(
            "Generate SQL", 
            on_click=handle_generate_sql, 
            type="primary",
            # use_container_width=True
        )
        
    with col2:
        st.button(
            "Clear History", 
            on_click=handle_clear_history,
            # use_container_width=True
        )

    st.markdown("---")

    # 5. History / Output Display
    if st.session_state.history:
        # Get the latest item index
        latest_index = len(st.session_state.history) - 1
        latest_item = st.session_state.history[latest_index]
        
        st.subheader("Generated SQL Query")

        # --- Display Latest Query ---
        
        # Prompt display
        st.markdown(
            f"""
            <div style="background-color: #36454F; padding: 10px; border-radius: 6px;">
                <p>Question: {latest_item["prompt"]}</p>
            </div>
            """, 
            unsafe_allow_html=True
        )

        st.markdown("Review and edit the SQL query if needed:")
        
        # Editable SQL Area for the latest item
        sql_key = f"editable_sql_{latest_index}"
        
        if sql_key not in st.session_state or st.session_state[sql_key] == "": 
            st.session_state[sql_key] = latest_item["sql"]
            
        st.text_area(
            label="SQL Query", 
            value=st.session_state[sql_key], 
            height=150, 
            key=sql_key,
            label_visibility="collapsed"
        )
        
        # Run Query Button
        st.button(
            "Run Query", 
            on_click=handle_run_query, 
            args=[latest_index], 
            key=f"run_btn_{latest_index}", 
            type="primary"
        )
        
        # 6. Query Results Section
        if st.session_state.get('execution_message') or st.session_state.get('execution_error'):
            
            st.markdown("---")
            st.subheader("📊 Query Results")
            
            results_container = st.container() 
            
            if st.session_state.get('execution_message'):
                with results_container:
                    
                    # Display success message
                    st.success(st.session_state.execution_message)
                    
                    # Display the simulated DataFrame result
                    if st.session_state.query_results_df is not None:
                        st.dataframe(st.session_state.query_results_df, use_container_width=True)
                
            if st.session_state.get('execution_error'):
                with results_container:
                    # Display error message
                    st.error(st.session_state.execution_error)

        st.markdown("---")

        # --- Display Older History Items ---
        if len(st.session_state.history) > 1:
            st.subheader("Previous Queries")
            for i in range(len(st.session_state.history) - 2, -1, -1):
                item = st.session_state.history[i]
                
                with st.expander(f"**Previous Query:** {item['prompt']}", expanded=False):
                    st.caption("Generated SQL Query:")
                    st.code(item['sql'], language='sql')
    
    # Optional: Display a help message if history is empty
    else:
        st.info("Ask your first natural language question above to generate an SQL query!")

# Run the app
if __name__ == "__main__":
    main()