import os
from langchain.agents import *
from langchain.sql_database import SQLDatabase
from langchain.chat_models import ChatOpenAI
from langchain.chains import SQLDatabaseSequentialChain
from langchain.prompts.prompt import PromptTemplate

import psycopg2
import pandas as pd
import streamlit as st

import warnings
warnings.filterwarnings("ignore")

API_KEY = open('key.txt', 'r').read().strip()
os.environ["OPENAI_API_KEY"] = API_KEY

from dotenv import load_dotenv
load_dotenv()

host="localhost"
port="5432"
database="ReportDB"
user="postgres"
password="postgres"

db = SQLDatabase.from_uri(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")

llm = ChatOpenAI(model_name="gpt-3.5-turbo")


_postgres_prompt = """You are a PostgreSQL expert. Given an input question, first create a syntactically correct PostgreSQL query to run, then look at the results of the query and return the answer to the input question.
Unless the user specifies in the question a specific number of examples to obtain, query for at most {top_k} results using the LIMIT clause as per PostgreSQL. You can order the results to return the most informative data in the database.
Never query for all columns from a table. You must query only the columns that are needed to answer the question. Wrap each column name in double quotes (") to denote them as delimited identifiers.
Pay attention to use only the column names you can see in the tables below. Be careful to not query for columns that do not exist. Also, pay attention to which column is in which table.
Pay attention to use CURRENT_DATE function to get the current date, if the question involves "today".

Use the following format:

Question: Question here
SQLQuery: SQL Query to run
SQLResult: Result of the SQLQuery
Answer: Final answer here

"""

PROMPT_SUFFIX = """Only use the following tables:
{table_info}

Question: {input}"""

POSTGRES_PROMPT = PromptTemplate(
    input_variables=["input", "table_info", "top_k"],
    template=_postgres_prompt + PROMPT_SUFFIX,
)


# Setup the database chain
db_chain = SQLDatabaseSequentialChain.from_llm(llm=llm, database=db, query_prompt=POSTGRES_PROMPT, verbose=False, return_intermediate_steps=True)

def get_query(input_text):
    result = db_chain(input_text)
    query = result['intermediate_steps'][1]
    print(query)
    return query

# Define function to execute a query on the database and return the results as a dataframe
def execute_query(query, connection):
    cursor = connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(results, columns=columns)
    return df

# Define function to connect to the database using input parameters
def connect_to_database(host, port, username, password, database):
    conn = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=username,
        password=password
    )
    return conn

def sqlout(cursor,query):
    
    cursor.execute(query)

    # Given column information
    column_info = list(cursor.description)

    # Extract column names
    column_names = [column.name for column in column_info]

    # Create a DataFrame
    df = pd.DataFrame(columns=range(len(column_names)))

    # Fetch all the rows returned by the query

    rows = cursor.fetchall()
    df=df.from_records(rows).astype(str)

    # Set column names as column headers
    df.columns = column_names
    st.text(query)
    st.dataframe(df,width=None)

# Create the sidebar for DB connection parameters
st.sidebar.header("Database Connection")
host = st.sidebar.text_input("Host", value="localhost")
port = st.sidebar.text_input("Port", value="5432")
username = st.sidebar.text_input("Username", value="postgres")
password = st.sidebar.text_input("Password", value="postgres")
database = st.sidebar.text_input("Database", value="ReportDB")
submit_button = st.sidebar.checkbox("Connect")

# Create the main panel
st.title("Database Query Application")

# Check if the submit button is clicked and establish the database connection
if submit_button:
    try:
        connection = connect_to_database(host, port, user, password, database)
    except:
        connection=None


    if connection:
        st.sidebar.success("Connected to the database!")
    else:
        st.sidebar.error("Failed to connect to the database. Please check your connection parameters.")

# Get the user's natural question input
question = st.text_input("Enter your question")

# Create a submit button for executing the query
query_button = st.button("Submit")

# Execute the query when the submit button is clicked
if query_button:
    if connection:

        # Display the results as text outputs
        #st.subheader("Text Outputs")
        st.text("Refined Question")
        st.text("Tables Used")
        st.text("SQL Executed")

        # Display the results as a dataframe
        # Execute the query and get the results as a dataframe
        try:
            query = get_query(question)
            results_df = sqlout(connection.cursor(), query)
        except:
            st.error("Failed to execute query")