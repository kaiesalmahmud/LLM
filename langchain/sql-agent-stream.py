import os
from langchain.agents import create_sql_agent
from langchain.agents.agent_toolkits import SQLDatabaseToolkit
from langchain.sql_database import SQLDatabase
from langchain.llms.openai import OpenAI
from langchain.agents import AgentExecutor
from langchain.agents.agent_types import AgentType
from langchain.chat_models import ChatOpenAI

import psycopg2
import pandas as pd
import streamlit as st

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

llm = ChatOpenAI(model_name="gpt-4", temperature=0)

toolkit = SQLDatabaseToolkit(db=db, llm=llm)

# Implement: If null value is found at the top while trying to sort in descending order, try to look for the next non-null value.

SQL_PREFIX = """You are an agent designed to interact with a SQL database.
Given an input question, create a syntactically correct {dialect} query to run, then look at the results of the query and return the answer.
Unless the user specifies a specific number of examples they wish to obtain, always limit your query to at most {top_k} results.
You can order the results by a relevant column to return the most interesting examples in the database.
Never query for all the columns from a specific table, only ask for the relevant columns given the question.
You have access to tools for interacting with the database.
Only use the below tools. Only use the information returned by the below tools to construct your final answer.
You MUST double check your query before executing it. If you get an error while executing a query, rewrite the query and try again.

DO NOT make any DML statements (INSERT, UPDATE, DELETE, DROP etc.) to the database.

If the question does not seem related to the database, just return "I don't know" as the answer.

SQL query format example:
Question: "Who are the top 5 retailers for the month of May?"
Query: SELECT "Retail Name", 
       SUM("Total Play time Minutes") as TotalTime 
       FROM "monthWise" 
       WHERE EXTRACT(MONTH FROM "Date") = 5 
       GROUP BY "Retail Name" 
       ORDER BY TotalTime DESC 
       LIMIT 5

Question: What is the name of the retail with the highest average efficiency?
Query: SELECT "Retail Name", AVG("Efficiency %") as AverageEfficiency 
       FROM "monthWise" 
       GROUP BY "Retail Name" 
       ORDER BY AverageEfficiency DESC 
       LIMIT 1
"""

SQL_FUNCTIONS_SUFFIX = """I should look at the tables in the database to see what I can query.  Then I should query the schema of the most relevant tables."""

FORMAT_INSTRUCTIONS = """Use the following format:

Question: the input question you must answer
Thought: you should always think about what to do
Action: the action to take, should be one of [{tool_names}]
Action Input: the input to the action
Observation: the result of the action
... (this Thought/Action/Action Input/Observation can repeat N times)
Thought: I now know the final answer
Final Answer: the final answer to the original input question"""

# Define function to connect to the database using input parameters
def connect_to_database(host, port, username, password, database):
    connection = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=username,
        password=password
    )
    return connection

def get_response(input_text):
    response = agent_executor(input_text)
    sql_query = response['intermediate_steps'][-1][0].tool_input
    message = response['intermediate_steps'][-1][0].message_log[0].content
    answer = response['output']
    return sql_query, message, answer

def sqlout(connection,query):

    cursor = connection.cursor()
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
    # st.text(query)
    st.dataframe(df,width=None)

agent_executor = create_sql_agent(
    llm=llm,
    toolkit=toolkit,
    verbose=True,
    agent_type=AgentType.OPENAI_FUNCTIONS,
    prefix=SQL_PREFIX,
    suffix=SQL_FUNCTIONS_SUFFIX,
    format_instructions=FORMAT_INSTRUCTIONS,
    agent_executor_kwargs = {'return_intermediate_steps': True}
)

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

connection = None

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
question = st.text_input("Enter your question", placeholder="eg. Describe the databese.")

# Create a submit button for executing the query
query_button = st.button("Submit")

# Execute the query when the submit button is clicked
if query_button:
    if connection:

        # Display the results as text outputs
        #st.subheader("Text Outputs")
        # st.text("Refined Question")
        # st.text("Tables Used")
        # st.text("Calculating...")

        # Display the results as a dataframe
        # Execute the query and get the results as a dataframe
        try:
            with st.spinner('Calculating...'):
                sql_query, message, answer = get_response(question)
            # st.subheader("SQL Query")
            # st.text(sql_query)
            # st.text("Message")
            # st.text(message)
            st.subheader("Answer  :robot_face:")
            st.write(answer)
            # results_df = sqlout(connection, sql_query)
            st.info(":coffee:    Did that answer your question? If not, try to be more specific.")
        except:
            st.write("Please enter a valid question. Try to be as specific as possible.")

    else:
        st.text("Please connect to the database first.")
