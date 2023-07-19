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

# host="localhost"
# port="5432"
# database="ReportDB"
# user="postgres"
# password="postgres"

host="ep-wispy-forest-393400.ap-southeast-1.aws.neon.tech"
port="5432"
database="neondb"
user="db_user"
password="UdH3VeKu7Cik"

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
st.sidebar.header("Choose Database")

# Create the main panel
st.title("DB Connect :cyclone:")
st.subheader("Connect your database and ask questions!!")

connection = None

side_button_01 = st.sidebar.button("ReportDB")
side_button_02 = st.sidebar.button("AccountsDB")

if side_button_01:

    host="localhost"
    port="5432"
    database="ReportDB"
    user="postgres"
    password="postgres"

elif side_button_02:

    host="ep-wispy-forest-393400.ap-southeast-1.aws.neon.tech"
    port="5432"
    database="neondb"
    user="db_user"
    password="UdH3VeKu7Cik"


submit_button = st.sidebar.checkbox("Connect")

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
question = st.text_input(":blue[Ask a question:]", placeholder="Enter your question")

# Create a submit button for executing the query
query_button = st.button("Submit")

# Execute the query when the submit button is clicked
if query_button:
    if connection:

        # Display the results as a dataframe
        # Execute the query and get the results as a dataframe
        try:
            with st.spinner('Calculating...'):
                sql_query, message, answer = get_response(question)

            st.subheader("Answer :robot_face:")
            st.write(answer)
            # results_df = sqlout(connection, sql_query)
            st.info(":coffee: _Did that answer your question? If not, try to be more specific._")
        except:
            st.warning(":wave: Please enter a valid question. Try to be as specific as possible.")

    else:
        st.warning(":wave: Please connect to the database first.")
