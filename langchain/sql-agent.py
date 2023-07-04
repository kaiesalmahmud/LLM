from langchain.agents import create_sql_agent
from langchain.agents.agent_toolkits import SQLDatabaseToolkit
from langchain.sql_database import SQLDatabase
from langchain.chat_models import ChatOpenAI
import os

# import psycopg2

os.environ["OPENAI_API_KEY"] = open('key.txt', 'r').read().strip()

from dotenv import load_dotenv
load_dotenv()

llm = ChatOpenAI(model_name="gpt-3.5-turbo")

host="localhost",
port="5432",
database="ReportDB",
user="postgres",
password="postgres"

db = SQLDatabase.from_uri("postgresql://postgres:postgres@localhost:5432/ReportDB")

toolkit = SQLDatabaseToolkit(db=db, llm=llm)

agent_executor = create_sql_agent(
    llm=llm,
    toolkit=toolkit,
    verbose=True
)

agent_executor.run("Who are the top 5 retailers this month?")

