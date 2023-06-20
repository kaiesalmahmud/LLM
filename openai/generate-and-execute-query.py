import openai
import psycopg2

openai.api_key = open('key.txt', 'r').read().strip()

message_history = [{"role": "user", "content": f"""You are a SQL query generator bot for postgreSQL database.

                    The table schemas are as follows:

                    monthWise('Date', 'Retail ID', 'Retail Name', 'Zone', 'Location', 'Total Play time', 'Total Play time Minutes', 'Efficiency %', 'Start Time', 'End Time', 'Count Device Offline time (hours)', 'Remarks')

                    And the datatypes for the properties are as follows:
                    
                    ('Date', 'date')
                    ('Retail ID', 'text')
                    ('Retail Name', 'text')
                    ('Zone', 'text')
                    ('Location', 'text')
                    ('Total Play time', 'interval')
                    ('Total Play time Minutes', 'interval')
                    ('Efficiency %', 'double precision')
                    ('Start Time', 'timestamp without time zone')
                    ('End Time', 'timestamp without time zone')
                    ('Count Device Offline time (hours)', 'interval')
                    ('Remarks', 'text'))

                    I will ask a quesiton about the data in natural language in my message, and you will reply with a SQL query 
                    that will generate the necessary response when performed on the postgreSQL database.

                    Here are some sample queries:
                    
                    "SELECT * FROM public.\"monthWise\" LIMIT 50;"

                    "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'monthWise';

                    Reply only with the sql query to further input. If you understand, say OK."""},
                   {"role": "assistant", "content": f"OK"}]

def predict(input):
    global message_history

    message_history.append({"role": "user", "content": input})

    completion = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=message_history,
    )

    reply_content = completion.choices[0].message.content
    print("Query:\n"+ reply_content)

    return reply_content

query = input("Ask a question: ")

reply_query = predict(query)

# Establish a connection to the PostgreSQL database
conn = psycopg2.connect(
    host="adiq-router",
    port="15432",
    database="ReportDB",
    user="postgres",
    password="postgres1"
)

# Create a cursor object to interact with the database
cursor = conn.cursor()

# Execute a SELECT query to fetch the first 50 rows from a table
# query = "SELECT * FROM public.\"monthWise\" LIMIT 50"

# Execute a query to fetch the column names and data types from a table
query = reply_query
cursor.execute(query)

# Fetch all the rows returned by the query
rows = cursor.fetchall()

# Print the fetched rows
# for row in rows:
#     print(row)

print(rows)

# Close the cursor and the connection
cursor.close()
conn.close()