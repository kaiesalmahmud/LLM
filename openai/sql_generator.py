import openai

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

query = input("Give me a query: ")

predict(query)