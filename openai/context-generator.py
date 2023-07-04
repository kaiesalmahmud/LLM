import psycopg2
from tabulate import tabulate

# Function to execute a SQL query and return the results
def execute_query(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    return result

# Establish a connection to the PostgreSQL database
connection = psycopg2.connect(
    host="localhost",
    port="5432",
    database="ReportDB",
    user="postgres",
    password="postgres"
)


queries =[
"""
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'monthWise';""",

"""
SELECT
    c.table_name,
    c.column_name,
    c.data_type,
    CASE WHEN tc.constraint_type = 'PRIMARY KEY' THEN 'Primary Key'
        WHEN tc.constraint_type = 'FOREIGN KEY' THEN 'Foreign Key'
        ELSE ''
    END AS column_key,
    ccu.table_name AS referenced_table_name,
    ccu.column_name AS referenced_column_name
FROM
    information_schema.columns c
    LEFT JOIN information_schema.key_column_usage kcu
        ON c.table_schema = kcu.table_schema
        AND c.table_name = kcu.table_name
        AND c.column_name = kcu.column_name
    LEFT JOIN information_schema.table_constraints tc
        ON kcu.constraint_schema = tc.constraint_schema
        AND kcu.constraint_name = tc.constraint_name
    LEFT JOIN information_schema.constraint_column_usage ccu
        ON tc.constraint_schema = ccu.constraint_schema
        AND tc.constraint_name = ccu.constraint_name
WHERE
    c.table_schema = 'public'
    AND c.table_catalog = 'ReportDB'
ORDER BY
    c.table_name,
    c.ordinal_position;""",

"""
SELECT schema_name FROM information_schema.schemata;"""]

# Fetch all the rows returned by the query
result = execute_query(connection, queries[1])

print(result)

# headers = ['Table Name', 'Column Name', 'Data Type', 'Column Key', 'Referenced Table Name', 'Referenced Column Name']
# table = tabulate(result, headers, tablefmt="grid")

# print(table)