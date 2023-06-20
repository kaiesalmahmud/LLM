import psycopg2

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
# query = """SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'monthWise';
# """

query = """SELECT COALESCE(
  NULLIF(MIN(NULLIF("Count Device Offline time (hours)", INTERVAL '0 hour')), INTERVAL '0 hour'),
  MIN(NULLIF("Count Device Offline time (hours)", INTERVAL '0 hour'))
) AS "Minimum Downtime"
FROM public."monthWise"
WHERE "Date" >= DATE_TRUNC('month', CURRENT_DATE);"""

# query = """SELECT SUM(EXTRACT(EPOCH FROM "Count Device Offline time (hours)")/3600) AS "Total Downtime (Hours)"
# FROM public."monthWise"
# WHERE "Date" >= DATE_TRUNC('month', CURRENT_DATE)
# AND "Count Device Offline time (hours)" IS NOT NULL;"""

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