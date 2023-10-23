import psycopg2
import pandas as pd

conn = psycopg2.connect(
    host="server-alpha.ad-iq.com",
    port="32974",
    dbname="adiq",
    user="mlteam",
    # user="postgres",
    password="q7a2C6MKV570B1MNZSQpyO3EifwdtkmV"
    # password="tJpvTb51h4CWVtbU3wiF2O2LV5bKYu8s"
)

# Create a cursor object to interact with the database
cursor = conn.cursor()

# cursor.execute("""
#     SET TIMEZONE TO 'Asia/Dhaka';
# """);

start_date = "'2023-10-15'"
end_date = start_date

# from datetime import date, timedelta

# yesterday = str(date.today() - timedelta(days=1))
# yesterday = "'"+yesterday+"'"

# start_date = "'2023-07-01'"
# end_date = yesterday

base_query = f"""

SET TIMEZONE TO 'Asia/Dhaka';

WITH "on_beats" as (
    SELECT
        *
    FROM
        "campaign_events_aggregated_heartbeat_view"
    WHERE
        "day_start" >= {start_date}::date
        AND "day_start" < {end_date}::date + INTERVAL '1 day'
), "play_time" as (
    SELECT
        "bundle_number",
        "day_start",
        EXTRACT(epoch FROM uptime("on_beat")) as "total_play_time",
        EXTRACT(epoch FROM downtime("on_beat")) as "total_non_play_time"
    FROM "on_beats"
), "online_beats" as (
    SELECT
        *
    FROM
        "bundle_online_heartbeat_aggregated_view"
    WHERE
        "day_start" >= {start_date} :: date
        AND "day_start" < {end_date} :: date + INTERVAL '1 day'
), "online_time" as (
    SELECT
        "bundle_number",
        "day_start",
        EXTRACT(epoch FROM uptime("online_beat")) as "total_online_time",
        EXTRACT(epoch FROM downtime("online_beat")) as "total_offline_time"
    FROM "online_beats"
), "raw_data" as (
    SELECT
        "on_beats"."bundle_number",
        "on_beats"."day_start" as "day_start",
        "on_beats"."opening" as "start_time",
        "on_beats"."closing" as "end_time",
        "play_time"."total_play_time" as "total_play_time",
        GREATEST(
            "play_time"."total_play_time" - "online_time"."total_online_time",
            0
        ) as "total_offline_time",
        GREATEST(
            "play_time"."total_non_play_time" - EXTRACT(epoch FROM (INTERVAL '1 day' - ("on_beats"."closing" - "on_beats"."opening")))::integer,
            0
        ) as "off_time",
        EXTRACT(epoch FROM INTERVAL '10 hours')::integer as "expected_play_time",
        "play_time"."total_play_time" / EXTRACT(epoch FROM INTERVAL '10 hours')::integer as "efficiency"
    FROM "on_beats"
    LEFT OUTER JOIN "play_time"
        ON "on_beats"."bundle_number" = "play_time"."bundle_number"
        AND "on_beats"."day_start" = "play_time"."day_start"
    LEFT OUTER JOIN "online_time"
        ON "on_beats"."bundle_number" = "online_time"."bundle_number"
        AND "on_beats"."day_start" = "online_time"."day_start"
), "dates" as (
    SELECT
        {start_date}::date + (i || ' days')::interval as "day_start"
    FROM generate_series(0, {end_date}::date - {start_date}::date) i
), "bundles" as (
    SELECT
        id as "bundle_id",
        document->>'bundle_number' as "bundle_number"
    FROM raw_bundles
), "dailyLog" as (
    SELECT
        raw_zones.document->>'name' as "zone_name",
        raw_areas.document->>'name' as "area_name",
        raw_points.document->>'code' as "point_code",
        raw_points.document->>'name' as "point_name",
        bundles.bundle_number as "bundle_number",
        "dates"."day_start"::date as "Date",
        "raw_data"."start_time" as "Started",
        end_time as "Ended",
        total_play_time as "total_play_time_seconds",
        ROUND(total_play_time/60) as "total_playtime_minutes",
        TO_CHAR((total_play_time || ' seconds')::interval, 'HH24:MI:SS') as "Total Play Duration",
        efficiency as "Efficiency",
        total_offline_time as "total_offline_time_seconds",
        TO_CHAR((total_offline_time || ' seconds')::interval, 'HH24:MI:SS') as "Offline Play Duration",
        off_time as "off_time_seconds",
        TO_CHAR((off_time || ' seconds')::interval, 'HH24:MI:SS') as "Box Off Duration",
        CASE
            WHEN start_time IS NOT NULL THEN 'OPENED'
            ELSE 'NOT-OPENED'
        END as "Opened"
    FROM bundles
    RIGHT OUTER JOIN dates ON TRUE
    LEFT OUTER JOIN raw_data
        ON bundles.bundle_number = raw_data.bundle_number
        AND "raw_data"."day_start" = "dates"."day_start"
    LEFT OUTER JOIN "raw_points"
        ON "raw_points".document->>'bundle_id' = "bundles"."bundle_id"
    LEFT OUTER JOIN "raw_areas"
        ON "raw_areas".id = "raw_points".document->>'area_id'
    LEFT OUTER JOIN "raw_zones"
        ON "raw_zones".id = "raw_areas".document->>'zone_id'
    ORDER BY
        dates.day_start::date,
        bundles.bundle_number
)

"""

test_query = f"""

    {base_query}

    SELECT *
    FROM "dailyLog"
    LIMIT 1
"""

name_of_zones_query = f"""

    {base_query}

    SELECT DISTINCT "zone_name"
    FROM "dailyLog"
    ORDER BY "zone_name"
"""

total_shops_query = f"""

    {base_query}

    SELECT "zone_name", COUNT(DISTINCT "point_code"), SUM(total_playtime_minutes)
    FROM "dailyLog"
    GROUP BY "zone_name"
"""

satisfied_shops_query = f"""

    {base_query}

    SELECT "zone_name", COUNT(DISTINCT "point_code"), SUM(total_playtime_minutes) 
    FROM "dailyLog" 
    WHERE "Efficiency" >= 1
    GROUP BY "zone_name"
"""

num_not_open_shops_query = f"""

    {base_query}

    SELECT "zone_name", COUNT(DISTINCT "point_code")
    FROM "dailyLog" 
    WHERE "Started" IS NULL
    GROUP BY "zone_name"
"""

name_not_open_shops_query = f"""

    {base_query}

    SELECT "zone_name", "point_code", "point_name"
    FROM "dailyLog" 
    WHERE "Started" IS NULL
"""

under_performing_shops_query = f"""

    {base_query}

    SELECT "zone_name", COUNT(DISTINCT "point_code"), SUM(total_playtime_minutes) 
    FROM "dailyLog" 
    WHERE "Started" IS NOT NULL AND "Efficiency" < 1
    GROUP BY "zone_name"
"""

under_performing_shops_name_query = f"""

    {base_query}

    SELECT "zone_name", "point_code", "point_name"
    FROM "dailyLog" 
    WHERE "Started" IS NOT NULL AND "Efficiency" < 1
"""

shops_before_10_query = f"""

    {base_query}

    SELECT "zone_name", COUNT(DISTINCT "point_code") 
    FROM "dailyLog" 
    WHERE EXTRACT(HOUR FROM "Started") < 10
    GROUP BY "zone_name"
"""

shops_10to11_query = f"""

    {base_query}

    SELECT "zone_name", COUNT(DISTINCT "point_code") 
    FROM "dailyLog" 
    WHERE EXTRACT(HOUR FROM "Started") >= 10 AND EXTRACT(HOUR FROM "Started") < 11
    GROUP BY "zone_name"
"""

shop_names_10to11_query = f"""

    {base_query}

    SELECT "zone_name", "point_code", "point_name" 
    FROM "dailyLog" 
    WHERE EXTRACT(HOUR FROM "Started") >= 10 AND EXTRACT(HOUR FROM "Started") < 11
"""

shops_after_11_query = f"""

    {base_query}

    SELECT "zone_name", COUNT(DISTINCT "point_code") 
    FROM "dailyLog" 
    WHERE EXTRACT(HOUR FROM "Started") >= 11
    GROUP BY "zone_name"
"""

shop_names_after_11_query = f"""

    {base_query}

    SELECT "zone_name", "point_code", "point_name" 
    FROM "dailyLog" 
    WHERE EXTRACT(HOUR FROM "Started") >= 11
"""

data_dict = {}

#######################################
cursor.execute(name_of_zones_query)

# Fetch all the rows returned by the query
rows = cursor.fetchall()

for r in rows:
    data_dict[r[0]] = {'Name of Not Opened Shops': '', 'Name of Under Performing Shops': '', 'Name of After 11AM': ''}


#######################################
cursor.execute(total_shops_query)

# Fetch all the rows returned by the query
rows = cursor.fetchall()

for r in rows:
    data_dict[r[0]]['Number of Shops'] = r[1]
    data_dict[r[0]]['Total Play Time (Minutes)'] = int(r[2])

#######################################
cursor.execute(satisfied_shops_query)

# Fetch all the rows returned by the query
rows = cursor.fetchall()

for r in rows:
    data_dict[r[0]]['Number of Satisfied Shops'] = r[1]
    data_dict[r[0]]['Total Play Time (Minutes) for Satisfied'] = int(r[2])

#######################################
cursor.execute(num_not_open_shops_query)

# Fetch all the rows returned by the query
rows = cursor.fetchall()

for r in rows:
    data_dict[r[0]]['Number of Not Opened Shops'] = r[1]

#######################################
cursor.execute(name_not_open_shops_query)

# Fetch all the rows returned by the query
rows = cursor.fetchall()

for zone, code, store in rows:
    # print(zone, code, store)
    data_dict[zone]['Name of Not Opened Shops'] += f'{code} : {store}, \n'

# print(data_dict['C']['Name of Not Opened Shops'])

#######################################
cursor.execute(under_performing_shops_query)

# Fetch all the rows returned by the query
rows = cursor.fetchall()

for r in rows:
    data_dict[r[0]]['Number of Under Performing Shops'] = r[1]
    data_dict[r[0]]['Total Play Time (Minutes) for Under Performing Shops'] = int(r[2])

#######################################
cursor.execute(under_performing_shops_name_query)

# Fetch all the rows returned by the query
rows = cursor.fetchall()

for zone, code, store in rows:
    data_dict[zone]['Name of Under Performing Shops'] += f'{code} : {store}, \n'

#######################################
cursor.execute(shops_before_10_query)

# Fetch all the rows returned by the query
rows = cursor.fetchall()

for r in rows:
    data_dict[r[0]]['Before 10 AM'] = r[1]

#######################################
cursor.execute(shops_10to11_query)

# Fetch all the rows returned by the query
rows = cursor.fetchall()

for r in rows:
    data_dict[r[0]]['10 AM to 11AM'] = r[1]
    # print(r)

# #######################################
# cursor.execute(shop_names_10to11_query)

# # Fetch all the rows returned by the query
# rows = cursor.fetchall()

# for zone, code, store in rows:
#     data_dict[zone]['Name of 10AM to 11AM'] += f'{code} : {store}, \n'

#######################################
cursor.execute(shops_after_11_query)

# Fetch all the rows returned by the query
rows = cursor.fetchall()

for r in rows:
    data_dict[r[0]]['After 11 AM'] = r[1]
    # print(r)

######################################
cursor.execute(shop_names_after_11_query)

# Fetch all the rows returned by the query
rows = cursor.fetchall()

for zone, code, store in rows:
    data_dict[zone]['Name of After 11AM'] += f'{code} : {store}, \n'
    # print(zone, code, store)

######################################
cursor.execute(test_query)

# Fetch all the rows returned by the query
results = cursor.fetchall()

# Fetch column names
column_names = [desc[0] for desc in cursor.description]

# Render the output
print("| " + " | ".join(column_names) + " |")
print("|-" + "-|-".join(["-" * len(name) for name in column_names]) + "-|")
for row in results:
    print("| " + " | ".join(map(str, row)) + " |")

# Remove the trailing comma and space from each dictionary value
for zone in data_dict.keys():
    try:
        data_dict[zone]['Name of Under Performing Shops'] = data_dict[zone]['Name of Under Performing Shops'].rstrip(', ')
        data_dict[zone]['Name of Not Opened Shops'] = data_dict[zone]['Name of Not Opened Shops'].rstrip(', ')
        data_dict[zone]['Name of After 11AM'] = data_dict[zone]['Name of After 11AM'].rstrip(', ')
        data_dict[zone]['Expected Total Play Time'] = data_dict[zone]['Number of Shops'] * 600
        data_dict[zone]['Cumulative Efficiency'] = data_dict[zone]['Total Play Time (Minutes)'] / data_dict[zone]['Expected Total Play Time'] * 100
        data_dict[zone]['Cumulative Efficiency'] = round(data_dict[zone]['Cumulative Efficiency'], 2)
        data_dict[zone]['Expected Total Play Time for Satisfied'] = data_dict[zone]['Number of Satisfied Shops'] * 600
        data_dict[zone]['Cumulative Efficiency for Satisfied'] = data_dict[zone]['Total Play Time (Minutes) for Satisfied'] / data_dict[zone]['Expected Total Play Time for Satisfied'] * 100
        data_dict[zone]['Cumulative Efficiency for Satisfied'] = round(data_dict[zone]['Cumulative Efficiency for Satisfied'], 2)
        data_dict[zone]['Satisfied Shop Percentage'] = data_dict[zone]['Number of Satisfied Shops'] / data_dict[zone]['Number of Shops'] * 100
        data_dict[zone]['Satisfied Shop Percentage'] = round(data_dict[zone]['Satisfied Shop Percentage'], 2)
        data_dict[zone]['Expected Total Play Time for Under Performing'] = data_dict[zone]['Number of Under Performing Shops'] * 600
        data_dict[zone]['Cumulative Efficiency for Under Performing'] = data_dict[zone]['Total Play Time (Minutes) for Under Performing Shops'] / data_dict[zone]['Expected Total Play Time for Under Performing'] * 100
        data_dict[zone]['Cumulative Efficiency for Under Performing'] = round(data_dict[zone]['Cumulative Efficiency for Under Performing'], 2)
        data_dict[zone]['Under Performing Shop Percentage'] = data_dict[zone]['Number of Under Performing Shops'] / data_dict[zone]['Number of Shops'] * 100
        data_dict[zone]['Under Performing Shop Percentage'] = round(data_dict[zone]['Under Performing Shop Percentage'], 2)
        data_dict[zone]['Not Open Shop Percentage'] = data_dict[zone]['Number of Not Opened Shops'] / data_dict[zone]['Number of Shops'] * 100
        data_dict[zone]['Not Open Shop Percentage'] = round(data_dict[zone]['Not Open Shop Percentage'], 2)
    except:
        pass

# # Print Value Dictionary
# for key, value in data_dict.items():
#     print(key, value)

df = pd.DataFrame(data_dict)

# print(df.index)
# print(df.columns)

df = df.reindex(['Number of Shops',
            'Expected Total Play Time',
            'Total Play Time (Minutes)',
            'Cumulative Efficiency',
            'Number of Satisfied Shops',
            'Satisfied Shop Percentage',
            'Expected Total Play Time for Satisfied',
            'Total Play Time (Minutes) for Satisfied',
            'Cumulative Efficiency for Satisfied',
            'Number of Not Opened Shops',
            'Not Open Shop Percentage',
            'Name of Not Opened Shops',
            'Number of Under Performing Shops',
            'Under Performing Shop Percentage',
            'Expected Total Play Time for Under Performing',
            'Total Play Time (Minutes) for Under Performing Shops',
            'Cumulative Efficiency for Under Performing',
            'Name of Under Performing Shops',
            'Before 10 AM',
            '10 AM to 11AM',
            'After 11 AM',
            'Name of After 11AM'
            ])

# print(df)
# df.to_csv('data.csv')

# Close the cursor and the connection
cursor.close()
conn.close()