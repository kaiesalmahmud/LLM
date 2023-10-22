import asyncio
import asyncpg


host="server-alpha.ad-iq.com",
port="32974",
dbname="adiq",
user="mlteam",
password="q7a2C6MKV570B1MNZSQpyO3EifwdtkmV"

# host="localhost"
# port="5432"
# dbname="ReportDB"
# user="postgres"
# password="postgres"


start_date = "'2023-10-15'"
end_date = "'2023-10-16'"

query = f"""WITH "on_beats" as (
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
        "on_beats"."day_start",
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
        {start_date} :: date + (i || ' days')::interval as "date"
    FROM generate_series(0, {end_date} :: date - {start_date} :: date) i
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
        dates.date::date as "Date",
        start_time as "Started",
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
        AND dates.date = raw_data.day_start
    LEFT OUTER JOIN "raw_points"
        ON "raw_points".document->>'bundle_id' = "bundles"."bundle_id"
    LEFT OUTER JOIN "raw_areas"
        ON "raw_areas".id = "raw_points".document->>'area_id'
    LEFT OUTER JOIN "raw_zones"
        ON "raw_zones".id = "raw_areas".document->>'zone_id'
    ORDER BY
        dates.date::date,
        bundles.bundle_number
)

SELECT * FROM "dailyLog" LIMIT 5;"""

async def run():
    conn = await asyncpg.connect(user="mlteam", password="q7a2C6MKV570B1MNZSQpyO3EifwdtkmV",
                                 database="adiq", host="server-alpha.ad-iq.com", port="32974")
    values = await conn.fetch(
        query
    )

    print(values)
    await conn.close()

loop = asyncio.get_event_loop()
loop.run_until_complete(run())
