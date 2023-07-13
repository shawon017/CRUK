WITH trip_data AS (
    SELECT 
        EXTRACT(WEEK FROM tpep_pickup_datetime) AS week,
        PULocationID,
        trip_distance / EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime))/3600 AS trip_speed
    FROM public.nyctripdata
    AND  tpep_dropoff_datetime > tpep_pickup_datetime
),
average_speed AS (
    SELECT AVG(trip_speed) AS avg_speed
    FROM trip_data
),
slow_trips AS (
    SELECT week, PULocationID
    FROM trip_data
    WHERE trip_speed < (SELECT avg_speed FROM average_speed)
),
grouped_slow_trips AS (
    SELECT week, PULocationID, COUNT(*) AS trip_count
    FROM slow_trips
    GROUP BY week, PULocationID
)
SELECT week, PULocationID, trip_count
FROM (
    SELECT week, PULocationID, trip_count, ROW_NUMBER() OVER (PARTITION BY week ORDER BY trip_count DESC) AS rn
    FROM grouped_slow_trips
) t
WHERE rn <= 5
ORDER BY week, trip_count DESC, PULocationID;
