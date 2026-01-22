# de-camp-module-1-hw

1. To check that I firstly did 

docker run -it \
    --rm \
    --entrypoint=bash \
    python:3.13

and then - 

root@d553518e07cf:/# pip --version
pip 25.3 from /usr/local/lib/python3.13/site-packages/pip (python 3.13)

2. Considering the fact both containters are in the same docker-compose file, they are in the same network, so they will communicate by service name ("db") and port inside of container ("5432")

3. SELECT COUNT(*) 
FROM green_tripdata_2025_11
WHERE trip_distance <= 1
  AND lpep_pickup_datetime >= TIMESTAMP '2025-11-01'
  AND lpep_pickup_datetime <  TIMESTAMP '2025-12-01';

  It appeared that table contains 2 October rides, without them it is 8,007. 

4. WITH filtered AS (SELECT  * 
FROM green_tripdata_2025_11
WHERE trip_distance < 100
  AND lpep_pickup_datetime >= TIMESTAMP '2025-11-01'
  AND lpep_pickup_datetime <  TIMESTAMP '2025-12-01'),
maximum AS (SELECT MAX(trip_distance) AS max_distance FROM filtered)

SELECT DATE(lpep_pickup_datetime) FROM filtered
JOIN maximum ON filtered.trip_distance = maximum.max_distance

2025-11-14

5. SELECT zones."Zone", SUM(trips.total_amount) as total_amount
FROM green_tripdata_2025_11 AS trips
JOIN taxi_zone_lookup AS zones 
ON trips."PULocationID" = zones."LocationID"
WHERE trips.lpep_pickup_datetime >= TIMESTAMP '2025-11-18'
  AND trips.lpep_pickup_datetime <  TIMESTAMP '2025-11-19'
GROUP BY zones."Zone"
ORDER BY total_amount DESC
LIMIT 1

East Harlem North

6. WITH ehn_trips as (SELECT "DOLocationID", tip_amount
FROM green_tripdata_2025_11 AS trips
JOIN taxi_zone_lookup AS zones 
ON trips."PULocationID" = zones."LocationID"
WHERE zones."Zone" = 'East Harlem North'
AND lpep_pickup_datetime >= TIMESTAMP '2025-11-01'
AND lpep_pickup_datetime <  TIMESTAMP '2025-12-01')

SELECT taxi_zone_lookup."Zone" FROM ehn_trips
JOIN taxi_zone_lookup on taxi_zone_lookup."LocationID" = ehn_trips."DOLocationID"
ORDER BY tip_amount DESC
LIMIT 1

Yorkville West

7. https://github.com/EvgeniyaBrezhe/terrademo