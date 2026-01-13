```
Question 3. Counting short trips

For the trips in November 2025 (lpep_pickup_datetime between '2025-11-01' and '2025-12-01', exclusive of the upper bound), how many trips had a trip_distance of less than or equal to 1 mile?

Answer: 8007
```

SELECT 'November' as month, COUNT(*) count_trip FROM green_taxi_data_2025_11 
WHERE trip_distance <= 1
AND lpep_pickup_datetime >= '2025-11-01' AND lpep_pickup_datetime <= '2025-12-01';


```
Question 4. Longest trip for each day

Which was the pick up day with the longest trip distance? Only consider trips with trip_distance less than 100 miles (to exclude data errors).
Use the pick up time for your calculations.

Answer: 2025-11-14
```

SELECT
DATE(lpep_pickup_datetime) AS date_pick_up,
MAX(trip_distance) AS longest_trip
FROM green_taxi_data_2025_11
WHERE trip_distance <= 100
GROUP BY 1
ORDER BY 2 DESC;


```
Question 5. Biggest pickup zone

Which was the pickup zone with the largest total_amount (sum of all trips) on November 18th, 2025?

Answer: East Harlem North

```

SELECT
    a."PULocationID" AS pickup_id,
    (
        SELECT b."Zone"
        FROM public.zone b
        WHERE a."PULocationID" = b."LocationID"
    ) AS zone_name,
    SUM(a.total_amount) AS sum_all_trips
FROM public.green_taxi_data_2025_11 a
WHERE DATE(a.lpep_pickup_datetime) = '2025-11-18'
GROUP BY 1,2
ORDER BY 3 DESC;


```
Question 6. Largest tip
For the passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop off zone that had the largest tip?

Answer: Yorkville West
```

SELECT
a.pu_id,
a.pu_zone,
a.do_id,
b."Zone",
a.tip_amount
FROM (
	SELECT a."PULocationID" AS pu_id,
	b."Zone" pu_zone,
	a."DOLocationID" AS do_id,
	a.tip_amount
	FROM public.green_taxi_data_2025_11 a
	LEFT JOIN public.zone b 
	ON a."PULocationID" = b."LocationID"
	WHERE DATE(a.lpep_pickup_datetime) >= '2025-11-01' 
	AND DATE(a.lpep_pickup_datetime) <= '2025-11-30'
	AND b."Zone" = 'East Harlem North'
	) a
LEFT JOIN public.zone b
ON a."do_id" = b."LocationID"
ORDER BY 5 DESC;