```
For the trips in November 2025 (lpep_pickup_datetime between '2025-11-01' and '2025-12-01', exclusive of the upper bound), how many trips had a trip_distance of less than or equal to 1 mile?
```

SELECT 'November' as month, COUNT(*) count_trip FROM green_taxi_data_2025_11 
WHERE trip_distance <= 1
AND lpep_pickup_datetime >= '2025-11-01' AND lpep_pickup_datetime <= '2025-12-01';

```
Which was the pick up day with the longest trip distance? Only consider trips with trip_distance less than 100 miles (to exclude data errors).
Use the pick up time for your calculations.
```

SELECT
DATE(lpep_pickup_datetime) AS date_pick_up,
MAX(trip_distance) AS longest_trip
FROM green_taxi_data_2025_11
WHERE trip_distance <= 100
GROUP BY 1
ORDER BY 2 DESC;

```
Which was the pickup zone with the largest total_amount (sum of all trips) on November 18th, 2025?
```