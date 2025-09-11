-- Query 1: Daily trip count
SELECT toDate(tpep_pickup_datetime) AS trip_day, count(*) AS trip_count
    FROM ny_taxi_trips
    GROUP BY trip_day
    ORDER BY trip_day;

-- Query 2: Average income per vendor
SELECT vendor_id, avg(total_amount) AS avg_income
    FROM ny_taxi_trips
    GROUP BY vendor_id;

-- Query 3: Total trip distance per pickup location
SELECT pulocation_id, sum(trip_distance) AS total_distance
    FROM ny_taxi_trips
    GROUP BY pulocation_id
    ORDER BY total_distance DESC;

-- Query 4: Average tip per payment type
SELECT payment_type, avg(tip_amount) AS avg_tip
    FROM ny_taxi_trips
    GROUP BY payment_type
    ORDER BY avg_tip DESC;

-- Query 5: Monthly income per dropoff location
SELECT dolocation_id, toYYYYMM(tpep_pickup_datetime) AS month, sum(total_amount) AS monthly_income
    FROM ny_taxi_trips
    GROUP BY dolocation_id, month
    ORDER BY month, monthly_income DESC;

-- Query 6: 7-day rolling average of daily trip count
WITH daily_trip_counts AS (
   SELECT toDate(tpep_pickup_datetime) AS trip_day, count(*) AS trip_count
   FROM ny_taxi_trips
   GROUP BY trip_day
)
SELECT current_day.trip_day,
      avg(past.trip_count) AS rolling_avg_trip_count
   FROM daily_trip_counts AS current_day
   JOIN daily_trip_counts AS past
       ON past.trip_day BETWEEN current_day.trip_day - INTERVAL 6 DAY AND current_day.trip_day
   GROUP BY current_day.trip_day
   ORDER BY current_day.trip_day;

-- Query 7: Top 10 pickup/dropoff pairs by total income
SELECT pulocation_id, dolocation_id, sum(total_amount) AS total_income
    FROM ny_taxi_trips
    GROUP BY pulocation_id, dolocation_id
    ORDER BY total_income DESC
    LIMIT 10;

-- Query 8: Daily P90 of trip distance
SELECT toDate(tpep_pickup_datetime) AS trip_day, quantile(0.9)(trip_distance) AS p90_distance
    FROM ny_taxi_trips
    GROUP BY trip_day
    ORDER BY trip_day;

-- Query 9: Ranking dropoff locations by average tip percentage
SELECT dolocation_id, avg(tip_amount / total_amount) * 100 AS avg_tip_percent
    FROM ny_taxi_trips
    GROUP BY dolocation_id
    ORDER BY avg_tip_percent DESC;

-- Query 10: Vendor IDs with daily trip count above 95th percentile
WITH daily_counts AS (
   SELECT vendor_id, toDate(tpep_pickup_datetime) AS trip_day, count(*) AS trip_count
   FROM ny_taxi_trips
   GROUP BY vendor_id, trip_day
),
thresholds AS (
   SELECT trip_day, quantile(0.95)(trip_count) AS p95_count
   FROM daily_counts
   GROUP BY trip_day
)
SELECT d.vendor_id, d.trip_day, d.trip_count
   FROM daily_counts d
   JOIN thresholds t ON d.trip_day = t.trip_day
   WHERE d.trip_count > t.p95_count
   ORDER BY d.trip_day, d.trip_count DESC;