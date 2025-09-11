-- Query 1: Daily trip count
SELECT trip_day, trip_count AS trip_count
    FROM mv_trip_counts_daily
    ORDER BY trip_day;

-- Query 2: Average income per vendor
SELECT vendor_id, avg(total_amount) AS avg_income
    FROM ny_taxi_trips
    GROUP BY vendor_id;

-- Query 3: Total trip distance per pickup location
SELECT pulocation_id, sumMerge(sum_distance_state) AS total_distance
    FROM mv_location_stats
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
SELECT current_day.trip_day,
       avg(past.trip_count) AS rolling_avg_trip_count
    FROM mv_trip_counts_daily AS current_day
    JOIN mv_trip_counts_daily AS past
        ON past.trip_day BETWEEN current_day.trip_day - INTERVAL 6 DAY AND current_day.trip_day
    GROUP BY current_day.trip_day
    ORDER BY current_day.trip_day;

-- Query 7: Top 10 pickup/dropoff pairs by total income
SELECT pulocation_id, dolocation_id, sumMerge(sum_amount_state) AS total_income
    FROM mv_location_stats
    GROUP BY pulocation_id, dolocation_id
    ORDER BY total_income DESC
    LIMIT 10;

-- Query 8: Daily P90 of trip distance
SELECT trip_day, p90_distance
    FROM mv_trip_counts_daily
    ORDER BY trip_day;

-- Query 9: Ranking dropoff locations by average tip percentage
SELECT dolocation_id, avgMerge(tip_pct_state) AS avg_tip_percent
    FROM mv_location_stats
    GROUP BY dolocation_id
    ORDER BY avg_tip_percent DESC;

-- Query 10: Vendor IDs with daily trip count above 95th percentile
WITH thresholds AS (
    SELECT trip_day, quantile(0.95)(trip_count) AS p95_count
    FROM mv_trip_stats_daily
    GROUP BY trip_day
)
SELECT d.vendor_id, d.trip_day, d.trip_count
    FROM mv_trip_stats_daily d
    JOIN thresholds t ON d.trip_day = t.trip_day
    WHERE d.trip_count > t.p95_count
    ORDER BY d.trip_day, d.trip_count DESC;