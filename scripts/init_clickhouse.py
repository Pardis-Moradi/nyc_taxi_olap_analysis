import clickhouse_connect

def create_users_and_roles():
    client = clickhouse_connect.get_client(
        host='localhost',
        port=8123,
        user='default',
        password='',
        secure=False
    )

    # Create role if not exists
    client.command("CREATE ROLE IF NOT EXISTS readonly_role")

    # Create users
    client.command("""
        CREATE USER IF NOT EXISTS admin_user IDENTIFIED WITH plaintext_password BY 'admin_pass'
    """)
    client.command("""
        CREATE USER IF NOT EXISTS analyst_user IDENTIFIED WITH plaintext_password BY 'analyst_pass'
    """)

    # Grant privileges
    client.command("GRANT ALL ON *.* TO admin_user")
    client.command("GRANT SELECT ON *.* TO readonly_role")
    client.command("GRANT readonly_role TO analyst_user")

    print("✅ ClickHouse users and roles initialized.")

def create_taxi_table_if_not_exists(client):
    client.command("""
    CREATE TABLE IF NOT EXISTS ny_taxi_trips (
        vendor_id Int32,
        tpep_pickup_datetime DateTime,
        tpep_dropoff_datetime DateTime,
        passenger_count Nullable(Int32),
        trip_distance Float64,
        ratecode_id Nullable(Int32),
        store_and_fwd_flag Nullable(String),
        pulocation_id Int32,
        dolocation_id Int32,
        payment_type Int32,
        fare_amount Float64,
        extra Float64,
        mta_tax Float64,
        tip_amount Float64,
        tolls_amount Float64,
        improvement_surcharge Float64,
        total_amount Float64,
        congestion_surcharge Nullable(Float64),
        airport_fee Nullable(Float64),
        cbd_congestion_fee Nullable(Float64)
    ) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(tpep_pickup_datetime)
    ORDER BY (pulocation_id, dolocation_id, tpep_pickup_datetime);
    """)
    print("✅ Table is ready.")

def create_views_and_projections(client):
    # Materialized Views
    client.command("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS mv_trip_stats_daily
        ENGINE = SummingMergeTree
        PARTITION BY toYYYYMM(trip_day)
        ORDER BY (trip_day, vendor_id)
        AS
        SELECT
            toDate(tpep_pickup_datetime) AS trip_day,
            vendor_id,
            count(*) AS trip_count
        FROM ny_taxi_trips
        GROUP BY trip_day, vendor_id;
    """)

    client.command("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS mv_trip_counts_daily
        ENGINE = SummingMergeTree
        PARTITION BY toYYYYMM(trip_day)
        ORDER BY trip_day
        AS
        SELECT
            toDate(tpep_pickup_datetime) AS trip_day,
            count(*) AS trip_count,
            sum(trip_distance) AS total_distance,
            quantile(0.9)(trip_distance) AS p90_distance
        FROM ny_taxi_trips
        GROUP BY trip_day;

    """)

    client.command("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS mv_location_stats
        ENGINE = AggregatingMergeTree
        ORDER BY (pulocation_id, dolocation_id)
        AS
        SELECT
            pulocation_id,
            dolocation_id,
            count(*) AS trip_count,
            sumState(trip_distance) AS sum_distance_state,
            sumState(total_amount) AS sum_amount_state,
            avgState(tip_amount / total_amount * 100) AS tip_pct_state
        FROM ny_taxi_trips
        GROUP BY pulocation_id, dolocation_id;
    """)


    # Projections
    client.command("""
        ALTER TABLE ny_taxi_trips ADD PROJECTION IF NOT EXISTS vendor_avg_income
        (
            SELECT vendor_id, avg(total_amount) AS avg_income
            GROUP BY vendor_id
        );
    """)

    client.command("""
        ALTER TABLE ny_taxi_trips ADD PROJECTION IF NOT EXISTS payment_avg_tip
        (
            SELECT payment_type, avg(tip_amount) AS avg_tip
            GROUP BY payment_type
        );
    """)

    print("✅ Materialized views and projections created.")

def setup_project():
    client = clickhouse_connect.get_client(
        host='localhost',
        port=8123,
        user='default',
        password='',
        secure=False
    )
    # create_users_and_roles()
    create_taxi_table_if_not_exists(client)
    create_views_and_projections(client)

if __name__ == "__main__":
    setup_project()