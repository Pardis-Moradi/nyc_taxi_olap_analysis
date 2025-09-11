import pandas as pd
from config import CLICKHOUSE_TABLE

def preprocess_data(df):
    processed_df = df.copy()

    # 1. Standardize column names to lowercase
    processed_df.columns = [col.lower() for col in processed_df.columns]

    # Rename columns to match the list above if they exist in the dataframe
    processed_df.rename(columns={
        'vendor_id': 'vendorid', 'ratecode_id': 'ratecodeid', 
        'pulocation_id':'pulocationid', 'dolocation_id': 'dolocationid'
    }, inplace=True)

    expected_cols = {
        'vendorid': 0, 'tpep_pickup_datetime': None, 'tpep_dropoff_datetime': None,
        'passenger_count': 0, 'trip_distance': 0.0, 'ratecodeid': 0,
        'store_and_fwd_flag': '-', 'pulocationid': 0, 'dolocationid': 0,
        'payment_type': 0, 'fare_amount': 0.0, 'extra': 0.0, 'mta_tax': 0.0,
        'tip_amount': 0.0, 'tolls_amount': 0.0, 'improvement_surcharge': 0.0,
        'total_amount': 0.0, 'congestion_surcharge': 0.0, 'airport_fee': 0.0,
        'cbd_congestion_fee': 0.0
    }
    
    for col, default_value in expected_cols.items():
        if col not in processed_df.columns:
            processed_df[col] = default_value

    # Convert datetime columns
    processed_df['tpep_pickup_datetime'] = pd.to_datetime(processed_df['tpep_pickup_datetime'], errors='coerce')
    processed_df['tpep_dropoff_datetime'] = pd.to_datetime(processed_df['tpep_dropoff_datetime'], errors='coerce')

    # Convert numeric columns
    numeric_cols = [
        'vendorid', 'passenger_count', 'trip_distance', 'ratecodeid', 'pulocationid', 
        'dolocationid', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 
        'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 
        'congestion_surcharge', 'airport_fee', 'cbd_congestion_fee'
    ]
    for col in numeric_cols:
        if col in processed_df.columns:
            processed_df[col] = pd.to_numeric(processed_df[col], errors='coerce')
    

    # 3. Handle missing data created during coercion
    # Drop rows where critical information is missing
    processed_df.dropna(subset=['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'pulocationid', 'dolocationid', 'vendorid'], inplace=True)
    
    # if 'store_and_fwd_flag' in processed_df.columns:
    processed_df['store_and_fwd_flag'] = processed_df['store_and_fwd_flag'].fillna('-')

    # For non-critical numeric fields, fill missing values with 0
    numeric_cols_to_fill = [col for col in numeric_cols if col in processed_df.columns]
    processed_df[numeric_cols_to_fill] = processed_df[numeric_cols_to_fill].fillna(0)

    # check and correct total_amount:
    component_cols = [
        'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
        'improvement_surcharge', 'congestion_surcharge', 'airport_fee',
        'cbd_congestion_fee'
    ]
    processed_df['total_amount'] = processed_df[component_cols].sum(axis=1)

    # 4. Apply data validation and business rules
    # Ensure total_amount is positive
    processed_df = processed_df[processed_df['total_amount'] > 0]

    # Trip distance must be greater than 0
    processed_df = processed_df[processed_df['trip_distance'] > 0]

    # Passenger count should be reasonable (e.g., 1 to 6)
    processed_df = processed_df[processed_df['passenger_count'].between(1, 6)]

    # Rule: Pickup time must be before dropoff time
    processed_df = processed_df[processed_df['tpep_pickup_datetime'] < processed_df['tpep_dropoff_datetime']]

    # 5. Final type casting to match ClickHouse schema precisely
    # This converts float representations of IDs (like 1.0) to clean integers (1).
    int_cols = ['vendorid', 'passenger_count', 'ratecodeid', 'pulocationid', 'dolocationid', 'payment_type']
    for col in int_cols:
        if col in processed_df.columns:
            processed_df[col] = processed_df[col].astype(int)

    # 6. Ensure final columns match the database table to prevent insertion errors
    final_columns = [
        'vendor_id', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count',
        'trip_distance', 'ratecode_id', 'store_and_fwd_flag', 'pulocation_id',
        'dolocation_id', 'payment_type', 'fare_amount', 'extra', 'mta_tax',
        'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount',
        'congestion_surcharge', 'airport_fee', 'cbd_congestion_fee'
    ]

    # Rename columns back to the database schema names
    processed_df.rename(columns={
        'vendorid': 'vendor_id', 'ratecodeid': 'ratecode_id', 
        'pulocationid':'pulocation_id', 'dolocationid': 'dolocation_id'
    }, inplace=True)

    df_columns = [col for col in final_columns if col in processed_df.columns]
    processed_df = processed_df[df_columns]

    return processed_df