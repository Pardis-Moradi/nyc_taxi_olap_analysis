# 🗽 NYC Taxi OLAP Project

This repository contains an OLAP-style analytical pipeline built on top of NYC Taxi trip data using [ClickHouse](https://clickhouse.com/), designed for high-performance querying and aggregation. The project is part of the **Database Design course (Spring 2025)** at **Sharif University of Technology**.

## 🎯 Project Aim

The goal of this project is to design and implement a scalable analytical backend for NYC taxi trip data, enabling fast execution of complex queries such as aggregations, percentile calculations, and rolling averages. It demonstrates:

- Schema design for OLAP workloads
- Use of materialized views and projections
- Compression and indexing strategies
- Query optimization and benchmarking

## 🧱 Project Structure

nyc_taxi_olap_project/
├── setup/                  # Initialization scripts for ClickHouse
│   └── init_clickhouse.py  # Table/view creation and configuration
├── scripts/                # Utility scripts for resetting and managing data
│   └── reset_project.py    # Drops and rebuilds the schema
├── query_scenarios/        # Query definitions and benchmarking logic
│   └── scenario_runner.py  # Runs query scenarios in parallel
├── data/                   # Input and processed data files
├── run.sh                  # Main entrypoint for setup, reset, and run commands
└── README.md               # You're reading it!


## 📦 Components Explained

### `setup/init_clickhouse.py`
- Creates the main fact table `ny_taxi_trips`
- Defines materialized views:
  - `mv_trip_counts_daily`: daily trip stats
  - `mv_trip_stats_daily`: vendor-level daily stats
  - `mv_location_stats`: pickup/dropoff location aggregates
- Adds projections and compression codecs for performance

### `scripts/reset_project.py`
- Drops existing tables and views
- Moves processed files back to input directory
- Re-initializes the schema using `setup_project()`

### `query_scenarios/scenario_runner.py`
- Executes 10 analytical queries across multiple threads
- Measures latency and throughput for each scenario
- Supports `--optimized` flag to use materialized views and projections

### `run.sh`
A shell wrapper for common tasks:
```bash
./run.sh setup        # Initialize ClickHouse schema
./run.sh reset        # Reset the project state
./run.sh run          # Run query scenarios
./run.sh run --optimized  # Run optimized queries using views/projections
```


## 📊 Query Scenarios

Includes queries such as:

- **Daily trip counts**
- **Vendor income averages**
- **Location-based aggregations**
- **Rolling averages**
- **Percentile-based filtering**

Each query is designed to test a different aspect of OLAP performance and schema design.

## 🚀 How to Run

### 1. Install Dependencies

- Python **3.12+**
- [`clickhouse-connect`](https://pypi.org/project/clickhouse-connect/)
- A running **ClickHouse server** on `localhost:8123`

### 2. Setup the Project

```bash
./run.sh setup
```

### 3. Run Query Scenarios

```bash
./run.sh run
```

### 4. Run Optimized Queries (using views and projections)

```bash
./run.sh run --optimized
```

### 5. Reset the Project

```bash
./run.sh reset
```

## 🏫 Academic Context

This project was developed as part of the **Database Design** course offered in **Spring 2025** at **Sharif University of Technology**. It showcases practical applications of:

- **OLAP modeling**
- **Query optimization**
- **Materialized views and projections**
- **Compression and indexing strategies**
- **Performance benchmarking in analytical databases**
