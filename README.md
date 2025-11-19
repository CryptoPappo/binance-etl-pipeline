# Binance Trading Data ETL Pipeline
This project implements a simple yet production-style ETL pipeline that collects **aggregated trades** for a single trading pair from the Binance REST API, transforms the data using pandas, and loads it into a PostgreSQL database.
The pipeline is executed hourly via a cron job.

The goal of this project is to strengthen practical data engineering skills using Python, SQL, scheduling, environment management, and clean project organization.

## Features
* Extracts aggregated trades using the Binance `/api/v3/aggTrades` endpoint
* Cleans and transforms raw data into a structured pandas DataFrame
* Derives key fields:
  - `trade_id`
  - `price`
  - `quantity`
  - `quote_qty`
  - `time`
  - `order_type` (mapped to Buy/Sell)
* Inserts transformed data into a PostgreSQL table
* Logs pipeline execution with Python's `logging` module
* Scheduled to run hourly using `cron`
* Organized following best practices for small ETL projects

## Tech Stack
* **Python 3**
* **Requests** (API calls)
* **Pandas** (data transformations)
* **SQLAlchemy / psycopg2** (database connection)
* **PostgreSQL**
* **Cron** (scheduling)
* **Git** (version control)

## Project Structure
```
binance-etl-pipeline/
├── src/
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   ├── utils.py
│   └── main.py
├── sql
|    └── create_tables.sql
├── config/
│   └── config.yaml
├── logs/
│   └── etl.log
├── requirements.txt
└── README.md
```

## Configuration
All settings (symbol, API URL, DB connection, etc.) are stored in `config/config.yaml`.

Example:
```
symbol: "BTCUSDT"
database:
  url: "driver://username:password@host:port/database_name"
```
You can adapt the structure to your needs.

## Database Schema
The pipeline loads data into a PostgreSQL table similar to:
```
CREATE TABLE IF NOT EXISTS trades (
  trade_id BIGINT PRIMARY KEY,
  price NUMERIC,
  quantity NUMERIC,
  quote_qty NUMERIC,
  time TIMESTAMP,
  order_type VARCHAR
)
```
Duplicate rows (based on `trade_id`) are naturally handled by PostgreSQL’s primary key constraint.

## How to Run the Pipeline
**Run manually:**
```
python3 src/main.py
```
**Run hourly with cron:**
Edit crontab:
```
crontab -e
```
Add:
```
0 * * * * /usr/bin/python3 /path/to/repo/src/main.py >> /path/to/repo/logs/etl.log 2>&1
```
This runs the ETL every hour on the hour. 

