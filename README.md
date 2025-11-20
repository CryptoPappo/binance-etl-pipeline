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

## Requirements
This project was built and tested using:

- Python 3.10+  
- PostgreSQL 13+  
- pip 23+  
- Ubuntu 20.04+ (recommended for scheduling with cron)

Python dependencies are listed in the `requirements.txt` file, which includes:

- requests
- pandas
- SQLAlchemy
- PyYAML
- psycopg2-binary
- logging (built-in)

## Installation
Clone the repository:
```bash
git clone https://github.com/CryptoPappo/binance-etl-pipeline.git
cd binance-etl-pipeline
```

Install PostgreSQL:
```bash
sudo apt install postgresql
```

Create and activate a virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate
```

Install dependencies:
```bash
pip3 install -r requirements.txt
```

Create a user and a database in PostgreSQL:
```bash
sudo -u postgres createuser --superuser $USER
createdb mydatabase
```

Then create the `trades` table using the script located in the `sql` folder:
```bash
psql -d mydatabase -f sql/create_tables.sql
```

## Configuration
All settings (symbol, DB connection and time window) are stored in `config/config.yaml`.

Example:
```yaml
symbol: "BTCUSDT"
hours_back: 1
database:
  url: "driver://username:password@host:port/database_name"
```
You can adapt the structure to your needs.

## Database Schema
The pipeline loads data into a PostgreSQL table similar to:
```sql
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
```bash
python3 src/main.py
```
**Run hourly with cron:**

Edit crontab:
```bash
crontab -e
```
Add:
```bash
0 * * * * /usr/bin/python3 /path/to/repo/src/main.py >> /path/to/repo/logs/etl.log 2>&1
```
This runs the ETL every hour on the hour, but you can configure it according to the time window chosen. 

## Pipeline Workflow
**1. Extract**

Fetch aggregated trades for the time window specified in `config/config.yaml`.

**2. Transform**

Convert raw JSON into a normalized DataFrame:
  * Convert numeric fields
  * Map `isBuyerMaker` to Buy/Sell categories
  * Convert date fields
  * Rename columns
  * Drop irrelevant fields and duplicates
    
**3. Load**

Insert DataFrame rows into PostgreSQL:
  * Bulk insert using SQLAlchemy
  * Handle duplicate `trade_id` safely
  * Commit transaction
    
**4. Logging**
    
Logs include:
  * Execution start/end
  * Number of trades processed
  * Errors from API or database
  * Cron-triggered execution reports

## Future Improvements
* Add support for multiple trading pairs
* Store raw and transformed data separately
* Move scheduling to Airflow for more complex workflow
* Add Docker support

## License
This project is open-source and available under the MIT License.

## Disclaimer
This project is for educational and data engineering practice purposes only.
It is not intended for automated trading or financial decision-making.
