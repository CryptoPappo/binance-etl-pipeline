import yaml
import time
from extract import extract
from transform import transform
from load import load
from utils import get_logger, get_latest_tradeId
logger = get_logger("main")

def main():
    """

    Run the full ETL pipeline for downloading and storing Binance aggregated trades
    from a single trading pair.

    Steps performed:
        1. Initialize logger and configuration.
        2. Extract data from the Binance API.
        3. Transform the raw trade data into a structured format.
        4. Insert the transformed data into the PostgreSQL database.
        5. Record success or detailed error information in the logs.


    Raises:
        Exception: If any step of the ETL pipeline fails. The exception is
            logged before being re-raised to allow cron or other scheduling
            tools to detect the failed execution.
    """
    logger.info("ETL job started")
    with open("/root/binance-etl-pipeline/config/config.yaml") as f:
        config = yaml.safe_load(f)
    symbol = config["symbol"]
    db_url = config["database"]["url"]
    start_time = int(time.time()*1000 - 60*60*1000)

    logger.info("Retrieving latest tradeId stored...")
    tradeId = get_latest_tradeId(db_url)
    if tradeId is not None:
        tradeId +=1 

    try:
        logger.info("Extracting data...")
        raw = extract(symbol, start_time, tradeId)
        logger.info("Transforming data...")
        transformed = transform(raw)
        logger.info("Loading data...")
        load(transformed, db_url)
        logger.info("ETL job finished successfully")
    except Exception as e:
        logger.exception("ETL failed due to an error")
        raise

if __name__ == "__main__":
    main()
