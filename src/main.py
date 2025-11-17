import yaml
import time
from extract import extract
from transform import transform
from load import load
from utils import get_logger
logger = get_logger("main")

def main():
    logger.info("ETL job started")

    with open("/root/binance-etl-pipeline/config/config.yaml") as f:
        config = yaml.safe_load(f)

    symbol = config["symbol"]
    db_url = config["database"]["url"]
    start_time = int(time.time() * 1000 - 60 * 60 * 1000)
    
    try:
        logger.info("Extracting data...")
        raw = extract(symbol, start_time)
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
