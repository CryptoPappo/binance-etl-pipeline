import yaml
import time
from extract import extract
from transform import transform
from load import load

def main():
    with open("/root/binance-etl-pipeline/config/config.yaml") as f:
        config = yaml.safe_load(f)

    symbol = config["symbol"]
    db_url = config["database"]["url"]
    start_time = int(time.time() * 1000) - 1000

    raw = extract(symbol, start_time)
    transformed = transform(raw)
    load(transformed, db_url)

if __name__ == "__main__":
    main()
