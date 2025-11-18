from typing import Union
import logging
from logging import Logger
from logging.handlers import RotatingFileHandler
import os
from sqlalchemy import create_engine, text

def get_logger(name: str) -> Logger:
    log_path = "/root/binance-etl-pipeline/logs/etl.log"

    # Ensure logs directory exists
    os.makedirs(os.path.dirname(log_path), exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Prevent duplicate handlers if loaded multiple times
    if logger.hasHandlers():
        return logger

    # Rotating file handler: 5MB per file, keep 2 backups
    handler = RotatingFileHandler(
        log_path, 
        maxBytes=5_000_000,
        backupCount=2
    )

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger

def get_latest_tradeId(db_url: str) -> Union[int, None]:
    engine = create_engine(db_url)
    query = text("SELECT MAX(trade_id) FROM trades;")
    with engine.connect() as conn:
        results = conn.execute(query).fetchall()
    
    return results[0][0]

