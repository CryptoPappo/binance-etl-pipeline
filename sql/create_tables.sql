CREATE TABLE IF NOT EXISTS trades (
	trade_id BIGINT PRIMARY KEY,
	price NUMERIC(18, 6),
	quantity NUMERIC(18, 6),
	quote_qty NUMERIC(18, 6),
	time TIMESTAMP,
	order_type VARCHAR(6)
);
