WITH dif_cte AS (
	SELECT trade_id,
		time,
		price,
		quantity,
		quote_qty,
		order_type,
		LEAD(price, 1) OVER (ORDER BY time) - price AS dif
	FROM trades
	WHERE time BETWEEN :start_time
		AND :end_time
), 
dif_clean AS (
	SELECT trade_id,
		time,
		price,
		quantity,
		quote_qty,
		order_type,
		CASE
			WHEN dif < 0 AND order_type = 'Buy' THEN 0
			WHEN dif > 0 AND order_type = 'Sell' THEN 0
			ELSE dif
		END AS dif
	FROM dif_cte
),
zero_flags AS (
	SELECT 
		trade_id,
		time,
		price,
		quantity,
		quote_qty,
		order_type,
		dif,
		CASE
			WHEN dif = 0
				AND LAG(dif, 1) OVER (ORDER BY time) <> 0 
				OR LAG(order_type, 1) OVER (ORDER BY time) <> order_type
			THEN 1 
			ELSE 0
		END AS zero_block_start
	FROM dif_clean
),
zero_groups AS (
	SELECT 
		trade_id,
		time,
		price,
		quantity,
		quote_qty,
		order_type,
		dif,
		SUM(zero_block_start) OVER (ORDER BY time) AS zero_group
	FROM zero_flags
),
full_table AS (
	SELECT 
		zero_group,
		MAX(trade_id) AS trade_id,
		MAX(time) AS time,
		MIN(price) AS price,
		MAX(order_type) AS order_type,
		MAX(dif) AS dif,
		SUM(quantity) AS quantity,
		SUM(quote_qty) AS quote_qty
	FROM zero_groups
	WHERE dif = 0
	GROUP BY zero_group
	UNION
	SELECT
		zero_group,
		trade_id,
		time,
		price,
		order_type,
		dif,
		quantity,
		quote_qty
	FROM zero_groups
	WHERE dif <> 0
),
zero_new_flag AS (
	SELECT 
		trade_id,
		time,
		price,
		order_type,
		quantity,
		quote_qty,
		dif,
		CASE 
			WHEN dif = 0
				AND LAG(dif, 1) OVER (ORDER BY time) <> 0 THEN 2
			WHEN dif = 0
				AND LAG(dif, 1) OVER (ORDER BY time) = 0
				AND LAG(order_type, 1 ) OVER (ORDER BY time) <> order_type 
				AND order_type = 'Sell' THEN -1
			WHEN dif = 0
				AND LAG(dif, 1) OVER (ORDER BY time) = 0
				AND LAG(order_type, 1 ) OVER (ORDER BY time) <> order_type 
				AND order_type = 'Buy' THEN 1
			ELSE 0
		END AS zero_block_start
	FROM full_table
),
zero_new_groups AS (
	SELECT 
		trade_id,
		time,
		price,
		order_type,
		dif,
		quantity,
		quote_qty,
		SUM(zero_block_start) OVER (ORDER BY time) AS zero_group
	FROM zero_new_flag
),
new_full_table AS (
	SELECT 
		zero_group,
		MAX(trade_id) AS trade_id,
		MAX(time) AS time,
		MIN(price) AS price,
		MAX(order_type) AS order_type,
		MAX(dif) AS dif,
		SUM(quantity) AS quantity,
		SUM(quote_qty) AS quote_qty
	FROM zero_new_groups
	WHERE dif = 0
	GROUP BY zero_group
	UNION
	SELECT 
		zero_group,
		trade_id,
		time,
		price,
		order_type,
		dif,
		quantity,
		quote_qty
	FROM zero_new_groups
	WHERE dif <> 0
	ORDER BY trade_id
),
full_clean_table AS (
	SELECT 
		zero_group,
		trade_id,
		time,
		price,
		order_type,
		dif,
		CASE
			WHEN dif <> 0
				AND ABS(LAG(dif, 1) OVER (ORDER BY time)) < 0.0001
				AND LAG(order_type, 1) OVER (ORDER BY time) = order_type 
				THEN LAG(quantity, 1) OVER (ORDER BY time) + quantity
			WHEN dif <> 0
				AND ABS(LAG(dif, 2) OVER (ORDER BY time)) < 0.0001
				AND LAG(order_type, 2) OVER (ORDER BY time) = order_type 
				THEN LAG(quantity, 2) OVER (ORDER BY time) + quantity
			ELSE quantity
		END AS quantity,
		CASE
			WHEN dif <> 0
				AND ABS(LAG(dif, 1) OVER (ORDER BY time)) < 0.0001
				AND LAG(order_type, 1) OVER (ORDER BY time) = order_type 
				THEN LAG(quote_qty, 1) OVER (ORDER BY time) + quote_qty
			WHEN dif <> 0
				AND ABS(LAG(dif, 2) OVER (ORDER BY time)) < 0.0001
				AND LAG(order_type, 2) OVER (ORDER BY time) = order_type 
				THEN LAG(quote_qty, 2) OVER (ORDER BY time) + quote_qty
			ELSE quote_qty
		END AS quote_qty
	FROM new_full_table
),
new_dif_table AS (
	SELECT 
		trade_id,
		time,
		price,
		order_type,
		quantity,
		quote_qty,
		CASE
			WHEN order_type = 'Sell' THEN 1/SQRT(LEAD(price, 1) OVER (ORDER BY time)) - 1/SQRT(price)
			ELSE SQRT(LEAD(price, 1) OVER (ORDER BY time)) - SQRT(price)
		END AS dif
	FROM full_clean_table
	WHERE dif <> 0
),
liquidity_table AS (
	SELECT 
		trade_id,
		time,
		price,
		order_type,
		quantity,
		quote_qty,
		CASE
			WHEN order_type = 'Sell' THEN quantity/dif
			ELSE quote_qty/dif
		END AS liquidity
	FROM new_dif_table
	WHERE dif <> 0
),
block_flags AS (
	SELECT 
		time,
		price,
		order_type,
		liquidity,
		CASE 
			WHEN order_type <> LAG(order_type, 1) OVER (ORDER BY time)
			THEN 1
			ELSE 0
		END AS block_flag
	FROM liquidity_table
),
blocks AS (
	SELECT 
		time,
		price,
		order_type,
		liquidity,
		SUM(block_flag) OVER (ORDER BY time) AS block_id
	FROM block_flags
),
blocks_grouped AS (
	SELECT 
		block_id,
		order_type,
		MIN(time) AS time,
		CASE
			WHEN order_type = 'Sell' THEN MAX(price)
			ELSE MIN(price)
		END AS open,
		AVG(liquidity) AS avg_liquidity
	FROM blocks
	GROUP BY block_id, order_type
	ORDER by block_id
)
SELECT 
	time,
	open,
	CASE 
		WHEN order_type = 'Buy'
			THEN avg_liquidity / (LAG(avg_liquidity, 1) OVER (ORDER BY time))
		ELSE (LAG(avg_liquidity, 1) OVER (ORDER BY time)) / avg_liquidity
	END AS ratio
FROM blocks_grouped;
