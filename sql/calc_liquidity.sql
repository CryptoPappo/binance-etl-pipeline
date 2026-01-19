WITH dif_cte AS (
	SELECT trade_id,
		time,
		price,
		quantity,
		order_type,
		LEAD(price, 1) OVER (ORDER BY time) - price AS dif
	FROM trades
	WHERE time BETWEEN '2025-11-18 21:00:00'
			AND '2025-11-18 21:10:00'
), 
dif_clean AS (
	SELECT trade_id,
		time,
		price,
		quantity,
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
		SUM(quantity) AS quantity
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
		quantity
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
		SUM(quantity) AS quantity
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
		quantity
	FROM zero_new_groups
	WHERE dif <> 0
	ORDER BY trade_id
)
SELECT * 
FROM new_full_table;
