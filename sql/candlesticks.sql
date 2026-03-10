SELECT
	date_trunc(:interval, time) AS interval,
	(ARRAY_AGG(price ORDER BY time ASC))[1] AS open,
	MAX(price) AS high,
	MIN(price) AS low,
	(ARRAY_AGG(price ORDER BY time DESC))[1] AS close,
FROM trades
WHERE time BETWEEN :start_time
	AND :end_time
GROUP BY interval
ORDER BY interval;
