SELECT
	date_trunc(:interval, time) AS time_interval,
	(ARRAY_AGG(price ORDER BY time ASC))[1] AS open,
	MAX(price) AS high,
	MIN(price) AS low,
	(ARRAY_AGG(price ORDER BY time DESC))[1] AS close
FROM trades
WHERE time BETWEEN :start_time
	AND :end_time
GROUP BY time_interval
ORDER BY time_interval;
