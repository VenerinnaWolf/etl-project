CREATE SCHEMA IF NOT EXISTS logs;

--DROP TABLE IF EXISTS logs.load_logs;
CREATE TABLE IF NOT EXISTS logs.load_logs(
	run_id			VARCHAR(100) PRIMARY KEY,
	start_time      TIMESTAMP,
	end_time 		TIMESTAMP,
	duration 		INTERVAL
);