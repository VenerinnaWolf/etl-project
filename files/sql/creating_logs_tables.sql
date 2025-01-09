CREATE SCHEMA IF NOT EXISTS logs;

-- Таблица для логирования загрузки данных из csv файлов
--DROP TABLE IF EXISTS logs.load_logs;
CREATE TABLE IF NOT EXISTS logs.load_logs(
	run_id			VARCHAR(100) PRIMARY KEY,
	start_time      TIMESTAMP,
	end_time 		TIMESTAMP,
	duration 		INTERVAL
);

-- Таблица для логирования процедур
--DROP TABLE IF EXISTS logs.procedures_logs;
CREATE TABLE IF NOT EXISTS logs.procedures_logs(
	run_id          SERIAL PRIMARY KEY,
	procedure_name  VARCHAR(50),
	start_time      TIMESTAMP,
	end_time        TIMESTAMP,
	duration        INTERVAL
);