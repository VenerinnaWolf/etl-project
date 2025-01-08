CREATE SCHEMA IF NOT EXISTS dm;

-- Витрина оборотов по лицевым счетам
--DROP TABLE IF EXISTS dm.dm_account_turnover_f;
CREATE TABLE IF NOT EXISTS dm.dm_account_turnover_f(
	on_date				DATE,
	account_rk			NUMERIC,
	credit_amount		NUMERIC(23,8),
	credit_amount_rub	NUMERIC(23,8),
	debet_amount		NUMERIC(23,8),
	debet_amount_rub	NUMERIC(23,8)
);

-- Витрина остатков по лицевым счетам
--DROP TABLE IF EXISTS dm.dm_account_balance_f;
CREATE TABLE IF NOT EXISTS dm.dm_account_balance_f(
	on_date       	DATE NOT NULL,
	account_rk    	INT8 NOT NULL,
	currency_rk   	INT8,
	balance_out   	NUMERIC(23,8),
	balance_out_rub NUMERIC(23,8)
);