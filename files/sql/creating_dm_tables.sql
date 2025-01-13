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

-- Витрина 101 формы
-- Она содержит информацию об остатках и оборотах за отчетный период, сгруппированных по балансовым счетам второго порядка.
--DROP TABLE IF EXISTS dm.dm_f101_round_f;
CREATE TABLE IF NOT EXISTS dm.dm_f101_round_f(
	from_date				DATE,
	to_date					DATE,
	chapter					CHAR(1),
	ledger_account			CHAR(5),
	characteristic			CHAR(1),
	balance_in_rub			NUMERIC(23,8),
	r_balance_in_rub		NUMERIC(23,8),
	balance_in_val			NUMERIC(23,8),
	r_balance_in_val		NUMERIC(23,8),
	balance_in_total		NUMERIC(23,8),
	r_balance_in_total	    NUMERIC(23,8),
	turn_deb_rub			NUMERIC(23,8),
	r_turn_deb_rub			NUMERIC(23,8),
	turn_deb_val			NUMERIC(23,8),
	r_turn_deb_val			NUMERIC(23,8),
	turn_deb_total			NUMERIC(23,8),
	r_turn_deb_total		NUMERIC(23,8),
	turn_cre_rub			NUMERIC(23,8),
	r_turn_cre_rub			NUMERIC(23,8),
	turn_cre_val			NUMERIC(23,8),
	r_turn_cre_val			NUMERIC(23,8),
	turn_cre_total			NUMERIC(23,8),
	r_turn_cre_total		NUMERIC(23,8),
	balance_out_rub			NUMERIC(23,8),
	r_balance_out_rub		NUMERIC(23,8),
	balance_out_val			NUMERIC(23,8),
	r_balance_out_val		NUMERIC(23,8),
	balance_out_total		NUMERIC(23,8),
	r_balance_out_total	    NUMERIC(23,8)
);

-- Создание копии таблицы 101 формы
--DROP TABLE IF EXISTS dm.dm_f101_round_f_v2;
CREATE TABLE IF NOT EXISTS dm.dm_f101_round_f_v2(
	from_date						DATE,
	to_date							DATE,
	chapter							CHAR(1),
	ledger_account			CHAR(5),
	characteristic			CHAR(1),
	balance_in_rub			NUMERIC(23,8),
	r_balance_in_rub		NUMERIC(23,8),
	balance_in_val			NUMERIC(23,8),
	r_balance_in_val		NUMERIC(23,8),
	balance_in_total		NUMERIC(23,8),
	r_balance_in_total	NUMERIC(23,8),
	turn_deb_rub				NUMERIC(23,8),
	r_turn_deb_rub			NUMERIC(23,8),
	turn_deb_val				NUMERIC(23,8),
	r_turn_deb_val			NUMERIC(23,8),
	turn_deb_total			NUMERIC(23,8),
	r_turn_deb_total		NUMERIC(23,8),
	turn_cre_rub				NUMERIC(23,8),
	r_turn_cre_rub			NUMERIC(23,8),
	turn_cre_val				NUMERIC(23,8),
	r_turn_cre_val			NUMERIC(23,8),
	turn_cre_total			NUMERIC(23,8),
	r_turn_cre_total		NUMERIC(23,8),
	balance_out_rub			NUMERIC(23,8),
	r_balance_out_rub		NUMERIC(23,8),
	balance_out_val			NUMERIC(23,8),
	r_balance_out_val		NUMERIC(23,8),
	balance_out_total		NUMERIC(23,8),
	r_balance_out_total	NUMERIC(23,8)
);