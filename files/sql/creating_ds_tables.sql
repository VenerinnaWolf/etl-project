CREATE SCHEMA IF NOT EXISTS ds;

--DROP TABLE IF EXISTS ds.ft_balance_f;
CREATE TABLE IF NOT EXISTS ds.ft_balance_f(
	on_date       DATE NOT NULL,
	account_rk    INT8 NOT NULL,
	currency_rk   INT8,
	balance_out   NUMERIC(19,2),
	PRIMARY KEY (on_date, account_rk)
);

--DROP TABLE IF EXISTS ds.ft_posting_f;
CREATE TABLE IF NOT EXISTS ds.ft_posting_f(
	oper_date          DATE NOT NULL,
	credit_account_rk  INT8 NOT NULL,
	debet_account_rk   INT8 NOT NULL,
	credit_amount      NUMERIC(19,2),
	debet_amount       NUMERIC(19,2)
);

--DROP TABLE IF EXISTS ds.md_account_d;
CREATE TABLE IF NOT EXISTS ds.md_account_d(
	data_actual_date 		DATE NOT NULL,
	data_actual_end_date 	DATE NOT NULL,
	account_rk 				INT8 NOT NULL,
	account_number 			VARCHAR(20) NOT NULL,
	char_type 				VARCHAR(1) NOT NULL,
	currency_rk 			INT8 NOT NULL,
	currency_code 			VARCHAR(3) NOT NULL,
	PRIMARY KEY (data_actual_date, account_rk)
);

--DROP TABLE IF EXISTS ds.md_currency_d;
CREATE TABLE IF NOT EXISTS ds.md_currency_d(
	currency_rk				INT8 NOT NULL,
	data_actual_date		DATE NOT NULL,
	data_actual_end_date	DATE,
	currency_code			VARCHAR(3),
	code_iso_char			VARCHAR(3),
	PRIMARY KEY (currency_rk, data_actual_date)
);

--DROP TABLE IF EXISTS ds.md_exchange_rate_d;
CREATE TABLE IF NOT EXISTS ds.md_exchange_rate_d(
	data_actual_date		DATE NOT NULL,
	data_actual_end_date	DATE,
	currency_rk				INT8 NOT NULL,
	reduced_cource			NUMERIC(12,10),
	code_iso_num			VARCHAR(3),
	PRIMARY KEY (data_actual_date, currency_rk)
);

--DROP TABLE IF EXISTS ds.md_ledger_account_s;
CREATE TABLE IF NOT EXISTS ds.md_ledger_account_s(
	chapter 				CHAR(1),
	chapter_name 			VARCHAR(16),
	section_number 			INTEGER,
	section_name 			VARCHAR(22),
	subsection_name 		VARCHAR(21),
	ledger1_account 		INTEGER,
	ledger1_account_name 	VARCHAR(47),
	ledger_account 			INTEGER NOT NULL,
	ledger_account_name 	VARCHAR(153),
	characteristic 			CHAR(1),
	is_resident 			INTEGER,
	is_reserve 				INTEGER,
	is_reserved 			INTEGER,
	is_loan 				INTEGER,
	is_reserved_assets 		INTEGER,
	is_overdue 				INTEGER,
	is_interest 			INTEGER,
	pair_account 			VARCHAR(5),
	start_date 				DATE NOT NULL,
	end_date 				DATE,
	is_rub_only     		INTEGER,
	min_term 		    	VARCHAR(1),
	min_term_measure 		VARCHAR(1),
	max_term 				VARCHAR(1),
	max_term_measure 		VARCHAR(1),
	ledger_acc_full_name_translit  VARCHAR(1),
	is_revaluation 			VARCHAR(1),
	is_correct 				VARCHAR(1),
	PRIMARY KEY (ledger_account, start_date)
);