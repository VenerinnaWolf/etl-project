TRUNCATE TABLE dsl.md_currency_d;

INSERT INTO dsl.md_currency_d(
	currency_rk,
	data_actual_date,
	data_actual_end_date,
	currency_code,
	code_iso_char
)
SELECT 
	mcd."CURRENCY_RK",
	to_date(mcd."DATA_ACTUAL_DATE", 'YYYY-mm-dd') AS data_actual_date,
	to_date(mcd."DATA_ACTUAL_END_DATE", 'YYYY-mm-dd') AS data_actual_end_date,
	mcd."CURRENCY_CODE",
	mcd."CODE_ISO_CHAR"
FROM stage.md_currency_d mcd
WHERE mcd."DATA_ACTUAL_DATE" IS NOT NULL
	AND mcd."CURRENCY_RK" IS NOT NULL;