INSERT INTO ds.md_currency_d(
	currency_rk,
	data_actual_date,
	data_actual_end_date,
	currency_code,
	code_iso_char
)
SELECT DISTINCT
	mcd."CURRENCY_RK",
	to_date(mcd."DATA_ACTUAL_DATE", 'YYYY-mm-dd') AS data_actual_date,
	to_date(mcd."DATA_ACTUAL_END_DATE", 'YYYY-mm-dd') AS data_actual_end_date,
	mcd."CURRENCY_CODE",
	mcd."CODE_ISO_CHAR"
FROM stage.md_currency_d mcd
WHERE mcd."DATA_ACTUAL_DATE" IS NOT NULL
	AND mcd."CURRENCY_RK" IS NOT NULL
ON CONFLICT ON CONSTRAINT md_currency_d_pkey DO UPDATE
	SET data_actual_end_date = excluded.data_actual_end_date,
        code_iso_char = excluded.code_iso_char,
        currency_code = excluded.currency_code;