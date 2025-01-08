INSERT INTO ds.md_exchange_rate_d(
	data_actual_date,
	data_actual_end_date,
	currency_rk,
	reduced_cource,
	code_iso_num
)
SELECT DISTINCT
	to_date(merd."DATA_ACTUAL_DATE", 'YYYY-mm-dd') AS data_actual_date,
	to_date(merd."DATA_ACTUAL_END_DATE", 'YYYY-mm-dd') AS data_actual_end_date,
	merd."CURRENCY_RK",
	merd."REDUCED_COURCE",
	merd."CODE_ISO_NUM"
FROM stage.md_exchange_rate_d merd
WHERE merd."DATA_ACTUAL_DATE" IS NOT NULL
	AND merd."CURRENCY_RK" IS NOT NULL
ON CONFLICT ON CONSTRAINT md_exchange_rate_d_pkey DO UPDATE
	SET currency_rk = excluded.currency_rk,
        reduced_cource = excluded.reduced_cource,
        code_iso_num = excluded.code_iso_num;