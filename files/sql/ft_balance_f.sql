INSERT INTO ds.ft_balance_f(
	account_rk,
	currency_rk,
	balance_out,
	on_date
)
SELECT
	fbf."ACCOUNT_RK",
	fbf."CURRENCY_RK",
	fbf."BALANCE_OUT",
	to_date(fbf."ON_DATE", 'dd.mm.YYYY') AS on_date
FROM stage.ft_balance_f fbf
WHERE fbf."ACCOUNT_RK" IS NOT NULL
	AND fbf."ON_DATE" IS NOT NULL
ON CONFLICT ON CONSTRAINT ft_balance_f_pkey DO UPDATE
	SET currency_rk = excluded.currency_rk,
      balance_out = excluded.balance_out;