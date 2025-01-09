-- Заполнение витрины остатков по лицевым счетам DM.DM_ACCOUNT_BALANCE_F за 31.12.2017 данными из DS.FT_BALANCE_F
WITH actual_exchange_rates AS (
	SELECT merd.reduced_cource, merd.currency_rk
	FROM ds.md_exchange_rate_d merd
	WHERE '2017-12-31' BETWEEN merd.data_actual_date AND merd.data_actual_end_date
)
INSERT INTO dm.dm_account_balance_f
SELECT
	fbf.on_date,
	fbf.account_rk,
	fbf.currency_rk,
	fbf.balance_out,
	fbf.balance_out * COALESCE(aer.reduced_cource, 1)
FROM ds.ft_balance_f fbf
LEFT JOIN actual_exchange_rates aer USING (currency_rk);

-- Заполнение витрины остатков по лицевым счетам DM.DM_ACCOUNT_BALANCE_F за каждый день января 2018 года:
DO
$$
BEGIN
	FOR i IN 1..31 LOOP
		CALL ds.fill_account_balance_f(('2018-01-' || i)::date);
	END LOOP;
END
$$ LANGUAGE plpgsql;