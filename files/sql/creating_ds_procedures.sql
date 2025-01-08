CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f(i_OnDate date)
LANGUAGE SQL AS $$

    -- для возможности перезапускать расчет много раз за одни и те же даты, удаляем записи за дату расчета
    DELETE FROM dm.dm_account_turnover_f WHERE on_date = i_OnDate;

    WITH actual_exchange_rates AS (
        SELECT merd.reduced_cource, merd.currency_rk
        FROM ds.md_exchange_rate_d merd
        WHERE i_OnDate BETWEEN merd.data_actual_date AND merd.data_actual_end_date
    ),
    cred_accounts AS (
        SELECT
            fpf.credit_account_rk AS account_rk,
            SUM(fpf.credit_amount) AS credit_amount
        FROM ds.ft_posting_f fpf
        WHERE fpf.oper_date = i_OnDate
        GROUP BY fpf.oper_date, account_rk
    ),
    deb_accounts AS (
        SELECT
            fpf.debet_account_rk AS account_rk,
            SUM(fpf.debet_amount) AS debet_amount
        FROM ds.ft_posting_f fpf
        WHERE fpf.oper_date = i_OnDate
        GROUP BY fpf.oper_date, account_rk
    )
    INSERT INTO dm.dm_account_turnover_f (
        on_date,
        account_rk,
        credit_amount,
        credit_amount_rub,
        debet_amount,
        debet_amount_rub
    )
    SELECT
        i_OnDate AS on_date,
        account_rk,
        c.credit_amount,
        c.credit_amount * COALESCE(aer.reduced_cource, 1) AS credit_amount_rub,
        d.debet_amount,
        d.debet_amount * COALESCE(aer.reduced_cource, 1) AS debet_amount_rub
    FROM cred_accounts c
    FULL JOIN deb_accounts d USING (account_rk)
    LEFT JOIN ds.ft_balance_f fbf USING (account_rk)
    LEFT JOIN actual_exchange_rates aer USING (currency_rk);

$$;