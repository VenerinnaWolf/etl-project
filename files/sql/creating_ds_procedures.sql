-- Процедура заполнения витрины оборотов по лицевым счетам
CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f(i_OnDate date)
AS $$
DECLARE
	_log_start	TIMESTAMP;
	_log_end 		TIMESTAMP;
	_duration 	INTERVAL;
BEGIN
	_log_start = clock_timestamp();

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

	-- Логирование
	_log_end = clock_timestamp();
	_duration = _log_end - _log_start;

	INSERT INTO logs.procedures_logs (procedure_name, end_time, start_time, duration)
	VALUES ('ds.fill_account_turnover_f(' || i_OnDate|| ')', _log_end, _log_start, _duration);
END
$$ LANGUAGE PLPGSQL;


-- Процедура заполнения витрины остатков по лицевым счетам
CREATE OR REPLACE PROCEDURE ds.fill_account_balance_f(i_OnDate date)
AS $$
DECLARE
	_log_start	TIMESTAMP;
	_log_end 		TIMESTAMP;
	_duration 	INTERVAL;
BEGIN
	_log_start = clock_timestamp();

	-- для возможности перезапускать расчет много раз за одни и те же даты, удаляем записи за дату расчета
	DELETE FROM dm.dm_account_balance_f WHERE on_date = i_OnDate;

	WITH actual_accounts AS (
		SELECT mad.account_rk, mad.char_type
		FROM ds.md_account_d mad
		WHERE i_OnDate BETWEEN mad.data_actual_date AND mad.data_actual_end_date
	),
	cur_balances AS (
		SELECT *
		FROM dm.dm_account_turnover_f datf
		WHERE datf.on_date = i_OnDate
	)
	INSERT INTO dm.dm_account_balance_f
	SELECT
		i_OnDate AS on_date,
		prev.account_rk,
		prev.currency_rk,
		CASE
			WHEN aa.char_type = 'А'
				THEN COALESCE(prev.balance_out, 0) + COALESCE(cb.debet_amount, 0) - COALESCE(cb.credit_amount, 0)
			WHEN aa.char_type = 'П'
				THEN COALESCE(prev.balance_out, 0) - COALESCE(cb.debet_amount, 0) + COALESCE(cb.credit_amount, 0)
		END AS balance_out,
		CASE
			WHEN aa.char_type = 'А'
				THEN COALESCE(prev.balance_out_rub, 0) + COALESCE(cb.debet_amount_rub, 0) - COALESCE(cb.credit_amount_rub, 0)
			WHEN aa.char_type = 'П'
				THEN COALESCE(prev.balance_out_rub, 0) - COALESCE(cb.debet_amount_rub, 0) + COALESCE(cb.credit_amount_rub, 0)
		END AS balance_out_rub
	FROM dm.dm_account_balance_f prev
		JOIN actual_accounts aa USING (account_rk)
		LEFT JOIN cur_balances cb USING (account_rk)
	WHERE prev.on_date = i_OnDate - interval '1 day';

	-- Логирование
	_log_end = clock_timestamp();
	_duration = _log_end - _log_start;

	INSERT INTO logs.procedures_logs (procedure_name, end_time, start_time, duration)
	VALUES ('ds.fill_account_balance_f(' || i_OnDate|| ')', _log_end, _log_start, _duration);
END
$$ LANGUAGE PLPGSQL;


-- Процедура заполнения витрины для формы 101
CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f(i_OnDate date)
AS $$
DECLARE
	_log_start	TIMESTAMP;
	_log_end 	TIMESTAMP;
	_duration 	INTERVAL;
	_from_date 	DATE;
	_to_date 	DATE;
BEGIN

	_log_start = clock_timestamp();
	_from_date = i_OnDate - interval '1 month';
	_to_date = i_OnDate - interval '1 day';

	-- для возможности перезапускать расчет много раз за одни и те же даты, удаляем записи за дату расчета
	DELETE FROM dm.dm_f101_round_f WHERE _from_date = from_date AND _to_date = to_date;

	WITH
		-- счета, действующие в отчетном периоде
		actual_accounts AS (
			SELECT
				mad.account_rk,
				mad.char_type,
				(LEFT(mad.account_number, 5))::int AS ledger_account
			FROM ds.md_account_d mad
			WHERE mad.data_actual_date <= _from_date AND _to_date <= mad.data_actual_end_date
		),
		-- рублевые счета (у которых код валюты равен 810 или 643)
		rub_accounts AS (
			SELECT *
			FROM ds.md_account_d mad
			WHERE mad.currency_code::int IN (810, 643)
		),
		-- витрина остатков за день, предшествующий первому дню отчетного периода
		bal_pre_first AS (
			SELECT *
			FROM dm.dm_account_balance_f dabf
			WHERE dabf.on_date = (_from_date - interval '1 day')
		),
		-- витрина остатков за последний день отчетного периода
		bal_last AS (
			SELECT *
			FROM dm.dm_account_balance_f
			WHERE on_date = _to_date
		),
		-- витрина оборотов за все дни отчетного периода
		turnovers AS (
			SELECT *
			FROM dm.dm_account_turnover_f datf
			WHERE datf.on_date BETWEEN _from_date AND _to_date
		)
	INSERT INTO dm.dm_f101_round_f(
		from_date,
		to_date,
		chapter,
		ledger_account,
		characteristic,
		balance_in_rub,
		balance_in_val,
		balance_in_total,
		balance_out_rub,
		balance_out_val,
		balance_out_total,
		turn_deb_rub,
		turn_deb_val,
		turn_deb_total,
		turn_cre_rub,
		turn_cre_val,
		turn_cre_total
	)
	SELECT
		_from_date,
		_to_date,
		mlas.chapter,
		aa.ledger_account,
		aa.char_type AS characteristic,
	--	суммы остатков за день, предшествующий первому дню отчетного периода
		SUM(CASE  -- если счет рублевый, то прибавляем баланс к сумме, иначе нет
                WHEN bf.account_rk IN (SELECT account_rk FROM rub_accounts)
                    THEN bf.balance_out_rub
                ELSE 0
            END) AS balance_in_rub,
		SUM(CASE  -- наоборот, если счет НЕ рублевый, прибавляем к сумме, иначе нет
                WHEN bf.account_rk NOT IN (SELECT account_rk FROM rub_accounts)
                    THEN bf.balance_out_rub
                ELSE 0
            END) AS balance_in_val,
		SUM(bf.balance_out_rub) AS balance_in_total, -- сумма для всех счетов (рублевых и валютных)
	--	то же самое, но за последний день отчетного периода
		SUM(CASE
                WHEN bl.account_rk IN (SELECT account_rk FROM rub_accounts)
                    THEN bl.balance_out_rub
                ELSE 0
            END) AS balance_out_rub,
		SUM(CASE
                WHEN bl.account_rk NOT IN (SELECT account_rk FROM rub_accounts)
                    THEN bl.balance_out_rub
                ELSE 0
            END) AS balance_out_val,
		SUM(bl.balance_out_rub) AS balance_out_total,
	--	то же самое, но для ДЕБЕТОВ в витрине turnover (DM_ACCOUNT_TURNOVER_F)
		SUM(CASE
                WHEN t.account_rk IN (SELECT account_rk FROM rub_accounts)
                    THEN t.debet_amount_rub
                ELSE 0
            END) AS turn_deb_rub,
		SUM(CASE
                WHEN t.account_rk NOT IN (SELECT account_rk FROM rub_accounts)
                    THEN t.debet_amount_rub
                ELSE 0
            END) AS turn_deb_val,
		SUM(t.debet_amount_rub) AS turn_deb_total,
	--	то же самое, но для КРЕДИТОВ в витрине turnover (DM_ACCOUNT_TURNOVER_F)
		SUM(CASE
                WHEN t.account_rk IN (SELECT account_rk FROM rub_accounts)
                    THEN t.credit_amount_rub
                ELSE 0
            END) AS turn_cre_rub,
		SUM(CASE
                WHEN t.account_rk NOT IN (SELECT account_rk FROM rub_accounts)
                    THEN t.credit_amount_rub
                ELSE 0
            END) AS turn_cre_val,
		SUM(t.credit_amount_rub) AS turn_cre_total
	FROM actual_accounts aa  -- актуальные счета
		JOIN ds.md_ledger_account_s mlas USING (ledger_account)  -- справочник балансовых счетов
		JOIN bal_pre_first bf USING (account_rk)  -- витрина остатков за день, предшествующий первому дню отчетного периода
		JOIN bal_last bl USING (account_rk)  -- витрина остатков за последний день отчетного периода
		LEFT JOIN turnovers t USING (account_rk)  -- витрина оборотов за все дни отчетного периода
	GROUP BY ledger_account,  -- группировка по балансовым счетам второго порядка
		_from_date,
		_to_date,
		mlas.chapter,
		aa.char_type;

	-- Логирование
	_log_end = clock_timestamp();
	_duration = _log_end - _log_start;

	INSERT INTO logs.procedures_logs (procedure_name, end_time, start_time, duration)
	VALUES ('ds.fill_f101_round_f(' || i_OnDate|| ')', _log_end, _log_start, _duration);

END
$$ LANGUAGE PLPGSQL;