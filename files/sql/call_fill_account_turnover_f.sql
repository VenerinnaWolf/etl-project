-- Заполнение витрины оборотов по лицевым счетам dm.dm_account_turnover_f за каждый день января 2018 года:
DO
$$
BEGIN
	FOR i IN 1..31 LOOP
		CALL ds.fill_account_turnover_f(('2018-01-' || i)::date);
--		PERFORM pg_sleep(1);
	END LOOP;
END
$$ LANGUAGE plpgsql;