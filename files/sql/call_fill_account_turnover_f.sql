--рассчитайте витрину dm.dm_account_turnover_f за каждый день января 2018 года
BEGIN;

CALL ds.fill_account_turnover_f('2018-01-09');
SELECT * FROM dm.dm_account_turnover_f;
CALL ds.fill_account_turnover_f('2018-01-10');
SELECT * FROM dm.dm_account_turnover_f;

ROLLBACK;
--COMMIT;