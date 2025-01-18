-- Задание 2.1

-- Количество строк в таблице dm.client (20147)
SELECT count(*) FROM dm.client; 

-- Этим запросом можно увидеть дубликаты
SELECT * FROM dm.client
ORDER BY client_rk, effective_from_date;

-- Количество строк с неповторяющимися данными в таблице dm.client (10019)
-- ctid - уникальный идентификатор строки в PostgreSQL
SELECT COUNT(*) FROM dm.client
WHERE ctid IN (
  SELECT MIN(ctid)
  FROM dm.client
  GROUP BY client_rk, effective_from_date
);

-- Удаление дубликатов
BEGIN;

DELETE FROM dm.client
WHERE ctid NOT IN (
  SELECT MIN(ctid)
  FROM dm.client
  GROUP BY client_rk, effective_from_date
);

SELECT count(*) FROM dm.client; -- (10019)

SELECT * FROM dm.client
ORDER BY client_rk, effective_from_date;

ROLLBACK;
COMMIT;
