-- Задание 2.3

-- 1)
-- Подготовить запрос, который определит корректное значение поля account_in_sum. 
-- Если значения полей account_in_sum одного дня и account_out_sum предыдущего дня отличаются, 
-- то корректным выбирается значение account_out_sum предыдущего дня.

-- Сначала я придумала вот такую реализацию, но она выполнялась 16 секунд.
DO
$$
DECLARE
  rec RECORD;
	prev_date DATE;
	cur_date DATE;
BEGIN
	-- Запускаем цикл для каждой записи из витрины dm.account_balance_turnover, отсортированной по account_rk, effective_date
	FOR rec IN 
		SELECT *
		FROM dm.account_balance_turnover abt
		ORDER BY abt.account_rk, abt.effective_date
	LOOP
		cur_date = rec.effective_date;  -- текущая дата (effective_date для записи на этой итерации)
		prev_date = (cur_date - interval '1 day')::date;  -- предыдущая дата
		
	  -- Вместо SELECT используем PERFORM. Ищем запись для текущего account_rk для предыдущей даты (effective_date = prev_date)
		PERFORM 1 FROM dm.account_balance_turnover 
		WHERE account_rk = rec.account_rk
			AND effective_date = prev_date;
		
		-- Если нет записей за предыдущую дату, то не делаем ничего
		IF FOUND THEN
			--RAISE NOTICE '----------- Previous date found -----------';
			UPDATE dm.account_balance_turnover cur
			SET account_in_sum = prev.account_out_sum
			FROM dm.account_balance_turnover prev
			WHERE cur.account_rk = rec.account_rk
				AND prev.account_rk = rec.account_rk
				AND cur.effective_date = cur_date
				AND prev.effective_date = prev_date;
		END IF;
--		RAISE NOTICE '%: cur_date: %, prev_date: %, account_in_sum: %, account_out_sum: %', rec.account_rk, cur_date, prev_date, rec.account_in_sum, rec.account_out_sum;
	END LOOP;
END
$$ LANGUAGE plpgsql;

-- После этого я сделала другой такой запрос, на основе этого, который делает то же самое, только уже за 0,02 секунды :)

-- Этим запросом можно посмотреть, для каких записей значения полей account_in_sum одного дня и account_out_sum предыдущего дня отличаются
SELECT 
	cur.account_rk ,
	cur.effective_date AS cur_date,
	prev.effective_date AS prev_date,
	cur.account_in_sum ,
	prev.account_out_sum 
FROM dm.account_balance_turnover cur
JOIN dm.account_balance_turnover prev 
	ON cur.account_rk = prev.account_rk
	AND cur.effective_date = prev.effective_date + interval '1 day'
WHERE cur.account_in_sum != prev.account_out_sum ;

-- Этим запросом можно посмотреть на всю таблицу
SELECT *
FROM dm.account_balance_turnover abt
ORDER BY abt.account_rk, abt.effective_date;

-- --------
-- Сам запрос, требующийся по заданию

BEGIN;

UPDATE dm.account_balance_turnover cur
SET account_in_sum = prev.account_out_sum
FROM dm.account_balance_turnover prev
WHERE cur.account_rk = prev.account_rk
	AND cur.effective_date = prev.effective_date + interval '1 day';

-- --------

-- Запросы для проверки
-- После выполнения запроса больше нет строк, для которых данные не совпадают
SELECT 
	cur.account_rk ,
	cur.effective_date AS cur_date,
	prev.effective_date AS prev_date,
	cur.account_in_sum ,
	prev.account_out_sum 
FROM dm.account_balance_turnover cur
JOIN dm.account_balance_turnover prev 
	ON cur.account_rk = prev.account_rk
	AND cur.effective_date = prev.effective_date + interval '1 day'
WHERE cur.account_in_sum != prev.account_out_sum ;

SELECT *
FROM dm.account_balance_turnover abt
ORDER BY abt.account_rk, abt.effective_date;

ROLLBACK;


-- --------
-- 2)
-- Подготовить такой же запрос, только проблема теперь в том, что account_in_sum одного дня правильная, 
-- а account_out_sum предыдущего дня некорректна. 
-- Это означает, что если эти значения отличаются, то корректным значением для account_out_sum предыдущего дня 
-- выбирается значение account_in_sum текущего дня.

BEGIN;

UPDATE dm.account_balance_turnover prev
SET account_out_sum = cur.account_in_sum
FROM dm.account_balance_turnover cur
WHERE cur.account_rk = prev.account_rk
	AND cur.effective_date = prev.effective_date + interval '1 day';

ROLLBACK;

-- --------
-- 3)
-- Подготовить запрос, который поправит данные в таблице rd.account_balance используя уже имеющийся запрос из п.1

BEGIN;

UPDATE rd.account_balance cur
SET account_in_sum = prev.account_out_sum
FROM rd.account_balance prev
WHERE cur.account_rk = prev.account_rk
	AND cur.effective_date = prev.effective_date + interval '1 day';

-- Запросы для проверки

SELECT 
	cur.account_rk ,
	cur.effective_date AS cur_date,
	prev.effective_date AS prev_date,
	cur.account_in_sum ,
	prev.account_out_sum 
FROM rd.account_balance cur
JOIN rd.account_balance prev 
	ON cur.account_rk = prev.account_rk
	AND cur.effective_date = prev.effective_date + interval '1 day'
WHERE cur.account_in_sum != prev.account_out_sum ;

SELECT *
FROM rd.account_balance abt
ORDER BY account_rk, effective_date;

ROLLBACK;

-- --------
-- 4)
-- Написать процедуру по аналогии с заданием 2.2 для перезагрузки данных в витрину

-- Создание процедуры
CREATE OR REPLACE PROCEDURE rd.fill_account_balance_turnover()
LANGUAGE SQL AS $$
	TRUNCATE TABLE dm.account_balance_turnover;

	INSERT INTO dm.account_balance_turnover 
	SELECT a.account_rk,
		   COALESCE(dc.currency_name, '-1'::TEXT) AS currency_name,
		   a.department_rk,
		   ab.effective_date,
		   ab.account_in_sum,
		   ab.account_out_sum
	FROM rd.account a
	LEFT JOIN rd.account_balance ab ON a.account_rk = ab.account_rk
	LEFT JOIN dm.dict_currency dc ON a.currency_cd = dc.currency_cd;
$$;

-- Вызов процедуры
CALL rd.fill_account_balance_turnover();

SELECT *
FROM dm.account_balance_turnover abt
ORDER BY abt.account_rk, abt.effective_date;

ROLLBACK;
