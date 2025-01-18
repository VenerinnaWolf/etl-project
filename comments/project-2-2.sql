-- Задание 2-2

-- --------
-- Проводим аналитику
-- --------

-- Общее количество строк в витрине
SELECT COUNT(*) FROM dm.loan_holiday_info lhi ; -- 10002

-- Количество строк, в которых пустые значения для разных полей
SELECT COUNT(*) FROM dm.loan_holiday_info lhi 
--WHERE deal_rk IS NULL; 									-- 0
--WHERE effective_from_date IS NULL; 			-- 0
--WHERE effective_to_date IS NULL; 				-- 0
--WHERE agreement_rk IS NULL; 						-- 3500
--WHERE account_rk IS NULL; 							-- 3500
--WHERE client_rk IS NULL; 								-- 3500
--WHERE department_rk IS NULL; 						-- 3500
--WHERE product_rk IS NULL; 							-- 3500
--WHERE product_name IS NULL; 						-- 10002 (т.е. это поле везде пустое)
--WHERE deal_type_cd IS NULL; 						-- 3500
--WHERE deal_start_date IS NULL; 					-- 3500
--WHERE deal_name IS NULL; 								-- 3500
--WHERE deal_number IS NULL; 							-- 3500
--WHERE deal_sum IS NULL; 								-- 3500
--WHERE loan_holiday_type_cd IS NULL;			-- 0
--WHERE loan_holiday_start_date IS NULL; 	-- 0
--WHERE loan_holiday_finish_date IS NULL; -- 10002
--WHERE loan_holiday_fact_finish_date IS NULL; -- 10002
--WHERE loan_holiday_finish_flg IS NULL; 	-- 0
--WHERE loan_holiday_last_possible_date IS NULL; -- 0

-- Посмотрим, какие даты есть в каждой из таблиц:
SELECT DISTINCT effective_from_date, effective_to_date 
FROM dm.loan_holiday_info lhi ; -- 3 строки
--FROM rd.deal_info di ; 		-- 2 строки (нет сочетания 2023-03-15  2999-12-31)
--FROM rd.loan_holiday lh ; -- 3 строки (все сочетания)
--FROM rd.product p ; 			-- 1 строка (только сочетание 2023-03-15  2999-12-31)

-- Т.к. effective_to_date везде равна 2999-12-31, в дальнейшем я буду опускать сравнение с ней


-- --------
-- Теперь загрузим данные из csv файлов и проанализируем уже их
-- --------

CREATE SCHEMA IF NOT EXISTS stage;

-- Далее запускаем DAG

-- Анализируем загруженные данные

SELECT DISTINCT effective_from_date
FROM stage.deal_info;  			-- 1 строка (только сочетание 2023-03-15)
-- У loan_holiday csv файла нет
--FROM stage.product_info;  -- 3 строки (все сочетания)

-- Значит, нам нужно загрузить недостающие данные для таблиц deal_info и product

-- По таблице deal_info все понятно - просто добавляем данные к имеющимся, т.к. никаких конфликтов по датам нет

-- С таблицей product нужно проверить, какие данные есть по дате 2023-03-15 в stage и rd:
SELECT pi2.product_rk, pi2.product_name FROM stage.product_info pi2
WHERE effective_from_date = '2023-03-15'
EXCEPT
--INTERSECT 
SELECT p.product_rk, p.product_name FROM rd.product p 
WHERE effective_from_date = '2023-03-15';
-- 0 строк, значит в csv нет данных, которых нет в старой таблице по дате 2023-03-15

-- Значит в таблицу product нужно добавлять данные только по датам 2023-08-11	и 2023-01-01


-- --------
-- Скрипты для заполнения таблиц rd
-- --------

BEGIN;

SELECT count(*) FROM rd.deal_info di; -- 6500

INSERT INTO rd.deal_info 
SELECT 
	di.deal_rk ,
	di.deal_num ,
	di.deal_name ,
	di.deal_sum ,
	di.client_rk ,
	di.account_rk ,
	di.agreement_rk ,
	di.deal_start_date::date,
	di.department_rk ,
	di.product_rk ,
	di.deal_type_cd ,
	di.effective_from_date::date,
	di.effective_to_date::date
FROM stage.deal_info di;

SELECT count(*) FROM rd.deal_info di; -- 10000, т.е. добавили 3500 нужных строк

SELECT count(*) FROM rd.product p ; -- 3500

INSERT INTO rd.product 
SELECT 
	p.product_rk ,
	p.product_name ,
	p.effective_from_date::date,
	p.effective_to_date::date
FROM stage.product_info p
WHERE effective_from_date != '2023-03-15';

SELECT count(*) FROM rd.product p ; -- 10000, т.е. добавили 6500 нужных строк

ROLLBACK;

-- --------
-- Создаем процедуру заполнения витрины
-- --------

-- Перед созданием процедуры посмотрим разные варианты запроса

with deal as (
		select  deal_rk
		   ,deal_num --Номер сделки
		   ,deal_name --Наименование сделки
		   ,deal_sum --Сумма сделки
		   ,account_rk
		   ,client_rk --Ссылка на клиента
		   ,agreement_rk --Ссылка на договор
		   ,deal_start_date --Дата начала действия сделки
		   ,department_rk --Ссылка на отделение
		   ,product_rk -- Ссылка на продукт
		   ,deal_type_cd
		   ,effective_from_date
		   ,effective_to_date
		from rd.deal_info
	), 
loan_holiday as (
		select  deal_rk
		   ,loan_holiday_type_cd  --Ссылка на тип кредитных каникул
		   ,loan_holiday_start_date     --Дата начала кредитных каникул
		   ,loan_holiday_finish_date    --Дата окончания кредитных каникул
		   ,loan_holiday_fact_finish_date      --Дата окончания кредитных каникул фактическая
		   ,loan_holiday_finish_flg     --Признак прекращения кредитных каникул по инициативе заёмщика
		   ,loan_holiday_last_possible_date    --Последняя возможная дата кредитных каникул
		   ,effective_from_date
		   ,effective_to_date
		from rd.loan_holiday
	), product as (
		select product_rk
		  ,product_name
		  ,effective_from_date
		  ,effective_to_date
		from rd.product
	), holiday_info as (
		select   d.deal_rk
		        ,lh.effective_from_date
		        ,lh.effective_to_date
		        ,d.deal_num as deal_number --Номер сделки
			      ,lh.loan_holiday_type_cd  --Ссылка на тип кредитных каникул
		        ,lh.loan_holiday_start_date     --Дата начала кредитных каникул
		        ,lh.loan_holiday_finish_date    --Дата окончания кредитных каникул
		        ,lh.loan_holiday_fact_finish_date      --Дата окончания кредитных каникул фактическая
		        ,lh.loan_holiday_finish_flg     --Признак прекращения кредитных каникул по инициативе заёмщика
		        ,lh.loan_holiday_last_possible_date    --Последняя возможная дата кредитных каникул
		        ,d.deal_name --Наименование сделки
		        ,d.deal_sum --Сумма сделки
		        ,d.client_rk --Ссылка на контрагента
		        ,d.account_rk
		        ,d.agreement_rk --Ссылка на договор
		        ,d.deal_start_date --Дата начала действия сделки
		        ,d.department_rk --Ссылка на ГО/филиал
		        ,d.product_rk -- Ссылка на продукт
		        ,p.product_name -- Наименование продукта
		        ,d.deal_type_cd -- Наименование типа сделки
		from deal d
		FULL join loan_holiday lh on 1=1  -- 1=1 забыли убрать видимо?
		                             and d.deal_rk = lh.deal_rk
		                             and d.effective_from_date = lh.effective_from_date
		FULL join product p on p.product_rk = d.product_rk
							   and p.effective_from_date = d.effective_from_date
--							   AND d.deal_name = p.product_name  -- для исключения дубликатов, в которых одинаковый product_rk, но разный product_name
	)
SELECT
--	SELECT DISTINCT 	-- для исключения полных дубликатов
--	      COUNT(*)
        deal_rk
	      ,effective_from_date
	      ,effective_to_date
	      ,agreement_rk
	      ,account_rk
	      ,client_rk
	      ,department_rk
	      ,product_rk
	      ,product_name
	      ,deal_type_cd
	      ,deal_start_date
	      ,deal_name
	      ,deal_number
	      ,deal_sum
	      ,loan_holiday_type_cd
	      ,loan_holiday_start_date
	      ,loan_holiday_finish_date
	      ,loan_holiday_fact_finish_date
	      ,loan_holiday_finish_flg
	      ,loan_holiday_last_possible_date
FROM holiday_info;

-- Результаты по количеству строк: 
-- С условием "AND d.deal_name = p.product_name" и с "DISTINCT": 		10004
-- С условием "AND d.deal_name = p.product_name" и без "DISTINCT": 	10012
-- Без условия "AND d.deal_name = p.product_name" и с "DISTINCT": 	10032
-- Без условия "AND d.deal_name = p.product_name" и без "DISTINCT": 10040

-- Создаем процедуру

CREATE OR REPLACE PROCEDURE rd.fill_loan_holiday_info()
LANGUAGE SQL AS $$
	TRUNCATE TABLE dm.loan_holiday_info;

	with deal as (
		select  deal_rk
		   ,deal_num --Номер сделки
		   ,deal_name --Наименование сделки
		   ,deal_sum --Сумма сделки
		   ,account_rk
		   ,client_rk --Ссылка на клиента
		   ,agreement_rk --Ссылка на договор
		   ,deal_start_date --Дата начала действия сделки
		   ,department_rk --Ссылка на отделение
		   ,product_rk -- Ссылка на продукт
		   ,deal_type_cd
		   ,effective_from_date
		   ,effective_to_date
		from rd.deal_info
	), 
	loan_holiday as (
		select  deal_rk
		   ,loan_holiday_type_cd  --Ссылка на тип кредитных каникул
		   ,loan_holiday_start_date     --Дата начала кредитных каникул
		   ,loan_holiday_finish_date    --Дата окончания кредитных каникул
		   ,loan_holiday_fact_finish_date      --Дата окончания кредитных каникул фактическая
		   ,loan_holiday_finish_flg     --Признак прекращения кредитных каникул по инициативе заёмщика
		   ,loan_holiday_last_possible_date    --Последняя возможная дата кредитных каникул
		   ,effective_from_date
		   ,effective_to_date
		from rd.loan_holiday
	), 
	product as (
		select product_rk
		  ,product_name
		  ,effective_from_date
		  ,effective_to_date
		from rd.product
	), 
	holiday_info as (
		select   d.deal_rk
		        ,lh.effective_from_date
		        ,lh.effective_to_date
		        ,d.deal_num as deal_number --Номер сделки
			      ,lh.loan_holiday_type_cd  --Ссылка на тип кредитных каникул
		        ,lh.loan_holiday_start_date     --Дата начала кредитных каникул
		        ,lh.loan_holiday_finish_date    --Дата окончания кредитных каникул
		        ,lh.loan_holiday_fact_finish_date      --Дата окончания кредитных каникул фактическая
		        ,lh.loan_holiday_finish_flg     --Признак прекращения кредитных каникул по инициативе заёмщика
		        ,lh.loan_holiday_last_possible_date    --Последняя возможная дата кредитных каникул
		        ,d.deal_name --Наименование сделки
		        ,d.deal_sum --Сумма сделки
		        ,d.client_rk --Ссылка на контрагента
		        ,d.account_rk
		        ,d.agreement_rk --Ссылка на договор
		        ,d.deal_start_date --Дата начала действия сделки
		        ,d.department_rk --Ссылка на ГО/филиал
		        ,d.product_rk -- Ссылка на продукт
		        ,p.product_name -- Наименование продукта
		        ,d.deal_type_cd -- Наименование типа сделки
		from deal d
		FULL join loan_holiday lh on d.deal_rk = lh.deal_rk
		                          and d.effective_from_date = lh.effective_from_date
		FULL join product p on p.product_rk = d.product_rk
							          and p.effective_from_date = d.effective_from_date
	)
	INSERT INTO dm.loan_holiday_info
	SELECT DISTINCT 	-- для исключения полных дубликатов
        deal_rk
	      ,effective_from_date
	      ,effective_to_date
	      ,agreement_rk
	      ,account_rk
	      ,client_rk
	      ,department_rk
	      ,product_rk
	      ,product_name
	      ,deal_type_cd
	      ,deal_start_date
	      ,deal_name
	      ,deal_number
	      ,deal_sum
	      ,loan_holiday_type_cd
	      ,loan_holiday_start_date
	      ,loan_holiday_finish_date
	      ,loan_holiday_fact_finish_date
	      ,loan_holiday_finish_flg
	      ,loan_holiday_last_possible_date
	FROM holiday_info;
$$;

-- --------
-- Вызываем процедуру
-- --------

CALL rd.fill_loan_holiday_info();

-- Проводим аналитику для заполненной таблицы
-- Количество строк, в которых пустые значения для разных полей
SELECT COUNT(*) FROM dm.loan_holiday_info lhi  -- 10032 (было 10002)
--WHERE deal_rk IS NULL; 									-- 0 (было 0)
--WHERE effective_from_date IS NULL; 			-- 0 (было 0)
--WHERE effective_to_date IS NULL; 				-- 0 (было 0)
--WHERE agreement_rk IS NULL; 						-- 0 (было 3500)
--WHERE account_rk IS NULL; 							-- 0 (было 3500)
--WHERE client_rk IS NULL; 								-- 0 (было 3500)
--WHERE department_rk IS NULL; 						-- 0 (было 3500)
--WHERE product_rk IS NULL; 							-- 0 (было 3500)
--WHERE product_name IS NULL; 						-- 0 (было 10002)
--WHERE deal_type_cd IS NULL; 						-- 0 (было 3500)
--WHERE deal_start_date IS NULL; 					-- 0 (было 3500)
--WHERE deal_name IS NULL; 								-- 0 (было 3500)
--WHERE deal_number IS NULL; 							-- 0 (было 3500)
--WHERE deal_sum IS NULL; 								-- 0 (было 3500)
--WHERE loan_holiday_type_cd IS NULL;			-- 0
--WHERE loan_holiday_start_date IS NULL; 	-- 0
--WHERE loan_holiday_finish_date IS NULL; -- 10032 (было 10002)
--WHERE loan_holiday_fact_finish_date IS NULL; -- 10032 (было 10002)
--WHERE loan_holiday_finish_flg IS NULL; 	-- 0
--WHERE loan_holiday_last_possible_date IS NULL; -- 0

-- Итог:
-- Все пустые поля, кроме loan_holiday_finish_date и loan_holiday_fact_finish_date заполнились.
-- Те поля, которые остались пустыми, были из таблицы loan_holiday, для которой csv файл не был предоставлен.
