INSERT INTO rd.product
SELECT
	p.product_rk ,
	p.product_name ,
	p.effective_from_date::date,
	p.effective_to_date::date
FROM stage.product_info p
WHERE effective_from_date != '2023-03-15';