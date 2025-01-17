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