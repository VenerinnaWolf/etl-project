SELECT
	fpf.debet_account_rk,
	fpf.credit_account_rk,
	SUM(fpf.debet_amount) AS debet_sum,
	SUM(fpf.credit_amount) AS credit_sum
FROM dsl.ft_posting_f fpf 
WHERE fpf.oper_date = '2018-01-15'
GROUP BY fpf.debet_account_rk, fpf.credit_account_rk
ORDER BY debet_sum DESC, credit_sum DESC
LIMIT 10;