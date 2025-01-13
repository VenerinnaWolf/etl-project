TRUNCATE TABLE dm.dm_f101_round_f_v2;

INSERT INTO dm.dm_f101_round_f_v2
SELECT
	to_date(f101.from_date, 'YYYY.mm.dd'),
	to_date(f101.to_date, 'YYYY.mm.dd'),
	f101.chapter,
	f101.ledger_account,
	f101.characteristic,
	f101.balance_in_rub,
	f101.r_balance_in_rub,
	f101.balance_in_val,
	f101.r_balance_in_val,
	f101.balance_in_total,
	f101.r_balance_in_total,
	f101.turn_deb_rub,
	f101.r_turn_deb_rub,
	f101.turn_deb_val,
	f101.r_turn_deb_val,
	f101.turn_deb_total,
	f101.r_turn_deb_total,
	f101.turn_cre_rub,
	f101.r_turn_cre_rub,
	f101.turn_cre_val,
	f101.r_turn_cre_val,
	f101.turn_cre_total,
	f101.r_turn_cre_total,
	f101.balance_out_rub,
	f101.r_balance_out_rub,
	f101.balance_out_val,
	f101.r_balance_out_val,
	f101.balance_out_total,
	f101.r_balance_out_total
FROM stage.dm_f101_round_f f101;