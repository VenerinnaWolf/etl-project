TRUNCATE TABLE dsl.md_ledger_account_s;

INSERT INTO dsl.md_ledger_account_s(
	chapter,
	chapter_name,
	section_number,
	section_name,
	subsection_name,
	ledger1_account,
	ledger1_account_name,
	ledger_account,
	ledger_account_name,
	characteristic,
	start_date,
	end_date
)
SELECT 
	mlas."CHAPTER",
	mlas."CHAPTER_NAME",
	mlas."SECTION_NUMBER",
	mlas."SECTION_NAME",
	mlas."SUBSECTION_NAME",
	mlas."LEDGER1_ACCOUNT",
	mlas."LEDGER1_ACCOUNT_NAME",
	mlas."LEDGER_ACCOUNT",
	mlas."LEDGER_ACCOUNT_NAME",
	mlas."CHARACTERISTIC",
	to_date(mlas."START_DATE", 'YYYY-mm-dd') AS start_date,
	to_date(mlas."END_DATE", 'YYYY-mm-dd') AS end_date
FROM stage.md_ledger_account_s mlas
WHERE mlas."LEDGER1_ACCOUNT" IS NOT NULL
	AND mlas."START_DATE" IS NOT NULL;
