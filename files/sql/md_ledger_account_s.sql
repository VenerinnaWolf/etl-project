INSERT INTO ds.md_ledger_account_s(
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
WHERE mlas."LEDGER_ACCOUNT" IS NOT NULL
	AND mlas."START_DATE" IS NOT NULL
ON CONFLICT ON CONSTRAINT md_ledger_account_s_pkey DO UPDATE
	SET chapter = excluded.chapter,
        chapter_name = excluded.chapter_name,
        section_number = excluded.section_number,
        section_name = excluded.section_name,
        ledger1_account = excluded.ledger1_account,
        ledger1_account_name = excluded.ledger1_account_name,
        ledger_account_name = excluded.ledger_account_name,
        characteristic = excluded.characteristic,
        end_date = excluded.end_date;