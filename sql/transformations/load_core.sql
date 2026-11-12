-- Moves the validated rows of one run from staging into the normalized core tables.
--
-- Executed by bank_pipeline/load/core.py: every block (separated by GO) runs in
-- order inside ONE transaction, so an error in any block rolls back all of them.
-- The "-- step:" name is used to log how many rows each block changed.
--
-- Safe to run again with the same data: lookup values are only added when they
-- are missing, and transactions are matched on source_row_id, so a repeated run
-- inserts 0 and updates 0 rows. Rows are never deleted (history is kept).

-- step: new_countries
INSERT INTO core.country (country_name)
SELECT DISTINCT s.country_name
FROM stg.transactions s
WHERE s.run_id = :run_id
  AND NOT EXISTS (SELECT 1 FROM core.country c WHERE c.country_name = s.country_name);
GO

-- step: new_cities
INSERT INTO core.city (city_name, country_id)
SELECT DISTINCT s.city_name, c.country_id
FROM stg.transactions s
JOIN core.country c ON c.country_name = s.country_name
WHERE s.run_id = :run_id
  AND NOT EXISTS (SELECT 1 FROM core.city ci
                  WHERE ci.city_name = s.city_name AND ci.country_id = c.country_id);
GO

-- step: new_card_types
INSERT INTO core.card_type (card_type_name)
SELECT DISTINCT s.card_type_name
FROM stg.transactions s
WHERE s.run_id = :run_id
  AND NOT EXISTS (SELECT 1 FROM core.card_type t WHERE t.card_type_name = s.card_type_name);
GO

-- step: new_expense_types
INSERT INTO core.expense_type (expense_type_name)
SELECT DISTINCT s.expense_type_name
FROM stg.transactions s
WHERE s.run_id = :run_id
  AND NOT EXISTS (SELECT 1 FROM core.expense_type t WHERE t.expense_type_name = s.expense_type_name);
GO

-- step: updated
-- Existing transactions whose values changed in the source.
UPDATE t
SET t.city_id = ci.city_id,
    t.transaction_date = s.transaction_date,
    t.card_type_id = ct.card_type_id,
    t.expense_type_id = et.expense_type_id,
    t.gender_code = s.gender_code,
    t.amount = s.amount,
    t.updated_run_id = :run_id
FROM core.transactions t
JOIN stg.transactions s    ON s.source_row_id = t.source_row_id AND s.run_id = :run_id
JOIN core.country co       ON co.country_name = s.country_name
JOIN core.city ci          ON ci.city_name = s.city_name AND ci.country_id = co.country_id
JOIN core.card_type ct     ON ct.card_type_name = s.card_type_name
JOIN core.expense_type et  ON et.expense_type_name = s.expense_type_name
WHERE t.city_id <> ci.city_id
   OR t.transaction_date <> s.transaction_date
   OR t.card_type_id <> ct.card_type_id
   OR t.expense_type_id <> et.expense_type_id
   OR t.gender_code <> s.gender_code
   OR t.amount <> s.amount;
GO

-- step: inserted
-- Transactions that are not in the core table yet.
INSERT INTO core.transactions
    (source_row_id, city_id, transaction_date, card_type_id, expense_type_id, gender_code, amount, loaded_run_id)
SELECT s.source_row_id, ci.city_id, s.transaction_date, ct.card_type_id, et.expense_type_id,
       s.gender_code, s.amount, :run_id
FROM stg.transactions s
JOIN core.country co       ON co.country_name = s.country_name
JOIN core.city ci          ON ci.city_name = s.city_name AND ci.country_id = co.country_id
JOIN core.card_type ct     ON ct.card_type_name = s.card_type_name
JOIN core.expense_type et  ON et.expense_type_name = s.expense_type_name
WHERE s.run_id = :run_id
  AND NOT EXISTS (SELECT 1 FROM core.transactions t WHERE t.source_row_id = s.source_row_id);
