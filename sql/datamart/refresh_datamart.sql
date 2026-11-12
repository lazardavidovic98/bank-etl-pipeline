-- Rebuilds the Data Mart from the core tables.
--
-- Executed by bank_pipeline/load/datamart.py in ONE transaction, after the core
-- load succeeded. Report users see either the old or the new mart, never a
-- half-refreshed one.
--
-- DELETE vs TRUNCATE:
--   * Dimension tables are referenced by foreign keys from the fact table, so
--     TRUNCATE is not allowed on them. They are updated with INSERT / UPDATE.
--   * The fact and aggregate tables are not referenced by any foreign key, have
--     no IDENTITY column and are fully rebuilt from core on every run, so
--     TRUNCATE is safe there and much faster than DELETE (it does not log every
--     deleted row). TRUNCATE inside a transaction is rolled back on error.

-- step: new_dates
SET LANGUAGE us_english;  -- DATENAME returns English month/day names
INSERT INTO mart.dim_date (date_key, full_date, [year], [quarter], [month], month_name, day_of_week, is_weekend)
SELECT YEAR(d) * 10000 + MONTH(d) * 100 + DAY(d),
       d,
       YEAR(d),
       DATEPART(QUARTER, d),
       MONTH(d),
       DATENAME(MONTH, d),
       DATENAME(WEEKDAY, d),
       CASE WHEN DATENAME(WEEKDAY, d) IN ('Saturday', 'Sunday') THEN 1 ELSE 0 END
FROM (SELECT DISTINCT transaction_date AS d FROM core.transactions) dates
WHERE NOT EXISTS (SELECT 1 FROM mart.dim_date dd WHERE dd.full_date = dates.d);
GO

-- step: updated_cities
UPDATE dc
SET dc.city_name = ci.city_name, dc.country_name = co.country_name
FROM mart.dim_city dc
JOIN core.city ci    ON ci.city_id = dc.city_key
JOIN core.country co ON co.country_id = ci.country_id
WHERE dc.city_name <> ci.city_name OR dc.country_name <> co.country_name;
GO

-- step: new_cities
INSERT INTO mart.dim_city (city_key, city_name, country_name)
SELECT ci.city_id, ci.city_name, co.country_name
FROM core.city ci
JOIN core.country co ON co.country_id = ci.country_id
WHERE NOT EXISTS (SELECT 1 FROM mart.dim_city dc WHERE dc.city_key = ci.city_id);
GO

-- step: new_card_types
INSERT INTO mart.dim_card_type (card_type_key, card_type_name)
SELECT card_type_id, card_type_name FROM core.card_type c
WHERE NOT EXISTS (SELECT 1 FROM mart.dim_card_type d WHERE d.card_type_key = c.card_type_id);
GO

-- step: new_expense_types
INSERT INTO mart.dim_expense_type (expense_type_key, expense_type_name)
SELECT expense_type_id, expense_type_name FROM core.expense_type e
WHERE NOT EXISTS (SELECT 1 FROM mart.dim_expense_type d WHERE d.expense_type_key = e.expense_type_id);
GO

-- step: new_genders
INSERT INTO mart.dim_gender (gender_key, gender_name)
SELECT gender_code, gender_name FROM core.gender g
WHERE NOT EXISTS (SELECT 1 FROM mart.dim_gender d WHERE d.gender_key = g.gender_code);
GO

-- step: clear_fact
TRUNCATE TABLE mart.fact_transactions;
GO

-- step: fact_rows
INSERT INTO mart.fact_transactions
    (transaction_key, date_key, city_key, card_type_key, expense_type_key, gender_key, amount)
SELECT transaction_id,
       YEAR(transaction_date) * 10000 + MONTH(transaction_date) * 100 + DAY(transaction_date),
       city_id, card_type_id, expense_type_id, gender_code, amount
FROM core.transactions;
GO

-- step: clear_city_summary
TRUNCATE TABLE mart.agg_city_summary;
GO

-- step: city_rows
INSERT INTO mart.agg_city_summary
    (city_key, city_name, country_name, transaction_count, percent_of_transactions, rank_by_count,
     total_amount, percent_of_amount, rank_by_amount, avg_amount, min_amount, max_amount, refreshed_at)
SELECT c.city_key,
       c.city_name,
       c.country_name,
       COUNT(*),
       CAST(100.0 * COUNT(*) / SUM(COUNT(*)) OVER () AS DECIMAL(7, 4)),
       RANK() OVER (ORDER BY COUNT(*) DESC),
       SUM(f.amount),
       CAST(100.0 * SUM(f.amount) / SUM(SUM(f.amount)) OVER () AS DECIMAL(7, 4)),
       RANK() OVER (ORDER BY SUM(f.amount) DESC),
       CAST(AVG(f.amount) AS DECIMAL(18, 2)),
       MIN(f.amount),
       MAX(f.amount),
       SYSDATETIME()
FROM mart.fact_transactions f
JOIN mart.dim_city c ON c.city_key = f.city_key
GROUP BY c.city_key, c.city_name, c.country_name;
GO

-- step: clear_city_expense
TRUNCATE TABLE mart.agg_city_expense;
GO

-- step: city_expense_rows
INSERT INTO mart.agg_city_expense (city_key, expense_type_key, transaction_count, total_amount, percent_within_city)
SELECT city_key,
       expense_type_key,
       COUNT(*),
       SUM(amount),
       CAST(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY city_key) AS DECIMAL(7, 4))
FROM mart.fact_transactions
GROUP BY city_key, expense_type_key;
