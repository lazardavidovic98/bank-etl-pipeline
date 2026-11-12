-- Example analysis queries on the Data Mart (run them in SSMS / Azure Data Studio / VS Code).

-- 1. Total number of transactions and overall numbers
SELECT * FROM mart.v_overview;

-- 2. Top 10 cities by number of transactions, with share and rank
SELECT TOP (10) rank_by_count, city_name, transaction_count, percent_of_transactions, cumulative_percent
FROM mart.v_city_distribution
ORDER BY rank_by_count;

-- 3. How many cities make up 80 % of all transactions?
SELECT COUNT(*) AS cities_for_80_percent
FROM mart.v_city_distribution
WHERE cumulative_percent - percent_of_transactions < 80;

-- 4. Top 10 cities by total amount spent
SELECT TOP (10) rank_by_amount, city_name, total_amount, percent_of_amount, avg_amount
FROM mart.agg_city_summary
ORDER BY rank_by_amount;

-- 5. Cities grouped by size (how many cities have 1, 2-10, 11-100, 100+ transactions)
SELECT CASE WHEN transaction_count = 1 THEN '1'
            WHEN transaction_count <= 10 THEN '2-10'
            WHEN transaction_count <= 100 THEN '11-100'
            ELSE '100+' END AS transactions_per_city,
       COUNT(*) AS cities,
       SUM(transaction_count) AS transactions
FROM mart.agg_city_summary
GROUP BY CASE WHEN transaction_count = 1 THEN '1'
              WHEN transaction_count <= 10 THEN '2-10'
              WHEN transaction_count <= 100 THEN '11-100'
              ELSE '100+' END
ORDER BY MIN(transaction_count);

-- 6. Expense mix in the 4 biggest cities
SELECT v.city_name, v.expense_type_name, v.transaction_count, v.percent_within_city
FROM mart.v_city_expense v
JOIN mart.agg_city_summary s ON s.city_name = v.city_name AND s.country_name = v.country_name
WHERE s.rank_by_count <= 4
ORDER BY s.rank_by_count, v.transaction_count DESC;

-- 7. Gender and card type split per city (straight from the star schema)
SELECT TOP (20) c.city_name, g.gender_name, ct.card_type_name,
       COUNT(*) AS transactions, SUM(f.amount) AS total_amount
FROM mart.fact_transactions f
JOIN mart.dim_city c       ON c.city_key = f.city_key
JOIN mart.dim_gender g     ON g.gender_key = f.gender_key
JOIN mart.dim_card_type ct ON ct.card_type_key = f.card_type_key
GROUP BY c.city_name, g.gender_name, ct.card_type_name
ORDER BY transactions DESC;

-- 8. Monthly trend for the biggest city
SELECT d.[year], d.[month], COUNT(*) AS transactions, SUM(f.amount) AS total_amount
FROM mart.fact_transactions f
JOIN mart.dim_date d ON d.date_key = f.date_key
WHERE f.city_key = (SELECT city_key FROM mart.agg_city_summary WHERE rank_by_count = 1)
GROUP BY d.[year], d.[month]
ORDER BY d.[year], d.[month];

-- 9. Last pipeline runs
SELECT TOP (10) run_id, started_at, status, source, used_fallback, rows_extracted,
       rows_valid, rows_rejected, rows_inserted, rows_updated, duration_seconds, error_message
FROM etl.pipeline_run
ORDER BY run_id DESC;
