-- Reporting views (re-created on every setup).

-- City distribution with cumulative percentage (how many cities make up X % of all transactions).
CREATE OR ALTER VIEW mart.v_city_distribution AS
SELECT rank_by_count,
       city_name,
       country_name,
       transaction_count,
       percent_of_transactions,
       SUM(percent_of_transactions) OVER (ORDER BY rank_by_count, city_key
                                          ROWS UNBOUNDED PRECEDING) AS cumulative_percent,
       total_amount,
       percent_of_amount,
       avg_amount,
       SUM(transaction_count) OVER () AS all_transactions
FROM mart.agg_city_summary;
GO

-- One row with totals for the whole dataset.
CREATE OR ALTER VIEW mart.v_overview AS
SELECT COUNT(*)                     AS total_transactions,
       SUM(f.amount)                AS total_amount,
       CAST(AVG(f.amount) AS DECIMAL(18, 2)) AS avg_amount,
       COUNT(DISTINCT f.city_key)   AS cities,
       MIN(d.full_date)             AS first_date,
       MAX(d.full_date)             AS last_date
FROM mart.fact_transactions f
JOIN mart.dim_date d ON d.date_key = f.date_key;
GO

-- Expense mix per city with readable names.
CREATE OR ALTER VIEW mart.v_city_expense AS
SELECT c.city_name, c.country_name, e.expense_type_name,
       a.transaction_count, a.total_amount, a.percent_within_city
FROM mart.agg_city_expense a
JOIN mart.dim_city c ON c.city_key = a.city_key
JOIN mart.dim_expense_type e ON e.expense_type_key = a.expense_type_key;
