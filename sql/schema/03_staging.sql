-- Validated rows of the current run. Emptied with TRUNCATE TABLE at the
-- start of every run (no foreign keys point to it and it keeps no history).
IF OBJECT_ID('stg.transactions') IS NULL
CREATE TABLE stg.transactions (
    run_id             INT            NOT NULL,
    source_row_id      INT            NOT NULL,
    city_name          NVARCHAR(100)  NOT NULL,
    country_name       NVARCHAR(100)  NOT NULL,
    transaction_date   DATE           NOT NULL,
    card_type_name     NVARCHAR(50)   NOT NULL,
    expense_type_name  NVARCHAR(50)   NOT NULL,
    gender_code        CHAR(1)        NOT NULL,
    amount             DECIMAL(18, 2) NOT NULL,
    PRIMARY KEY (run_id, source_row_id)
);
