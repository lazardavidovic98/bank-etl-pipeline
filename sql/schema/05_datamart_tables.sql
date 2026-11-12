-- Data Mart (star schema). One fact row = one transaction.
-- Dimension keys are the same ids as in the core tables.

IF OBJECT_ID('mart.dim_date') IS NULL
CREATE TABLE mart.dim_date (
    date_key      INT PRIMARY KEY,          -- e.g. 20141029
    full_date     DATE        NOT NULL,
    [year]        SMALLINT    NOT NULL,
    [quarter]     TINYINT     NOT NULL,
    [month]       TINYINT     NOT NULL,
    month_name    VARCHAR(10) NOT NULL,
    day_of_week   VARCHAR(10) NOT NULL,
    is_weekend    BIT         NOT NULL
);
GO

-- Denormalized on purpose: the country name is copied into the city dimension
-- so reports need one join instead of two.
IF OBJECT_ID('mart.dim_city') IS NULL
CREATE TABLE mart.dim_city (
    city_key      INT PRIMARY KEY,
    city_name     NVARCHAR(100) NOT NULL,
    country_name  NVARCHAR(100) NOT NULL
);
GO

IF OBJECT_ID('mart.dim_card_type') IS NULL
CREATE TABLE mart.dim_card_type (
    card_type_key   INT PRIMARY KEY,
    card_type_name  NVARCHAR(50) NOT NULL
);
GO

IF OBJECT_ID('mart.dim_expense_type') IS NULL
CREATE TABLE mart.dim_expense_type (
    expense_type_key   INT PRIMARY KEY,
    expense_type_name  NVARCHAR(50) NOT NULL
);
GO

IF OBJECT_ID('mart.dim_gender') IS NULL
CREATE TABLE mart.dim_gender (
    gender_key   CHAR(1) PRIMARY KEY,
    gender_name  NVARCHAR(20) NOT NULL
);
GO

IF OBJECT_ID('mart.fact_transactions') IS NULL
CREATE TABLE mart.fact_transactions (
    transaction_key   INT PRIMARY KEY,
    date_key          INT     NOT NULL REFERENCES mart.dim_date (date_key),
    city_key          INT     NOT NULL REFERENCES mart.dim_city (city_key),
    card_type_key     INT     NOT NULL REFERENCES mart.dim_card_type (card_type_key),
    expense_type_key  INT     NOT NULL REFERENCES mart.dim_expense_type (expense_type_key),
    gender_key        CHAR(1) NOT NULL REFERENCES mart.dim_gender (gender_key),
    amount            DECIMAL(18, 2) NOT NULL
);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_fact_city')
    CREATE INDEX ix_fact_city ON mart.fact_transactions (city_key) INCLUDE (amount);
GO

-- Pre-aggregated distribution by City (the main report).
IF OBJECT_ID('mart.agg_city_summary') IS NULL
CREATE TABLE mart.agg_city_summary (
    city_key                  INT PRIMARY KEY,
    city_name                 NVARCHAR(100)  NOT NULL,
    country_name              NVARCHAR(100)  NOT NULL,
    transaction_count         INT            NOT NULL,
    percent_of_transactions   DECIMAL(7, 4)  NOT NULL,
    rank_by_count             INT            NOT NULL,
    total_amount              DECIMAL(18, 2) NOT NULL,
    percent_of_amount         DECIMAL(7, 4)  NOT NULL,
    rank_by_amount            INT            NOT NULL,
    avg_amount                DECIMAL(18, 2) NOT NULL,
    min_amount                DECIMAL(18, 2) NOT NULL,
    max_amount                DECIMAL(18, 2) NOT NULL,
    refreshed_at              DATETIME2(0)   NOT NULL
);
GO

-- Spending per City and expense type.
IF OBJECT_ID('mart.agg_city_expense') IS NULL
CREATE TABLE mart.agg_city_expense (
    city_key            INT NOT NULL,
    expense_type_key    INT NOT NULL,
    transaction_count   INT NOT NULL,
    total_amount        DECIMAL(18, 2) NOT NULL,
    percent_within_city DECIMAL(7, 4)  NOT NULL,
    PRIMARY KEY (city_key, expense_type_key)
);
