-- Normalized model (3NF). See docs/database-schema.md for the explanation.
--
--   country 1---* city 1---* transaction *---1 card_type
--                                 |  *---1 expense_type
--                                 *---1 gender

IF OBJECT_ID('core.country') IS NULL
CREATE TABLE core.country (
    country_id    INT IDENTITY(1, 1) PRIMARY KEY,
    country_name  NVARCHAR(100) NOT NULL UNIQUE
);
GO

-- city -> country is stored here once, instead of repeating the country on
-- every transaction (this removes the transitive dependency, 3NF).
IF OBJECT_ID('core.city') IS NULL
CREATE TABLE core.city (
    city_id     INT IDENTITY(1, 1) PRIMARY KEY,
    city_name   NVARCHAR(100) NOT NULL,
    country_id  INT NOT NULL REFERENCES core.country (country_id),
    CONSTRAINT uq_city UNIQUE (city_name, country_id)
);
GO

IF OBJECT_ID('core.card_type') IS NULL
CREATE TABLE core.card_type (
    card_type_id    INT IDENTITY(1, 1) PRIMARY KEY,
    card_type_name  NVARCHAR(50) NOT NULL UNIQUE
);
GO

IF OBJECT_ID('core.expense_type') IS NULL
CREATE TABLE core.expense_type (
    expense_type_id    INT IDENTITY(1, 1) PRIMARY KEY,
    expense_type_name  NVARCHAR(50) NOT NULL UNIQUE
);
GO

IF OBJECT_ID('core.gender') IS NULL
CREATE TABLE core.gender (
    gender_code  CHAR(1)      PRIMARY KEY CHECK (gender_code IN ('F', 'M')),
    gender_name  NVARCHAR(20) NOT NULL
);
GO

IF NOT EXISTS (SELECT 1 FROM core.gender)
INSERT INTO core.gender (gender_code, gender_name) VALUES ('F', 'Female'), ('M', 'Male');
GO

IF OBJECT_ID('core.transactions') IS NULL
CREATE TABLE core.transactions (
    transaction_id    INT IDENTITY(1, 1) PRIMARY KEY,
    source_row_id     INT            NOT NULL UNIQUE,   -- "index" from the source, used for upserts
    city_id           INT            NOT NULL REFERENCES core.city (city_id),
    transaction_date  DATE           NOT NULL,
    card_type_id      INT            NOT NULL REFERENCES core.card_type (card_type_id),
    expense_type_id   INT            NOT NULL REFERENCES core.expense_type (expense_type_id),
    gender_code       CHAR(1)        NOT NULL REFERENCES core.gender (gender_code),
    amount            DECIMAL(18, 2) NOT NULL CHECK (amount > 0),
    loaded_run_id     INT            NOT NULL,          -- run that inserted the row
    updated_run_id    INT            NULL               -- last run that changed the row
);
GO

-- Indexes on foreign keys used in joins (SQL Server does not create them automatically).
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_transactions_city')
    CREATE INDEX ix_transactions_city ON core.transactions (city_id);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_transactions_date')
    CREATE INDEX ix_transactions_date ON core.transactions (transaction_date);
