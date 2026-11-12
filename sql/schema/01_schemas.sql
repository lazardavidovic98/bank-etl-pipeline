-- Database schemas (namespaces) used by the pipeline:
--   etl  - pipeline run log and rejected records
--   stg  - staging table (validated rows of the current run)
--   core - normalized operational model (3NF)
--   mart - Data Mart for reporting (star schema)

IF SCHEMA_ID('etl') IS NULL EXEC('CREATE SCHEMA etl');
IF SCHEMA_ID('stg') IS NULL EXEC('CREATE SCHEMA stg');
IF SCHEMA_ID('core') IS NULL EXEC('CREATE SCHEMA core');
IF SCHEMA_ID('mart') IS NULL EXEC('CREATE SCHEMA mart');
