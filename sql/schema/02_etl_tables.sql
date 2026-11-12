-- One row per pipeline run: status, source, record counts and duration.
IF OBJECT_ID('etl.pipeline_run') IS NULL
CREATE TABLE etl.pipeline_run (
    run_id            INT IDENTITY(1, 1) PRIMARY KEY,
    started_at        DATETIME2(0)   NOT NULL DEFAULT SYSDATETIME(),
    finished_at       DATETIME2(0)   NULL,
    status            VARCHAR(10)    NOT NULL DEFAULT 'running'
                      CHECK (status IN ('running', 'success', 'failed')),
    source            VARCHAR(10)    NULL,      -- 'api' or 'file'
    source_location   NVARCHAR(300)  NULL,      -- URL or file path (never the token)
    used_fallback     BIT            NOT NULL DEFAULT 0,
    rows_extracted    INT            NULL,
    rows_valid        INT            NULL,
    rows_rejected     INT            NULL,
    rows_inserted     INT            NULL,
    rows_updated      INT            NULL,
    duration_seconds  DECIMAL(10, 2) NULL,
    error_message     NVARCHAR(2000) NULL
);
GO

-- Records that failed validation, kept for later analysis.
IF OBJECT_ID('etl.rejected_record') IS NULL
CREATE TABLE etl.rejected_record (
    rejected_id    INT IDENTITY(1, 1) PRIMARY KEY,
    run_id         INT            NOT NULL REFERENCES etl.pipeline_run (run_id),
    source_row_id  NVARCHAR(50)   NULL,
    reasons        NVARCHAR(300)  NOT NULL,
    raw_data       NVARCHAR(MAX)  NOT NULL     -- original values as JSON
);
