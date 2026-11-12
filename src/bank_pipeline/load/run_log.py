"""Saves every pipeline run (status, counts, duration, error) in etl.pipeline_run."""


def start_run(engine):
    with engine.begin() as conn:
        # A run that is still 'running' here was interrupted (crash, reboot):
        # the lock guarantees that no other run is active right now.
        conn.exec_driver_sql(
            "UPDATE etl.pipeline_run SET status = 'failed', finished_at = SYSDATETIME(), "
            "error_message = 'Interrupted - the process stopped before the run finished' "
            "WHERE status = 'running'"
        )
        return conn.exec_driver_sql(
            "INSERT INTO etl.pipeline_run (status) OUTPUT INSERTED.run_id VALUES ('running')"
        ).scalar()


def finish_run(engine, run_id, status, stats, duration, error=None):
    with engine.begin() as conn:
        conn.exec_driver_sql(
            """
            UPDATE etl.pipeline_run
            SET status = ?, finished_at = SYSDATETIME(), source = ?, source_location = ?,
                used_fallback = ?, rows_extracted = ?, rows_valid = ?, rows_rejected = ?,
                rows_inserted = ?, rows_updated = ?, duration_seconds = ?, error_message = ?
            WHERE run_id = ?
            """,
            (
                status,
                stats.get("source"),
                stats.get("source_location"),
                int(stats.get("used_fallback", False)),
                stats.get("extracted"),
                stats.get("valid"),
                stats.get("rejected"),
                stats.get("inserted"),
                stats.get("updated"),
                round(duration, 2),
                (error or "")[:2000] or None,
                run_id,
            ),
        )
