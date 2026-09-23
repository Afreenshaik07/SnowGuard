# ============================================================
# SNOWGUARD PIPELINE ORCHESTRATOR
# WITH SNOWFLAKE EXECUTION LOGGING
# ============================================================

import os
import sys
import subprocess
from datetime import datetime
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv


# ------------------------------------------------------------
# ENVIRONMENT
# ------------------------------------------------------------

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[2]

PIPELINE_STEPS = [
    ("Data Quality", "app/quality/quality_engine.py"),
    ("Anomaly Detection", "app/anomaly/anomaly_detector.py"),
    ("Generate Drift Data", "app/drift/generate_drift_data.py"),
    ("Drift Detection", "app/drift/drift_detector.py"),
    ("Root Cause Analysis", "app/analytics/root_cause.py"),
    ("Monitoring Summary", "app/analytics/monitoring_summary.py"),
    ("Alert Engine", "app/analytics/alert_engine.py"),
]

DATABASE = "SNOWGUARD_DB"
SCHEMA = "ANALYTICS"


# ------------------------------------------------------------
# SNOWFLAKE LOGGING CONNECTION
# ------------------------------------------------------------

def create_connection():
    """Create a Snowflake connection using the existing .env settings."""
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=DATABASE,
        schema=SCHEMA,
        role=os.getenv("SNOWFLAKE_ROLE"),
    )


def setup_logging_tables():
    """Create pipeline logging tables when they do not already exist."""
    connection = create_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {DATABASE}.{SCHEMA}.PIPELINE_RUNS (
                RUN_ID VARCHAR(40) PRIMARY KEY,
                PIPELINE_NAME VARCHAR(100),
                STARTED_AT TIMESTAMP_NTZ,
                FINISHED_AT TIMESTAMP_NTZ,
                TOTAL_DURATION_SECONDS FLOAT,
                STATUS VARCHAR(20),
                SUCCESSFUL_STEPS NUMBER,
                TOTAL_STEPS NUMBER,
                ERROR_MESSAGE VARCHAR(5000)
            )
            """
        )

        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {DATABASE}.{SCHEMA}.PIPELINE_STEP_RUNS (
                STEP_RUN_ID NUMBER AUTOINCREMENT,
                RUN_ID VARCHAR(40),
                STEP_NUMBER NUMBER,
                STEP_NAME VARCHAR(100),
                SCRIPT VARCHAR(500),
                STARTED_AT TIMESTAMP_NTZ,
                FINISHED_AT TIMESTAMP_NTZ,
                DURATION_SECONDS FLOAT,
                STATUS VARCHAR(20),
                ERROR_MESSAGE VARCHAR(5000)
            )
            """
        )

        connection.commit()
    finally:
        cursor.close()
        connection.close()


def log_pipeline_start(run_id: str, started_at: datetime):
    """Insert a RUNNING record for the current pipeline execution."""
    connection = create_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            f"""
            INSERT INTO {DATABASE}.{SCHEMA}.PIPELINE_RUNS
            (
                RUN_ID,
                PIPELINE_NAME,
                STARTED_AT,
                STATUS,
                SUCCESSFUL_STEPS,
                TOTAL_STEPS
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                run_id,
                "SNOWGUARD FULL MONITORING PIPELINE",
                started_at,
                "RUNNING",
                0,
                len(PIPELINE_STEPS),
            ),
        )
        connection.commit()
    finally:
        cursor.close()
        connection.close()


def log_step_result(
    run_id: str,
    step_number: int,
    result: dict,
):
    """Insert a step execution record."""
    connection = create_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            f"""
            INSERT INTO {DATABASE}.{SCHEMA}.PIPELINE_STEP_RUNS
            (
                RUN_ID,
                STEP_NUMBER,
                STEP_NAME,
                SCRIPT,
                STARTED_AT,
                FINISHED_AT,
                DURATION_SECONDS,
                STATUS,
                ERROR_MESSAGE
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                run_id,
                step_number,
                result.get("name"),
                result.get("script"),
                _parse_dt(result.get("started_at")),
                _parse_dt(result.get("finished_at")),
                result.get("duration_seconds"),
                result.get("status"),
                result.get("error"),
            ),
        )
        connection.commit()
    finally:
        cursor.close()
        connection.close()


def log_pipeline_finish(
    run_id: str,
    finished_at: datetime,
    total_duration: float,
    status: str,
    successful_steps: int,
    error_message: str | None = None,
):
    """Update the pipeline execution record when the run finishes."""
    connection = create_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            f"""
            UPDATE {DATABASE}.{SCHEMA}.PIPELINE_RUNS
            SET
                FINISHED_AT = %s,
                TOTAL_DURATION_SECONDS = %s,
                STATUS = %s,
                SUCCESSFUL_STEPS = %s,
                ERROR_MESSAGE = %s
            WHERE RUN_ID = %s
            """,
            (
                finished_at,
                total_duration,
                status,
                successful_steps,
                error_message,
                run_id,
            ),
        )
        connection.commit()
    finally:
        cursor.close()
        connection.close()


def _parse_dt(value):
    if not value:
        return None
    return datetime.fromisoformat(value)


def safe_log(action, *args, **kwargs):
    """Log to Snowflake without allowing logging failures to stop monitoring."""
    try:
        action(*args, **kwargs)
        return True
    except Exception as error:
        print(f"WARNING: Snowflake pipeline logging failed: {error}", file=sys.stderr)
        return False


# ------------------------------------------------------------
# PIPELINE STEP EXECUTION
# ------------------------------------------------------------

def run_step(name: str, script: str) -> dict:
    """Run one SnowGuard pipeline stage and return execution details."""
    script_path = PROJECT_ROOT / script

    if not script_path.exists():
        raise FileNotFoundError(f"Pipeline script not found: {script_path}")

    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"

    started = datetime.now()

    print("=" * 70)
    print(f"RUNNING: {name}")
    print(f"SCRIPT : {script}")
    print(f"TIME   : {started:%Y-%m-%d %H:%M:%S}")
    print("=" * 70)

    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
    )

    finished = datetime.now()
    duration = (finished - started).total_seconds()

    if result.stdout.strip():
        print(result.stdout.strip())

    if result.returncode != 0:
        if result.stderr.strip():
            print(result.stderr.strip(), file=sys.stderr)

        raise RuntimeError(
            f"Step failed: {name} | return code={result.returncode}"
        )

    print(f"COMPLETED: {name} | {duration:.2f}s")
    print()

    return {
        "name": name,
        "script": script,
        "status": "SUCCESS",
        "started_at": started.isoformat(timespec="seconds"),
        "finished_at": finished.isoformat(timespec="seconds"),
        "duration_seconds": round(duration, 2),
    }


# ------------------------------------------------------------
# FULL PIPELINE
# ------------------------------------------------------------

def run_pipeline() -> list[dict]:
    """Run all SnowGuard stages in order and log the execution."""
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
    pipeline_started = datetime.now()

    print("\n" + "#" * 70)
    print("SNOWGUARD FULL MONITORING PIPELINE")
    print(f"RUN ID: {run_id}")
    print("#" * 70 + "\n")

    # Set up logging infrastructure before execution.
    safe_log(setup_logging_tables)
    safe_log(log_pipeline_start, run_id, pipeline_started)

    results = []
    successful_steps = 0

    for index, (name, script) in enumerate(PIPELINE_STEPS, start=1):
        try:
            result = run_step(name, script)
            results.append(result)
            successful_steps += 1
            safe_log(log_step_result, run_id, index, result)

        except Exception as error:
            failed_at = datetime.now()
            failed_result = {
                "name": name,
                "script": script,
                "status": "FAILED",
                "started_at": failed_at.isoformat(timespec="seconds"),
                "finished_at": failed_at.isoformat(timespec="seconds"),
                "duration_seconds": 0.0,
                "error": str(error),
            }
            results.append(failed_result)
            safe_log(log_step_result, run_id, index, failed_result)

            total_duration = (datetime.now() - pipeline_started).total_seconds()
            safe_log(
                log_pipeline_finish,
                run_id,
                datetime.now(),
                round(total_duration, 2),
                "FAILED",
                successful_steps,
                str(error),
            )

            print(f"\nPIPELINE STOPPED: {error}", file=sys.stderr)
            raise

    total_duration = (datetime.now() - pipeline_started).total_seconds()

    safe_log(
        log_pipeline_finish,
        run_id,
        datetime.now(),
        round(total_duration, 2),
        "SUCCESS",
        successful_steps,
        None,
    )

    print("#" * 70)
    print("SNOWGUARD PIPELINE COMPLETED SUCCESSFULLY")
    print(f"RUN ID : {run_id}")
    print(f"STEPS  : {successful_steps}/{len(PIPELINE_STEPS)}")
    print(f"TIME   : {total_duration:.2f}s")
    print("LOGS   : ANALYTICS.PIPELINE_RUNS + PIPELINE_STEP_RUNS")
    print("#" * 70)

    return results


# ------------------------------------------------------------
# ENTRY POINT
# ------------------------------------------------------------

if __name__ == "__main__":
    try:
        run_pipeline()
    except KeyboardInterrupt:
        print("\nPipeline interrupted by user.", file=sys.stderr)
        sys.exit(130)
    except Exception as error:
        print(f"\nPipeline failed: {error}", file=sys.stderr)
        sys.exit(1)
