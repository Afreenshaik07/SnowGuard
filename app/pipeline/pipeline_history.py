import os
import sys

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv


load_dotenv()

DATABASE = "SNOWGUARD_DB"
SCHEMA = "ANALYTICS"


def create_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=DATABASE,
        schema=SCHEMA,
        role=os.getenv("SNOWFLAKE_ROLE"),
    )


def query_to_dataframe(query):
    connection = create_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(query)

        rows = cursor.fetchall()

        columns = [
            column[0]
            for column in cursor.description
        ]

        return pd.DataFrame(
            rows,
            columns=columns,
        )

    finally:
        cursor.close()
        connection.close()


def load_pipeline_runs(limit=20):
    query = f"""
        SELECT
            RUN_ID,
            PIPELINE_NAME,
            STARTED_AT,
            FINISHED_AT,
            TOTAL_DURATION_SECONDS,
            STATUS,
            SUCCESSFUL_STEPS,
            TOTAL_STEPS,
            ERROR_MESSAGE
        FROM {DATABASE}.{SCHEMA}.PIPELINE_RUNS
        ORDER BY STARTED_AT DESC
        LIMIT {int(limit)}
    """

    return query_to_dataframe(query)


def load_step_runs(run_id, limit=100):
    safe_run_id = str(run_id).replace("'", "''")

    query = f"""
        SELECT
            STEP_RUN_ID,
            RUN_ID,
            STEP_NUMBER,
            STEP_NAME,
            SCRIPT,
            STARTED_AT,
            FINISHED_AT,
            DURATION_SECONDS,
            STATUS,
            ERROR_MESSAGE
        FROM {DATABASE}.{SCHEMA}.PIPELINE_STEP_RUNS
        WHERE RUN_ID = '{safe_run_id}'
        ORDER BY STEP_NUMBER ASC
        LIMIT {int(limit)}
    """

    return query_to_dataframe(query)


def main():
    print("=" * 70)
    print("SNOWGUARD PIPELINE EXECUTION HISTORY")
    print("=" * 70)

    runs = load_pipeline_runs()

    if runs.empty:
        print("No pipeline execution history found.")
        return

    print()
    print(f"Runs found: {len(runs)}")
    print()

    display_columns = [
        "RUN_ID",
        "STARTED_AT",
        "TOTAL_DURATION_SECONDS",
        "STATUS",
        "SUCCESSFUL_STEPS",
        "TOTAL_STEPS",
    ]

    print(
        runs[display_columns].to_string(index=False)
    )

    latest_run_id = runs.iloc[0]["RUN_ID"]

    print()
    print("=" * 70)
    print(
        f"LATEST RUN STEP DETAILS: {latest_run_id}"
    )
    print("=" * 70)

    steps = load_step_runs(
        latest_run_id
    )

    if steps.empty:
        print(
            "No step execution records found "
            "for the latest run."
        )
        return

    step_columns = [
        "STEP_NUMBER",
        "STEP_NAME",
        "DURATION_SECONDS",
        "STATUS",
        "ERROR_MESSAGE",
    ]

    print(
        steps[step_columns].to_string(index=False)
    )

    print()
    print("=" * 70)
    print("PIPELINE HISTORY CHECK COMPLETED")
    print("=" * 70)


if __name__ == "__main__":

    try:
        main()

    except Exception as error:

        print(
            f"Pipeline history failed: {error}",
            file=sys.stderr,
        )

        sys.exit(1)
