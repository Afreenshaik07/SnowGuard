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

def query_df(query):
    connection = create_connection()
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [column[0] for column in cursor.description]
        return pd.DataFrame(rows, columns=columns)
    finally:
        cursor.close()
        connection.close()

def load_monitoring_history(limit=100):
    query = f"""
        SELECT CREATED_AT, TOTAL_RECORDS, QUALITY_SCORE, QUALITY_STATUS, ANOMALY_COUNT, ANOMALY_RATE, DRIFTED_FEATURE_COUNT, DRIFT_STATUS, RCA_COUNT, OVERALL_STATUS
        FROM {DATABASE}.{SCHEMA}.MONITORING_HISTORY
        ORDER BY CREATED_AT DESC
        LIMIT {int(limit)}
    """
    return query_df(query)

def load_step_history(limit=500):
    query = f"""
        SELECT RUN_ID, STEP_NUMBER, STEP_NAME, DURATION_SECONDS, STATUS, STARTED_AT, FINISHED_AT
        FROM {DATABASE}.{SCHEMA}.PIPELINE_STEP_RUNS
        ORDER BY STARTED_AT DESC
        LIMIT {int(limit)}
    """
    return query_df(query)

def main():
    print("=" * 70)
    print("SNOWGUARD HISTORICAL MONITORING TREND ANALYSIS")
    print("=" * 70)
    history = load_monitoring_history()
    if history.empty:
        print("No monitoring history found.")
        return
    history["CREATED_AT"] = pd.to_datetime(history["CREATED_AT"])
    print(f"Monitoring snapshots found: {len(history)}")
    print()
    columns = ["CREATED_AT", "QUALITY_SCORE", "ANOMALY_RATE", "DRIFTED_FEATURE_COUNT", "DRIFT_STATUS", "RCA_COUNT", "OVERALL_STATUS"]
    print(history[columns].sort_values("CREATED_AT").to_string(index=False))
    latest = history.sort_values("CREATED_AT").iloc[-1]
    print()
    print("=" * 70)
    print("LATEST MONITORING SNAPSHOT")
    print("=" * 70)
    print(f"Time             : {latest['CREATED_AT']}")
    print(f"Quality score    : {float(latest['QUALITY_SCORE']):.2f}%")
    print(f"Anomaly rate     : {float(latest['ANOMALY_RATE']):.2f}%")
    print(f"Drifted features : {int(latest['DRIFTED_FEATURE_COUNT'])}")
    print(f"Drift status     : {latest['DRIFT_STATUS']}")
    print(f"RCA records      : {int(latest['RCA_COUNT'])}")
    print(f"Overall status   : {latest['OVERALL_STATUS']}")
    steps = load_step_history()
    print()
    print("=" * 70)
    print("PIPELINE STAGE PERFORMANCE")
    print("=" * 70)
    if steps.empty:
        print("No pipeline step history found.")
    else:
        performance = steps.groupby("STEP_NAME", as_index=False)["DURATION_SECONDS"].mean().sort_values("DURATION_SECONDS", ascending=False)
        print(performance.to_string(index=False))
    print()
    print("=" * 70)
    print("HISTORICAL TREND ANALYSIS COMPLETED")
    print("=" * 70)

if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"Trend analysis failed: {error}", file=sys.stderr)
        sys.exit(1)


