import os
import sys
import snowflake.connector
from dotenv import load_dotenv
from app.pipeline.run_pipeline import run_pipeline

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

def save_monitoring_snapshot():
    connection = create_connection()
    cursor = connection.cursor()
    try:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {DATABASE}.{SCHEMA}.MONITORING_HISTORY (
                HISTORY_ID NUMBER AUTOINCREMENT,
                CREATED_AT TIMESTAMP_NTZ,
                TOTAL_RECORDS NUMBER,
                QUALITY_SCORE FLOAT,
                QUALITY_STATUS VARCHAR(50),
                ANOMALY_COUNT NUMBER,
                ANOMALY_RATE FLOAT,
                DRIFTED_FEATURE_COUNT NUMBER,
                DRIFT_STATUS VARCHAR(50),
                RCA_COUNT NUMBER,
                OVERALL_STATUS VARCHAR(50)
            )
        """)

        cursor.execute(f"""
            INSERT INTO {DATABASE}.{SCHEMA}.MONITORING_HISTORY (
                CREATED_AT,
                TOTAL_RECORDS,
                QUALITY_SCORE,
                QUALITY_STATUS,
                ANOMALY_COUNT,
                ANOMALY_RATE,
                DRIFTED_FEATURE_COUNT,
                DRIFT_STATUS,
                RCA_COUNT,
                OVERALL_STATUS
            )
            SELECT
                CREATED_AT,
                TOTAL_RECORDS,
                QUALITY_SCORE,
                QUALITY_STATUS,
                ANOMALY_COUNT,
                ANOMALY_RATE,
                DRIFTED_FEATURE_COUNT,
                DRIFT_STATUS,
                RCA_COUNT,
                OVERALL_STATUS
            FROM {DATABASE}.{SCHEMA}.MONITORING_SUMMARY
            ORDER BY CREATED_AT DESC
            LIMIT 1
        """)

        connection.commit()
        print("MONITORING HISTORY SNAPSHOT SAVED ✅")
    finally:
        cursor.close()
        connection.close()

def main():
    print("=" * 70)
    print("SNOWGUARD PIPELINE + HISTORY")
    print("=" * 70)

    run_pipeline()
    save_monitoring_snapshot()

    print("=" * 70)
    print("PIPELINE + HISTORY COMPLETED")
    print("=" * 70)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Pipeline interrupted by user.", file=sys.stderr)
        sys.exit(130)
    except Exception as error:
        print(f"Pipeline + history failed: {error}", file=sys.stderr)
        sys.exit(1)
