# ============================================================
# SNOWGUARD HEALTH CHECK
# Production system health validation
# ============================================================

import os
from datetime import datetime

import snowflake.connector
from dotenv import load_dotenv


# ============================================================
# ENVIRONMENT
# ============================================================

load_dotenv()

DATABASE = "SNOWGUARD_DB"


# ============================================================
# SNOWFLAKE CONNECTION
# ============================================================

def create_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=DATABASE,
        schema="ANALYTICS",
        role=os.getenv("SNOWFLAKE_ROLE"),
    )


# ============================================================
# QUERY HELPER
# ============================================================

def execute_query(connection, query, params=None):
    cursor = connection.cursor()

    try:
        if params is None:
            cursor.execute(query)
        else:
            cursor.execute(query, params)

        rows = cursor.fetchall()

        columns = (
            [column[0] for column in cursor.description]
            if cursor.description
            else []
        )

        return rows, columns

    finally:
        cursor.close()


# ============================================================
# TABLE ROW COUNT
# ============================================================

def get_count(connection, table_name):
    query = f"""
        SELECT COUNT(*)
        FROM {DATABASE}.{table_name}
    """

    rows, _ = execute_query(connection, query)

    return int(rows[0][0]) if rows else 0


# ============================================================
# PIPELINE CHECK
# ============================================================

def check_pipeline(connection):

    try:

        query = f"""
            SELECT *
            FROM {DATABASE}.ANALYTICS.PIPELINE_RUNS
            ORDER BY RUN_ID DESC
            LIMIT 1
        """

        rows, columns = execute_query(
            connection,
            query,
        )

        if not rows:
            return {
                "status": "UNKNOWN",
                "message": "No pipeline runs found.",
                "run_id": "N/A",
                "successful_steps": 0,
                "total_steps": 0,
            }

        latest = dict(zip(columns, rows[0]))

        run_id = latest.get("RUN_ID", "N/A")

        run_status = str(
            latest.get(
                "STATUS",
                latest.get(
                    "RUN_STATUS",
                    "UNKNOWN",
                ),
            )
        ).upper()

        step_query = f"""
            SELECT STATUS, COUNT(*)
            FROM {DATABASE}.ANALYTICS.PIPELINE_STEP_RUNS
            WHERE RUN_ID = %s
            GROUP BY STATUS
        """

        step_rows, _ = execute_query(
            connection,
            step_query,
            (run_id,),
        )

        successful_steps = 0
        total_steps = 0

        for status, count in step_rows:

            count = int(count)

            total_steps += count

            if str(status).upper() == "SUCCESS":
                successful_steps = count

        if run_status == "SUCCESS" and successful_steps >= 7:

            health = "HEALTHY"

        elif total_steps > 0:

            health = "WARNING"

        else:

            health = "UNKNOWN"

        return {
            "status": health,
            "message": (
                f"Latest run {run_id}: "
                f"{successful_steps}/{total_steps} "
                f"successful stages."
            ),
            "run_id": str(run_id),
            "run_status": run_status,
            "successful_steps": successful_steps,
            "total_steps": total_steps,
        }

    except Exception as error:

        return {
            "status": "CRITICAL",
            "message": f"Pipeline check failed: {error}",
            "run_id": "N/A",
            "successful_steps": 0,
            "total_steps": 0,
        }


# ============================================================
# MONITORING SUMMARY CHECK
# ============================================================

def check_monitoring_summary(connection):

    try:

        query = f"""
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
            FROM {DATABASE}.ANALYTICS.MONITORING_SUMMARY
            ORDER BY CREATED_AT DESC
            LIMIT 1
        """

        rows, columns = execute_query(
            connection,
            query,
        )

        if not rows:

            return {
                "status": "CRITICAL",
                "message": "No monitoring summary found.",
            }

        result = dict(
            zip(
                columns,
                rows[0],
            )
        )

        return {
            "status": "AVAILABLE",
            "created_at": result.get("CREATED_AT"),
            "total_records": result.get("TOTAL_RECORDS"),
            "quality_score": result.get("QUALITY_SCORE"),
            "quality_status": result.get("QUALITY_STATUS"),
            "anomaly_count": result.get("ANOMALY_COUNT"),
            "anomaly_rate": result.get("ANOMALY_RATE"),
            "drifted_features": result.get(
                "DRIFTED_FEATURE_COUNT"
            ),
            "drift_status": result.get("DRIFT_STATUS"),
            "rca_count": result.get("RCA_COUNT"),
            "overall_status": result.get("OVERALL_STATUS"),
        }

    except Exception as error:

        return {
            "status": "CRITICAL",
            "message": (
                f"Monitoring summary check failed: {error}"
            ),
        }


# ============================================================
# MAIN HEALTH CHECK
# ============================================================

def run_health_check():

    print()
    print("=" * 70)
    print("             SNOWGUARD SYSTEM HEALTH CHECK")
    print("=" * 70)
    print(
        "Timestamp : "
        + datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    )
    print()

    connection = None

    try:

        # --------------------------------------------------------
        # SNOWFLAKE CONNECTION
        # --------------------------------------------------------

        try:

            connection = create_connection()

            rows, _ = execute_query(
                connection,
                "SELECT CURRENT_TIMESTAMP()",
            )

            print("SNOWFLAKE CONNECTION")
            print("-" * 70)
            print("Status                 : HEALTHY")
            print(
                "Server timestamp       : "
                + str(
                    rows[0][0]
                    if rows
                    else "UNKNOWN"
                )
            )

        except Exception as error:

            print("SNOWFLAKE CONNECTION")
            print("-" * 70)
            print("Status                 : CRITICAL")
            print(f"Error                  : {error}")

            return False

        print()

        # --------------------------------------------------------
        # REQUIRED TABLES
        # --------------------------------------------------------

        required_tables = [
            "RAW.TRANSACTIONS_RAW",
            "QUALITY.QUALITY_RESULTS",
            "QUALITY.ANOMALY_RESULTS",
            "QUALITY.DRIFT_RESULTS",
            "PROCESSED.TRANSACTIONS_REFERENCE",
            "PROCESSED.TRANSACTIONS_CURRENT",
            "ANALYTICS.ROOT_CAUSE_RESULTS",
            "ANALYTICS.MONITORING_SUMMARY",
            "ANALYTICS.ALERT_RESULTS",
            "ANALYTICS.PIPELINE_RUNS",
            "ANALYTICS.PIPELINE_STEP_RUNS",
            "ANALYTICS.MONITORING_HISTORY",
        ]

        print("DATA STORAGE")
        print("-" * 70)

        table_failures = 0

        for table in required_tables:

            try:

                count = get_count(
                    connection,
                    table,
                )

                print(
                    f"{table:<40} : {count} rows"
                )

            except Exception as error:

                table_failures += 1

                print(
                    f"{table:<40} : ERROR - {error}"
                )

        print()

        # --------------------------------------------------------
        # PIPELINE
        # --------------------------------------------------------

        pipeline = check_pipeline(
            connection
        )

        print("PIPELINE")
        print("-" * 70)

        print(
            f"Status                 : "
            f"{pipeline.get('status', 'UNKNOWN')}"
        )

        print(
            f"Run ID                 : "
            f"{pipeline.get('run_id', 'N/A')}"
        )

        print(
            f"Run status             : "
            f"{pipeline.get('run_status', 'UNKNOWN')}"
        )

        print(
            f"Successful stages      : "
            f"{pipeline.get('successful_steps', 0)}"
        )

        print(
            f"Total stages           : "
            f"{pipeline.get('total_steps', 0)}"
        )

        print(
            f"Message                : "
            f"{pipeline.get('message', '')}"
        )

        print()

        # --------------------------------------------------------
        # MONITORING SUMMARY
        # --------------------------------------------------------

        summary = check_monitoring_summary(
            connection
        )

        print("MONITORING SUMMARY")
        print("-" * 70)

        if summary.get("status") == "AVAILABLE":

            print(
                f"Created at             : "
                f"{summary.get('created_at')}"
            )

            print(
                f"Total records          : "
                f"{summary.get('total_records')}"
            )

            print(
                f"Quality score          : "
                f"{summary.get('quality_score')}%"
            )

            print(
                f"Quality status         : "
                f"{summary.get('quality_status')}"
            )

            print(
                f"Anomaly count          : "
                f"{summary.get('anomaly_count')}"
            )

            print(
                f"Anomaly rate           : "
                f"{summary.get('anomaly_rate')}%"
            )

            print(
                f"Drifted features       : "
                f"{summary.get('drifted_features')}"
            )

            print(
                f"Drift status           : "
                f"{summary.get('drift_status')}"
            )

            print(
                f"RCA records            : "
                f"{summary.get('rca_count')}"
            )

            print(
                f"Overall monitoring     : "
                f"{summary.get('overall_status')}"
            )

        else:

            print(
                f"Status                 : "
                f"{summary.get('status')}"
            )

            print(
                f"Message                : "
                f"{summary.get('message')}"
            )

        print()

        # --------------------------------------------------------
        # ALERT CHECK
        # --------------------------------------------------------

        try:

            alert_records = get_count(
                connection,
                "ANALYTICS.ALERT_RESULTS",
            )

            print("ALERTS")
            print("-" * 70)

            print(
                f"Alert records           : "
                f"{alert_records}"
            )

        except Exception as error:

            print("ALERTS")
            print("-" * 70)

            print(
                f"Status                 : ERROR - {error}"
            )

        print()

        # --------------------------------------------------------
        # FINAL HEALTH
        # --------------------------------------------------------

        infrastructure_ok = (
            pipeline.get("status") == "HEALTHY"
            and summary.get("status") == "AVAILABLE"
            and table_failures == 0
        )

        print("FINAL SYSTEM HEALTH")
        print("=" * 70)

        if infrastructure_ok:

            print(
                "Infrastructure health  : HEALTHY"
            )

            print(
                "System check            : PASSED"
            )

        else:

            print(
                "Infrastructure health  : "
                "WARNING / CRITICAL"
            )

            print(
                "System check            : "
                "REVIEW REQUIRED"
            )

        print(
            f"Monitoring status      : "
            f"{summary.get('overall_status', 'UNKNOWN')}"
        )

        print("=" * 70)
        print()

        return infrastructure_ok

    finally:

        if connection is not None:
            connection.close()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    success = run_health_check()

    raise SystemExit(
        0 if success else 1
    )
