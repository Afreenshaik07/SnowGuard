# ============================================================
# SNOWGUARD - ALERT & RECOMMENDATION ENGINE
# ============================================================
# Converts monitoring results into actionable alerts.
#
# Reads:
#   ANALYTICS.MONITORING_SUMMARY
#   QUALITY.DRIFT_RESULTS
#
# Writes:
#   ANALYTICS.ALERT_RESULTS
# ============================================================

import os
import uuid

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv


# ============================================================
# LOAD ENVIRONMENT VARIABLES
# ============================================================

load_dotenv()


# ============================================================
# DATABASE CONFIGURATION
# ============================================================

DATABASE = "SNOWGUARD_DB"

ANALYTICS_SCHEMA = "ANALYTICS"

QUALITY_SCHEMA = "QUALITY"

MONITORING_TABLE = (
    f"{DATABASE}.{ANALYTICS_SCHEMA}.MONITORING_SUMMARY"
)

DRIFT_TABLE = (
    f"{DATABASE}.{QUALITY_SCHEMA}.DRIFT_RESULTS"
)

ALERT_TABLE = (
    f"{DATABASE}.{ANALYTICS_SCHEMA}.ALERT_RESULTS"
)


# ============================================================
# CREATE SNOWFLAKE CONNECTION
# ============================================================

def create_connection():
    """
    Create a Snowflake connection using .env credentials.
    """

    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=DATABASE,
        schema=ANALYTICS_SCHEMA,
        role=os.getenv("SNOWFLAKE_ROLE"),
    )


# ============================================================
# FETCH DATAFRAME
# ============================================================

def fetch_dataframe(connection, query):
    """
    Execute a SQL query and return a Pandas DataFrame.
    """

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


# ============================================================
# GET MONITORING SUMMARY
# ============================================================

def get_monitoring_summary(connection):
    """
    Get the latest SnowGuard monitoring summary.
    """

    query = f"""
        SELECT
            TOTAL_RECORDS,
            QUALITY_SCORE,
            QUALITY_STATUS,
            ANOMALY_COUNT,
            ANOMALY_RATE,
            DRIFTED_FEATURE_COUNT,
            DRIFT_STATUS,
            RCA_COUNT,
            OVERALL_STATUS
        FROM {MONITORING_TABLE}
        ORDER BY CREATED_AT DESC
        LIMIT 1
    """

    df = fetch_dataframe(
        connection,
        query,
    )

    if df.empty:

        raise RuntimeError(
            "No monitoring summary found in MONITORING_SUMMARY."
        )

    return df.iloc[0]


# ============================================================
# GET DRIFT RESULTS
# ============================================================

def get_drift_results(connection):
    """
    Load drift detection results.
    """

    query = f"""
        SELECT *
        FROM {DRIFT_TABLE}
    """

    return fetch_dataframe(
        connection,
        query,
    )


# ============================================================
# FIND COLUMN
# ============================================================

def find_column(dataframe, possible_names):
    """
    Find a column using possible column names.

    This makes the code robust if Snowflake uses
    FEATURE_NAME instead of FEATURE, etc.
    """

    column_map = {
        str(column).upper(): column
        for column in dataframe.columns
    }

    for name in possible_names:

        if name.upper() in column_map:

            return column_map[name.upper()]

    return None


# ============================================================
# CREATE ALERT
# ============================================================

def create_alert(
    level,
    alert_type,
    feature_name,
    message,
    recommendation,
):
    """
    Create a single alert.
    """

    return {
        "ALERT_ID": str(uuid.uuid4()),

        "ALERT_LEVEL": level,

        "ALERT_TYPE": alert_type,

        "FEATURE_NAME": feature_name,

        "ALERT_MESSAGE": message,

        "RECOMMENDATION": recommendation,

        "STATUS": "OPEN",
    }


# ============================================================
# CHECK DATA QUALITY
# ============================================================

def check_quality_alert(summary, alerts):
    """
    Generate alerts based on quality score.
    """

    quality_score = float(
        summary["QUALITY_SCORE"]
    )

    quality_status = str(
        summary["QUALITY_STATUS"]
    ).upper()

    # --------------------------------------------------------
    # CRITICAL QUALITY
    # --------------------------------------------------------

    if quality_score < 90:

        alerts.append(
            create_alert(
                level="CRITICAL",
                alert_type="DATA_QUALITY",
                feature_name=None,
                message=(
                    f"Data quality score is "
                    f"{quality_score:.2f}%."
                ),
                recommendation=(
                    "Investigate missing values, "
                    "duplicates, invalid records, "
                    "and other quality failures "
                    "before downstream processing."
                ),
            )
        )

    # --------------------------------------------------------
    # WARNING QUALITY
    # --------------------------------------------------------

    elif quality_score < 95:

        alerts.append(
            create_alert(
                level="WARNING",
                alert_type="DATA_QUALITY",
                feature_name=None,
                message=(
                    f"Data quality score is "
                    f"{quality_score:.2f}%."
                ),
                recommendation=(
                    "Review the latest data quality "
                    "validation results."
                ),
            )
        )

    # --------------------------------------------------------
    # STATUS CHECK
    # --------------------------------------------------------

    elif quality_status == "CRITICAL":

        alerts.append(
            create_alert(
                level="CRITICAL",
                alert_type="DATA_QUALITY",
                feature_name=None,
                message=(
                    "Data quality status is CRITICAL."
                ),
                recommendation=(
                    "Review the data quality engine "
                    "results immediately."
                ),
            )
        )


# ============================================================
# CHECK ANOMALIES
# ============================================================

def check_anomaly_alert(summary, alerts):
    """
    Generate anomaly alerts.

    Thresholds:
        >= 10% = CRITICAL
        >= 5%  = WARNING
    """

    anomaly_count = int(
        summary["ANOMALY_COUNT"]
    )

    anomaly_rate = float(
        summary["ANOMALY_RATE"]
    )

    # --------------------------------------------------------
    # CRITICAL ANOMALY RATE
    # --------------------------------------------------------

    if anomaly_rate >= 10:

        alerts.append(
            create_alert(
                level="CRITICAL",
                alert_type="ANOMALY_DETECTION",
                feature_name=None,
                message=(
                    f"{anomaly_count:,} anomalous "
                    f"records detected "
                    f"({anomaly_rate:.2f}%)."
                ),
                recommendation=(
                    "Investigate anomalous records "
                    "before using the dataset for "
                    "downstream analytics or ML."
                ),
            )
        )

    # --------------------------------------------------------
    # WARNING ANOMALY RATE
    # --------------------------------------------------------

    elif anomaly_rate >= 5:

        alerts.append(
            create_alert(
                level="WARNING",
                alert_type="ANOMALY_DETECTION",
                feature_name=None,
                message=(
                    f"{anomaly_count:,} anomalous "
                    f"records detected "
                    f"({anomaly_rate:.2f}%)."
                ),
                recommendation=(
                    "Review the detected anomalous "
                    "transactions and investigate "
                    "potential data issues."
                ),
            )
        )


# ============================================================
# CHECK DRIFT
# ============================================================

def check_drift_alert(
    summary,
    drift_df,
    alerts,
):
    """
    Generate alerts for feature drift.
    """

    drift_status = str(
        summary["DRIFT_STATUS"]
    ).upper()

    drifted_count = int(
        summary["DRIFTED_FEATURE_COUNT"]
    )

    # --------------------------------------------------------
    # FIND FEATURE COLUMN
    # --------------------------------------------------------

    feature_column = find_column(
        drift_df,
        [
            "FEATURE_NAME",
            "FEATURE",
            "FEATURENAME",
        ],
    )

    # --------------------------------------------------------
    # FIND STATUS COLUMN
    # --------------------------------------------------------

    status_column = find_column(
        drift_df,
        [
            "DRIFT_STATUS",
            "STATUS",
            "DRIFT_LEVEL",
        ],
    )

    # --------------------------------------------------------
    # COLLECT HIGH-DRIFT FEATURES
    # --------------------------------------------------------

    high_drift_features = []

    if (
        feature_column is not None
        and status_column is not None
    ):

        for _, row in drift_df.iterrows():

            row_status = str(
                row[status_column]
            ).upper().strip()

            if row_status == "HIGH":

                feature = str(
                    row[feature_column]
                ).strip()

                if feature not in high_drift_features:

                    high_drift_features.append(
                        feature
                    )

    # --------------------------------------------------------
    # HIGH DRIFT
    # --------------------------------------------------------

    if drift_status == "HIGH":

        if high_drift_features:

            feature_text = ", ".join(
                high_drift_features
            )

        else:

            feature_text = (
                f"{drifted_count} features"
            )

        alerts.append(
            create_alert(
                level="CRITICAL",
                alert_type="DATA_DRIFT",
                feature_name=feature_text,
                message=(
                    f"High distribution drift detected "
                    f"for {feature_text}."
                ),
                recommendation=(
                    "Investigate the current data "
                    "distribution and compare it "
                    "with the reference dataset "
                    "before downstream ML usage."
                ),
            )
        )

    # --------------------------------------------------------
    # MEDIUM DRIFT
    # --------------------------------------------------------

    elif drift_status == "MEDIUM":

        alerts.append(
            create_alert(
                level="WARNING",
                alert_type="DATA_DRIFT",
                feature_name=None,
                message=(
                    f"Medium drift detected across "
                    f"{drifted_count} feature(s)."
                ),
                recommendation=(
                    "Review the affected features "
                    "and monitor their distributions "
                    "in subsequent data batches."
                ),
            )
        )


# ============================================================
# CHECK OVERALL SYSTEM HEALTH
# ============================================================

def check_overall_health(summary, alerts):
    """
    Generate an alert for overall system health.
    """

    overall_status = str(
        summary["OVERALL_STATUS"]
    ).upper()

    # --------------------------------------------------------
    # CRITICAL
    # --------------------------------------------------------

    if overall_status == "CRITICAL":

        alerts.append(
            create_alert(
                level="CRITICAL",
                alert_type="SYSTEM_HEALTH",
                feature_name=None,
                message=(
                    "Overall SnowGuard system health "
                    "is currently CRITICAL."
                ),
                recommendation=(
                    "Review active drift, anomaly, "
                    "and data quality alerts before "
                    "using the current dataset."
                ),
            )
        )

    # --------------------------------------------------------
    # WARNING
    # --------------------------------------------------------

    elif overall_status == "WARNING":

        alerts.append(
            create_alert(
                level="WARNING",
                alert_type="SYSTEM_HEALTH",
                feature_name=None,
                message=(
                    "Overall SnowGuard system health "
                    "requires attention."
                ),
                recommendation=(
                    "Review the active monitoring "
                    "alerts and investigate "
                    "the affected components."
                ),
            )
        )


# ============================================================
# SAVE ALERTS TO SNOWFLAKE
# ============================================================

def save_alerts(connection, alerts):
    """
    Replace previous alert results with latest alerts.
    """

    cursor = connection.cursor()

    try:

        # ----------------------------------------------------
        # CLEAR OLD ALERTS
        # ----------------------------------------------------

        cursor.execute(
            f"""
            TRUNCATE TABLE {ALERT_TABLE}
            """
        )

        # ----------------------------------------------------
        # INSERT NEW ALERTS
        # ----------------------------------------------------

        insert_query = f"""
            INSERT INTO {ALERT_TABLE}
            (
                ALERT_ID,
                ALERT_LEVEL,
                ALERT_TYPE,
                FEATURE_NAME,
                ALERT_MESSAGE,
                RECOMMENDATION,
                STATUS
            )
            VALUES
            (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
            )
        """

        for alert in alerts:

            cursor.execute(
                insert_query,
                (
                    alert["ALERT_ID"],
                    alert["ALERT_LEVEL"],
                    alert["ALERT_TYPE"],
                    alert["FEATURE_NAME"],
                    alert["ALERT_MESSAGE"],
                    alert["RECOMMENDATION"],
                    alert["STATUS"],
                ),
            )

        connection.commit()

    finally:

        cursor.close()


# ============================================================
# PRINT ALERTS
# ============================================================

def print_alerts(alerts):
    """
    Print alerts in a clean terminal format.
    """

    print()
    print("=" * 70)
    print(
        "              SNOWGUARD ALERT ENGINE"
    )
    print("=" * 70)

    print()
    print(
        f"Alerts generated : {len(alerts)}"
    )

    print()
    print("ALERTS")
    print("-" * 70)

    if not alerts:

        print(
            "No active alerts detected. ✅"
        )

    for index, alert in enumerate(
        alerts,
        start=1,
    ):

        print()

        print(
            f"[{index}] "
            f"{alert['ALERT_LEVEL']} | "
            f"{alert['ALERT_TYPE']}"
        )

        if alert["FEATURE_NAME"]:

            print(
                f"Feature        : "
                f"{alert['FEATURE_NAME']}"
            )

        print(
            f"Message        : "
            f"{alert['ALERT_MESSAGE']}"
        )

        print(
            f"Recommendation : "
            f"{alert['RECOMMENDATION']}"
        )

        print(
            f"Status         : "
            f"{alert['STATUS']}"
        )

    print()
    print("=" * 70)


# ============================================================
# MAIN ALERT PIPELINE
# ============================================================

def run_alert_engine():
    """
    Run the complete SnowGuard alert pipeline.
    """

    connection = None

    try:

        print()
        print("=" * 70)
        print(
            "       SNOWGUARD ALERT & RECOMMENDATION ENGINE"
        )
        print("=" * 70)

        # ----------------------------------------------------
        # CONNECT
        # ----------------------------------------------------

        connection = create_connection()

        print(
            "Snowflake connection successful! ✅"
        )

        # ----------------------------------------------------
        # LOAD MONITORING SUMMARY
        # ----------------------------------------------------

        summary = get_monitoring_summary(
            connection
        )

        print(
            "Monitoring summary loaded! ✅"
        )

        # ----------------------------------------------------
        # LOAD DRIFT RESULTS
        # ----------------------------------------------------

        drift_df = get_drift_results(
            connection
        )

        print(
            f"Drift records loaded : {len(drift_df)}"
        )

        # ----------------------------------------------------
        # GENERATE ALERTS
        # ----------------------------------------------------

        alerts = []

        check_quality_alert(
            summary,
            alerts,
        )

        check_anomaly_alert(
            summary,
            alerts,
        )

        check_drift_alert(
            summary,
            drift_df,
            alerts,
        )

        check_overall_health(
            summary,
            alerts,
        )

        # ----------------------------------------------------
        # PRINT
        # ----------------------------------------------------

        print_alerts(
            alerts
        )

        # ----------------------------------------------------
        # SAVE
        # ----------------------------------------------------

        save_alerts(
            connection,
            alerts,
        )

        print()
        print(
            "Alert results saved to "
            "ANALYTICS.ALERT_RESULTS! ✅"
        )

        print("=" * 70)

    except Exception as error:

        print()
        print("=" * 70)
        print(
            "             ALERT ENGINE ERROR"
        )
        print("=" * 70)

        print(
            f"Error: {error}"
        )

        print("=" * 70)

        raise

    finally:

        if connection is not None:

            connection.close()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    run_alert_engine()