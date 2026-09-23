# ============================================================
# SNOWGUARD - MONITORING SUMMARY SERVICE
# ============================================================
# Purpose:
#   Combines:
#       1. Data Quality
#       2. Anomaly Detection
#       3. Drift Detection
#       4. Root Cause Analysis
#
#   into one unified monitoring summary.
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
# SNOWFLAKE CONNECTION
# ============================================================

def create_connection():
    """Create and return a Snowflake connection."""

    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database="SNOWGUARD_DB",
        schema="ANALYTICS",
        role=os.getenv("SNOWFLAKE_ROLE"),
    )


# ============================================================
# QUERY HELPER
# ============================================================

def fetch_dataframe(connection, query):
    """Execute SQL query and return result as Pandas DataFrame."""

    cursor = connection.cursor()

    try:
        cursor.execute(query)

        rows = cursor.fetchall()

        columns = [column[0] for column in cursor.description]

        return pd.DataFrame(rows, columns=columns)

    finally:
        cursor.close()


# ============================================================
# GET TOTAL RECORD COUNT
# ============================================================

def get_total_records(connection):
    """Get total number of raw transaction records."""

    query = """
        SELECT COUNT(*) AS TOTAL_RECORDS
        FROM SNOWGUARD_DB.RAW.TRANSACTIONS_RAW
    """

    df = fetch_dataframe(connection, query)

    if df.empty:
        return 0

    return int(df.iloc[0]["TOTAL_RECORDS"])


# ============================================================
# GET DATA QUALITY SUMMARY
# ============================================================

def get_quality_summary(connection):
    """
    Get data quality score.

    QUALITY_RESULTS does not use CREATED_AT here.

    The monitoring layer derives the status directly from
    the quality score:

        >= 95  -> GOOD
        >= 90  -> WARNING
        < 90   -> CRITICAL
    """

    query = """
        SELECT
            QUALITY_SCORE
        FROM SNOWGUARD_DB.QUALITY.QUALITY_RESULTS
        LIMIT 1
    """

    df = fetch_dataframe(connection, query)

    if df.empty:
        return 0.0, "UNKNOWN"

    quality_score = float(df.iloc[0]["QUALITY_SCORE"])

    # --------------------------------------------------------
    # Derive quality status
    # --------------------------------------------------------

    if quality_score >= 95:
        quality_status = "GOOD"

    elif quality_score >= 90:
        quality_status = "WARNING"

    else:
        quality_status = "CRITICAL"

    return quality_score, quality_status


# ============================================================
# GET ANOMALY SUMMARY
# ============================================================

def get_anomaly_summary(connection, total_records):
    """
    Get anomaly count and anomaly rate.

    ANOMALY_RESULTS contains the detected anomalous records.
    """

    query = """
        SELECT COUNT(*) AS ANOMALY_COUNT
        FROM SNOWGUARD_DB.QUALITY.ANOMALY_RESULTS
    """

    df = fetch_dataframe(connection, query)

    if df.empty:
        anomaly_count = 0
    else:
        anomaly_count = int(df.iloc[0]["ANOMALY_COUNT"])

    # --------------------------------------------------------
    # Calculate anomaly rate
    # --------------------------------------------------------

    if total_records > 0:
        anomaly_rate = (
            anomaly_count / total_records
        ) * 100
    else:
        anomaly_rate = 0.0

    return anomaly_count, anomaly_rate


# ============================================================
# GET DRIFT SUMMARY
# ============================================================

def get_drift_summary(connection):
    """
    Get overall drift status.

    DRIFT_RESULTS only requires DRIFT_STATUS.

    Priority:

        HIGH
          ↓
        MEDIUM
          ↓
        LOW
          ↓
        STABLE
    """

    query = """
        SELECT
            DRIFT_STATUS
        FROM SNOWGUARD_DB.QUALITY.DRIFT_RESULTS
    """

    df = fetch_dataframe(connection, query)

    if df.empty:
        return 0, "UNKNOWN"

    # --------------------------------------------------------
    # Normalize statuses
    # --------------------------------------------------------

    drift_statuses = (
        df["DRIFT_STATUS"]
        .astype(str)
        .str.upper()
        .str.strip()
        .tolist()
    )

    # --------------------------------------------------------
    # Count drifted features
    # --------------------------------------------------------

    drifted_feature_count = sum(
        status in ["HIGH", "MEDIUM", "LOW"]
        for status in drift_statuses
    )

    # --------------------------------------------------------
    # Determine overall drift severity
    # --------------------------------------------------------

    if "HIGH" in drift_statuses:

        overall_drift_status = "HIGH"

    elif "MEDIUM" in drift_statuses:

        overall_drift_status = "MEDIUM"

    elif "LOW" in drift_statuses:

        overall_drift_status = "LOW"

    else:

        overall_drift_status = "STABLE"

    return drifted_feature_count, overall_drift_status


# ============================================================
# GET ROOT CAUSE COUNT
# ============================================================

def get_rca_count(connection):
    """Get total number of root cause analysis records."""

    query = """
        SELECT COUNT(*) AS RCA_COUNT
        FROM SNOWGUARD_DB.ANALYTICS.ROOT_CAUSE_RESULTS
    """

    df = fetch_dataframe(connection, query)

    if df.empty:
        return 0

    return int(df.iloc[0]["RCA_COUNT"])


# ============================================================
# CALCULATE OVERALL SNOWGUARD STATUS
# ============================================================

def calculate_overall_status(
    quality_score,
    quality_status,
    anomaly_rate,
    drift_status,
):
    """
    Calculate overall SnowGuard health.

    CRITICAL:
        - Quality score < 90
        - Quality status = CRITICAL
        - HIGH drift

    WARNING:
        - Quality score < 95
        - Quality status = WARNING
        - MEDIUM drift
        - Anomaly rate > 5%

    GOOD:
        - Otherwise
    """

    quality_status = str(
        quality_status
    ).upper().strip()

    drift_status = str(
        drift_status
    ).upper().strip()

    # --------------------------------------------------------
    # CRITICAL CONDITIONS
    # --------------------------------------------------------

    if quality_score < 90:
        return "CRITICAL"

    if quality_status == "CRITICAL":
        return "CRITICAL"

    if drift_status == "HIGH":
        return "CRITICAL"

    # --------------------------------------------------------
    # WARNING CONDITIONS
    # --------------------------------------------------------

    if quality_score < 95:
        return "WARNING"

    if quality_status == "WARNING":
        return "WARNING"

    if drift_status == "MEDIUM":
        return "WARNING"

    if anomaly_rate > 5:
        return "WARNING"

    # --------------------------------------------------------
    # GOOD
    # --------------------------------------------------------

    return "GOOD"


# ============================================================
# SAVE MONITORING SUMMARY
# ============================================================

def save_monitoring_summary(
    connection,
    total_records,
    quality_score,
    quality_status,
    anomaly_count,
    anomaly_rate,
    drifted_feature_count,
    drift_status,
    rca_count,
    overall_status,
):
    """Save latest monitoring summary into Snowflake."""

    cursor = connection.cursor()

    try:

        # ----------------------------------------------------
        # Remove previous summary
        # ----------------------------------------------------

        cursor.execute(
            """
            TRUNCATE TABLE
            SNOWGUARD_DB.ANALYTICS.MONITORING_SUMMARY
            """
        )

        # ----------------------------------------------------
        # Generate unique summary ID
        # ----------------------------------------------------

        summary_id = str(uuid.uuid4())

        # ----------------------------------------------------
        # Insert latest summary
        # ----------------------------------------------------

        insert_query = """
            INSERT INTO SNOWGUARD_DB.ANALYTICS.MONITORING_SUMMARY (
                SUMMARY_ID,
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
            VALUES (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
            )
        """

        cursor.execute(
            insert_query,
            (
                summary_id,
                total_records,
                quality_score,
                quality_status,
                anomaly_count,
                anomaly_rate,
                drifted_feature_count,
                drift_status,
                rca_count,
                overall_status,
            ),
        )

        connection.commit()

    finally:
        cursor.close()


# ============================================================
# DISPLAY MONITORING REPORT
# ============================================================

def print_monitoring_report(
    total_records,
    quality_score,
    quality_status,
    anomaly_count,
    anomaly_rate,
    drifted_feature_count,
    drift_status,
    rca_count,
    overall_status,
):
    """Display the SnowGuard monitoring report."""

    print()

    print("=" * 60)
    print("          SNOWGUARD MONITORING SUMMARY")
    print("=" * 60)

    # --------------------------------------------------------
    # DATASET
    # --------------------------------------------------------

    print()
    print("DATASET")
    print("-" * 60)

    print(
        f"Total records          : {total_records}"
    )

    # --------------------------------------------------------
    # DATA QUALITY
    # --------------------------------------------------------

    print()
    print("DATA QUALITY")
    print("-" * 60)

    print(
        f"Quality score          : {quality_score:.2f}%"
    )

    print(
        f"Quality status         : {quality_status}"
    )

    # --------------------------------------------------------
    # ANOMALIES
    # --------------------------------------------------------

    print()
    print("ANOMALY DETECTION")
    print("-" * 60)

    print(
        f"Anomaly count          : {anomaly_count}"
    )

    print(
        f"Anomaly rate           : {anomaly_rate:.2f}%"
    )

    # --------------------------------------------------------
    # DRIFT
    # --------------------------------------------------------

    print()
    print("DRIFT DETECTION")
    print("-" * 60)

    print(
        f"Drifted features       : {drifted_feature_count}"
    )

    print(
        f"Overall drift status   : {drift_status}"
    )

    # --------------------------------------------------------
    # ROOT CAUSE ANALYSIS
    # --------------------------------------------------------

    print()
    print("ROOT CAUSE ANALYSIS")
    print("-" * 60)

    print(
        f"RCA records            : {rca_count}"
    )

    # --------------------------------------------------------
    # OVERALL HEALTH
    # --------------------------------------------------------

    print()
    print("OVERALL SNOWGUARD HEALTH")
    print("-" * 60)

    print(
        f"Overall status         : {overall_status}"
    )

    # --------------------------------------------------------
    # COMPLETION
    # --------------------------------------------------------

    print()

    print("=" * 60)
    print("Monitoring summary saved to Snowflake! ✅")
    print("=" * 60)

    print()


# ============================================================
# MAIN MONITORING PIPELINE
# ============================================================

def run_monitoring_summary():
    """Run the complete SnowGuard monitoring pipeline."""

    connection = None

    try:

        # ----------------------------------------------------
        # HEADER
        # ----------------------------------------------------

        print()

        print("=" * 60)
        print("       SNOWGUARD MONITORING SERVICE")
        print("=" * 60)

        # ----------------------------------------------------
        # CONNECT TO SNOWFLAKE
        # ----------------------------------------------------

        connection = create_connection()

        print(
            "Snowflake connection successful! ✅"
        )

        # ----------------------------------------------------
        # TOTAL RECORDS
        # ----------------------------------------------------

        total_records = get_total_records(
            connection
        )

        # ----------------------------------------------------
        # DATA QUALITY
        # ----------------------------------------------------

        quality_score, quality_status = (
            get_quality_summary(
                connection
            )
        )

        # ----------------------------------------------------
        # ANOMALY DETECTION
        # ----------------------------------------------------

        anomaly_count, anomaly_rate = (
            get_anomaly_summary(
                connection,
                total_records,
            )
        )

        # ----------------------------------------------------
        # DRIFT DETECTION
        # ----------------------------------------------------

        drifted_feature_count, drift_status = (
            get_drift_summary(
                connection
            )
        )

        # ----------------------------------------------------
        # ROOT CAUSE ANALYSIS
        # ----------------------------------------------------

        rca_count = get_rca_count(
            connection
        )

        # ----------------------------------------------------
        # OVERALL HEALTH
        # ----------------------------------------------------

        overall_status = calculate_overall_status(
            quality_score,
            quality_status,
            anomaly_rate,
            drift_status,
        )

        # ----------------------------------------------------
        # SAVE MONITORING SUMMARY
        # ----------------------------------------------------

        save_monitoring_summary(
            connection,
            total_records,
            quality_score,
            quality_status,
            anomaly_count,
            anomaly_rate,
            drifted_feature_count,
            drift_status,
            rca_count,
            overall_status,
        )

        # ----------------------------------------------------
        # DISPLAY REPORT
        # ----------------------------------------------------

        print_monitoring_report(
            total_records,
            quality_score,
            quality_status,
            anomaly_count,
            anomaly_rate,
            drifted_feature_count,
            drift_status,
            rca_count,
            overall_status,
        )

    except Exception as error:

        print()

        print("=" * 60)
        print("MONITORING SERVICE ERROR")
        print("=" * 60)

        print(
            f"Error: {error}"
        )

        print("=" * 60)

        print()

        raise

    finally:

        # ----------------------------------------------------
        # CLOSE CONNECTION
        # ----------------------------------------------------

        if connection is not None:
            connection.close()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    run_monitoring_summary()