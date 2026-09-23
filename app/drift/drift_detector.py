import os
import uuid

import numpy as np
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from scipy.stats import ks_2samp


# ============================================================
# SNOWGUARD - STATISTICAL DRIFT DETECTOR
# ============================================================

load_dotenv()


FEATURES = [
    "AGE",
    "INCOME",
    "AMOUNT",
]


def create_connection():
    """Create Snowflake connection."""

    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database="SNOWGUARD_DB",
        schema="PROCESSED",
        role=os.getenv("SNOWFLAKE_ROLE"),
    )


def load_table(connection, table_name):
    """Load a Snowflake table into a DataFrame."""

    cursor = connection.cursor()

    try:
        cursor.execute(
            f"""
            SELECT
                TRANSACTION_ID,
                CUSTOMER_ID,
                AGE,
                INCOME,
                AMOUNT
            FROM SNOWGUARD_DB.PROCESSED.{table_name}
            """
        )

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


def calculate_drift(
    reference,
    current,
    feature,
):
    """Calculate KS two-sample drift statistics."""

    reference_values = (
        pd.to_numeric(
            reference[feature],
            errors="coerce",
        )
        .dropna()
        .to_numpy()
    )

    current_values = (
        pd.to_numeric(
            current[feature],
            errors="coerce",
        )
        .dropna()
        .to_numpy()
    )

    if (
        len(reference_values) == 0
        or len(current_values) == 0
    ):
        return None

    statistic, p_value = ks_2samp(
        reference_values,
        current_values,
    )

    reference_mean = float(
        np.mean(reference_values)
    )

    current_mean = float(
        np.mean(current_values)
    )

    return {
        "reference_records": len(
            reference_values
        ),
        "current_records": len(
            current_values
        ),
        "reference_mean": reference_mean,
        "current_mean": current_mean,
        "drift_score": float(statistic),
        "p_value": float(p_value),
    }


def classify_drift(
    drift_score,
    p_value,
):
    """
    Classify distribution drift.

    KS statistic:
        < 0.10  -> LOW
        < 0.20  -> MEDIUM
        >= 0.20 -> HIGH

    Statistical significance:
        p-value < 0.05 indicates
        statistically significant drift.
    """

    if p_value < 0.05:

        if drift_score >= 0.20:
            return "HIGH"

        if drift_score >= 0.10:
            return "MEDIUM"

        return "LOW"

    return "STABLE"


def build_details(
    feature,
    reference_mean,
    current_mean,
    drift_score,
    p_value,
    status,
):
    """Create human-readable drift explanation."""

    if reference_mean != 0:

        percentage_change = (
            (
                current_mean
                - reference_mean
            )
            / abs(reference_mean)
        ) * 100

    else:
        percentage_change = 0.0

    return (
        f"{feature} comparison: "
        f"reference mean={reference_mean:.2f}, "
        f"current mean={current_mean:.2f}, "
        f"mean change={percentage_change:.2f}%, "
        f"KS statistic={drift_score:.4f}, "
        f"p-value={p_value:.6f}, "
        f"status={status}"
    )


def save_results(results):
    """Save drift results to Snowflake."""

    connection = create_connection()
    cursor = connection.cursor()

    try:

        cursor.execute(
            "USE DATABASE SNOWGUARD_DB"
        )

        cursor.execute(
            "USE SCHEMA QUALITY"
        )

        # Clear previous drift results
        cursor.execute(
            "TRUNCATE TABLE DRIFT_RESULTS"
        )

        insert_sql = """
            INSERT INTO DRIFT_RESULTS (
                DRIFT_ID,
                DATASET_NAME,
                FEATURE_NAME,
                REFERENCE_RECORDS,
                CURRENT_RECORDS,
                REFERENCE_MEAN,
                CURRENT_MEAN,
                DRIFT_SCORE,
                DRIFT_STATUS,
                DETECTION_METHOD,
                DETAILS,
                P_VALUE
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
                %s,
                %s,
                %s
            )
        """

        for result in results:

            cursor.execute(
                insert_sql,
                (
                    str(uuid.uuid4()),
                    "TRANSACTIONS_REFERENCE_vs_CURRENT",
                    result["feature"],
                    result["reference_records"],
                    result["current_records"],
                    result["reference_mean"],
                    result["current_mean"],
                    result["drift_score"],
                    result["drift_status"],
                    "Kolmogorov-Smirnov Test",
                    result["details"],
                    result["p_value"],
                ),
            )

        connection.commit()

    finally:
        cursor.close()
        connection.close()


def run_drift_detection():
    """Run the complete SnowGuard drift detection pipeline."""

    print("\n========================================")
    print("       SNOWGUARD DRIFT DETECTOR")
    print("========================================")

    connection = create_connection()

    try:

        reference = load_table(
            connection,
            "TRANSACTIONS_REFERENCE",
        )

        current = load_table(
            connection,
            "TRANSACTIONS_CURRENT",
        )

    finally:
        connection.close()

    print(
        f"Reference records : "
        f"{len(reference)}"
    )

    print(
        f"Current records   : "
        f"{len(current)}"
    )

    results = []

    print("\nDRIFT ANALYSIS")
    print("----------------------------------------")

    for feature in FEATURES:

        result = calculate_drift(
            reference,
            current,
            feature,
        )

        if result is None:

            print(
                f"{feature:<10}"
                f" | Unable to calculate"
            )

            continue

        status = classify_drift(
            result["drift_score"],
            result["p_value"],
        )

        details = build_details(
            feature,
            result["reference_mean"],
            result["current_mean"],
            result["drift_score"],
            result["p_value"],
            status,
        )

        result["feature"] = feature
        result["drift_status"] = status
        result["details"] = details

        results.append(result)

        print(
            f"{feature:<10}"
            f" | KS={result['drift_score']:.4f}"
            f" | p={result['p_value']:.6f}"
            f" | {status}"
        )

    if not results:

        print(
            "\nNo drift results were generated."
        )

        return

    save_results(results)

    drifted_features = sum(
        1
        for result in results
        if result["drift_status"] != "STABLE"
    )

    print("----------------------------------------")

    print(
        f"Features analyzed : "
        f"{len(results)}"
    )

    print(
        f"Drifted features  : "
        f"{drifted_features}"
    )

    print("----------------------------------------")

    print(
        "Drift results saved to "
        "QUALITY.DRIFT_RESULTS! ✅"
    )

    print("========================================\n")


if __name__ == "__main__":
    run_drift_detection()