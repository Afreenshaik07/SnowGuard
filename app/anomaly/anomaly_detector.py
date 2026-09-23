import os
import uuid

import numpy as np
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from sklearn.ensemble import IsolationForest
from sklearn.impute import SimpleImputer


# ============================================================
# SNOWGUARD - ML ANOMALY DETECTION ENGINE
# ============================================================

load_dotenv()


def create_connection():
    """Create Snowflake connection."""

    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database="SNOWGUARD_DB",
        schema="QUALITY",
        role=os.getenv("SNOWFLAKE_ROLE"),
    )


def load_data():
    """Load transaction data from Snowflake."""

    connection = create_connection()

    try:
        cursor = connection.cursor()

        cursor.execute(
            """
            SELECT
                TRANSACTION_ID,
                CUSTOMER_ID,
                AGE,
                INCOME,
                AMOUNT
            FROM SNOWGUARD_DB.RAW.TRANSACTIONS_RAW
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
        connection.close()


def prepare_features(df):
    """Prepare numerical features for Isolation Forest."""

    feature_columns = [
        "AGE",
        "INCOME",
        "AMOUNT",
    ]

    features = df[feature_columns].copy()

    # Replace infinite values
    features = features.replace(
        [np.inf, -np.inf],
        np.nan,
    )

    # Fill missing values with median
    imputer = SimpleImputer(
        strategy="median"
    )

    features_imputed = imputer.fit_transform(
        features
    )

    return features_imputed


def detect_anomalies(df, features):
    """Detect anomalies using Isolation Forest."""

    model = IsolationForest(
        n_estimators=200,
        contamination=0.05,
        random_state=42,
    )

    predictions = model.fit_predict(
        features
    )

    scores = model.decision_function(
        features
    )

    result = df.copy()

    result["ANOMALY_SCORE"] = scores

    result["ANOMALY_LABEL"] = np.where(
        predictions == -1,
        "ANOMALY",
        "NORMAL",
    )

    return result


def assign_severity(score):
    """Assign severity based on anomaly score."""

    if score < -0.15:
        return "HIGH"

    if score < -0.05:
        return "MEDIUM"

    return "LOW"


def generate_reason(row):
    """Generate a human-readable reason."""

    reasons = []

    if row["AMOUNT"] is not None:

        if row["AMOUNT"] > 50000:
            reasons.append(
                "Extremely high transaction amount"
            )

        elif row["AMOUNT"] < 0:
            reasons.append(
                "Negative transaction amount"
            )

    if row["AGE"] is not None:

        if row["AGE"] < 18 or row["AGE"] > 80:
            reasons.append(
                "Unusual customer age"
            )

    if row["INCOME"] is not None:

        if row["INCOME"] > 150000:
            reasons.append(
                "Unusually high income"
            )

    if not reasons:
        reasons.append(
            "Statistical deviation detected by Isolation Forest"
        )

    return "; ".join(reasons)


def save_anomalies(result):
    """Save detected anomalies to Snowflake."""

    anomalies = result[
        result["ANOMALY_LABEL"] == "ANOMALY"
    ].copy()

    connection = create_connection()
    cursor = connection.cursor()

    try:

        cursor.execute(
            "USE DATABASE SNOWGUARD_DB"
        )

        cursor.execute(
            "USE SCHEMA QUALITY"
        )

        # Clear previous detection results
        cursor.execute(
            "TRUNCATE TABLE ANOMALY_RESULTS"
        )

        insert_sql = """
            INSERT INTO ANOMALY_RESULTS (
                ANOMALY_ID,
                TRANSACTION_ID,
                CUSTOMER_ID,
                AMOUNT,
                INCOME,
                AGE,
                ANOMALY_SCORE,
                ANOMALY_LABEL,
                SEVERITY,
                DETECTION_REASON
            )
            VALUES (
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s
            )
        """

        for _, row in anomalies.iterrows():

            amount = (
                None
                if pd.isna(row["AMOUNT"])
                else float(row["AMOUNT"])
            )

            income = (
                None
                if pd.isna(row["INCOME"])
                else float(row["INCOME"])
            )

            age = (
                None
                if pd.isna(row["AGE"])
                else int(row["AGE"])
            )

            score = float(
                row["ANOMALY_SCORE"]
            )

            severity = assign_severity(
                score
            )

            reason = generate_reason(
                row
            )

            cursor.execute(
                insert_sql,
                (
                    str(uuid.uuid4()),
                    row["TRANSACTION_ID"],
                    row["CUSTOMER_ID"],
                    amount,
                    income,
                    age,
                    score,
                    "ANOMALY",
                    severity,
                    reason,
                ),
            )

        connection.commit()

        return len(anomalies)

    finally:
        cursor.close()
        connection.close()


def run_anomaly_detection():
    """Run the complete anomaly detection pipeline."""

    print("\n========================================")
    print("      SNOWGUARD ANOMALY DETECTOR")
    print("========================================")

    df = load_data()

    print(
        f"Records analyzed : {len(df)}"
    )

    features = prepare_features(df)

    result = detect_anomalies(
        df,
        features,
    )

    anomaly_count = save_anomalies(
        result
    )

    normal_count = (
        len(result) - anomaly_count
    )

    print("\nANOMALY RESULTS")
    print("----------------------------------------")
    print(
        f"Normal records   : {normal_count}"
    )
    print(
        f"Anomalies        : {anomaly_count}"
    )

    if len(result) > 0:
        anomaly_percentage = (
            anomaly_count
            / len(result)
            * 100
        )

        print(
            f"Anomaly rate     : "
            f"{anomaly_percentage:.2f}%"
        )

    print("----------------------------------------")
    print(
        "Results saved to "
        "QUALITY.ANOMALY_RESULTS ✅"
    )
    print("========================================\n")


if __name__ == "__main__":
    run_anomaly_detection()