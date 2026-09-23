import os
import uuid

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv


# ============================================================
# SNOWGUARD - DATA QUALITY ENGINE
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
        schema="RAW",
        role=os.getenv("SNOWFLAKE_ROLE"),
    )


def load_data():
    """Load transaction data from Snowflake without pandas SQL warning."""

    connection = create_connection()

    try:
        cursor = connection.cursor()

        cursor.execute(
            """
            SELECT
                TRANSACTION_ID,
                CUSTOMER_ID,
                TRANSACTION_DATE,
                AGE,
                INCOME,
                AMOUNT,
                CATEGORY,
                CITY,
                PAYMENT_METHOD
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


def check_missing_values(df):
    """Check missing values."""

    missing_count = int(
        df.isnull().sum().sum()
    )

    return {
        "check_name": "Missing Values",
        "affected_records": missing_count,
        "details": (
            f"Total missing values detected: "
            f"{missing_count}"
        ),
    }


def check_duplicates(df):
    """Check duplicate transaction IDs."""

    duplicate_count = int(
        df.duplicated(
            subset=["TRANSACTION_ID"]
        ).sum()
    )

    return {
        "check_name": "Duplicate Transactions",
        "affected_records": duplicate_count,
        "details": (
            f"Duplicate transaction IDs detected: "
            f"{duplicate_count}"
        ),
    }


def check_invalid_ages(df):
    """Check ages outside the valid range."""

    invalid_age_count = int(
        (
            (df["AGE"].notna())
            & (
                (df["AGE"] < 18)
                | (df["AGE"] > 80)
            )
        ).sum()
    )

    return {
        "check_name": "Invalid Ages",
        "affected_records": invalid_age_count,
        "details": (
            f"Age values outside valid range: "
            f"{invalid_age_count}"
        ),
    }


def check_negative_amounts(df):
    """Check negative transaction amounts."""

    negative_count = int(
        (
            df["AMOUNT"].notna()
            & (df["AMOUNT"] < 0)
        ).sum()
    )

    return {
        "check_name": "Negative Amounts",
        "affected_records": negative_count,
        "details": (
            f"Negative transaction amounts detected: "
            f"{negative_count}"
        ),
    }


def check_category_consistency(df):
    """
    Detect category formatting inconsistencies.

    Expected canonical values:
    Electronics, Grocery, Fashion, Home, Travel
    """

    valid_categories = {
        "Electronics",
        "Grocery",
        "Fashion",
        "Home",
        "Travel",
    }

    categories = df["CATEGORY"]

    inconsistent_count = 0

    for value in categories.dropna():

        value = str(value)

        normalized = value.strip().lower()

        matching_category = next(
            (
                category
                for category in valid_categories
                if category.lower() == normalized
            ),
            None,
        )

        if (
            matching_category is None
            or value != matching_category
        ):
            inconsistent_count += 1

    return {
        "check_name": "Category Consistency",
        "affected_records": inconsistent_count,
        "details": (
            f"Inconsistent category formatting detected: "
            f"{inconsistent_count}"
        ),
    }


def calculate_quality_score(
    results,
    total_records,
):
    """
    Calculate quality score as the average
    pass percentage across all quality checks.
    """

    if total_records == 0:
        return 0.0

    check_scores = []

    for result in results:

        affected = result["affected_records"]

        check_score = max(
            0.0,
            100.0
            - (
                affected
                / total_records
                * 100.0
            ),
        )

        check_scores.append(
            check_score
        )

    overall_score = sum(
        check_scores
    ) / len(check_scores)

    return round(
        overall_score,
        2,
    )


def get_status(affected_records):
    """Determine status for an individual check."""

    if affected_records == 0:
        return "PASS"

    if affected_records <= 10:
        return "WARNING"

    return "FAIL"


def save_results(
    results,
    total_records,
    quality_score,
):
    """Save quality results to Snowflake."""

    connection = create_connection()
    cursor = connection.cursor()

    try:

        cursor.execute(
            "USE DATABASE SNOWGUARD_DB"
        )

        cursor.execute(
            "USE SCHEMA QUALITY"
        )

        insert_sql = """
            INSERT INTO QUALITY_RESULTS (
                CHECK_ID,
                DATASET_NAME,
                CHECK_NAME,
                TOTAL_RECORDS,
                AFFECTED_RECORDS,
                QUALITY_SCORE,
                STATUS,
                DETAILS
            )
            VALUES (
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

            status = get_status(
                result["affected_records"]
            )

            cursor.execute(
                insert_sql,
                (
                    str(uuid.uuid4()),
                    "TRANSACTIONS_RAW",
                    result["check_name"],
                    total_records,
                    result["affected_records"],
                    quality_score,
                    status,
                    result["details"],
                ),
            )

        connection.commit()

    finally:
        cursor.close()
        connection.close()


def run_quality_checks():
    """Run all SnowGuard quality checks."""

    print("\n========================================")
    print("       SNOWGUARD DATA QUALITY ENGINE")
    print("========================================")

    df = load_data()

    total_records = len(df)

    print(f"Records analyzed : {total_records}")

    results = [
        check_missing_values(df),
        check_duplicates(df),
        check_invalid_ages(df),
        check_negative_amounts(df),
        check_category_consistency(df),
    ]

    quality_score = calculate_quality_score(
        results,
        total_records,
    )

    print("\nDATA QUALITY RESULTS")
    print("----------------------------------------")

    for result in results:

        print(
            f"{result['check_name']:<25}"
            f": {result['affected_records']}"
        )

    print("----------------------------------------")

    print(
        f"Overall Quality Score : "
        f"{quality_score}%"
    )

    if quality_score >= 95:
        overall_status = "GOOD"

    elif quality_score >= 85:
        overall_status = "WARNING"

    else:
        overall_status = "CRITICAL"

    print(
        f"Overall Status        : "
        f"{overall_status}"
    )

    save_results(
        results,
        total_records,
        quality_score,
    )

    print("========================================")
    print(
        "Quality results saved to Snowflake! ✅"
    )
    print("========================================\n")


if __name__ == "__main__":
    run_quality_checks()