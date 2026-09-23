import os
import uuid

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv


# ============================================================
# SNOWGUARD - ROOT CAUSE ANALYSIS ENGINE
# ============================================================

load_dotenv()


# Features where drift was detected
FEATURES = [
    "INCOME",
    "AMOUNT",
]


# Dimensions used for root-cause analysis
DIMENSIONS = [
    "CATEGORY",
    "CITY",
    "PAYMENT_METHOD",
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
    """
    Load transaction data from Snowflake
    and normalize categorical columns.
    """

    cursor = connection.cursor()

    try:

        cursor.execute(
            f"""
            SELECT
                TRANSACTION_ID,
                CUSTOMER_ID,
                AGE,
                INCOME,
                AMOUNT,
                CATEGORY,
                CITY,
                PAYMENT_METHOD
            FROM SNOWGUARD_DB.PROCESSED.{table_name}
            """
        )

        rows = cursor.fetchall()

        columns = [
            column[0]
            for column in cursor.description
        ]

        df = pd.DataFrame(
            rows,
            columns=columns,
        )

        # ====================================================
        # NORMALIZE CATEGORICAL DIMENSIONS
        # ====================================================
        #
        # This prevents separate groups such as:
        #
        # Grocery
        # grocery
        # GROCERY
        # Grocery
        #
        # from being treated as different categories.
        # ====================================================

        for column in [
            "CATEGORY",
            "CITY",
            "PAYMENT_METHOD",
        ]:

            df[column] = (
                df[column]
                .astype("string")
                .str.strip()
                .str.title()
            )

        return df

    finally:
        cursor.close()


def calculate_segment_analysis(
    reference,
    current,
    feature,
    dimension,
):
    """
    Compare reference and current values
    for each segment of a dimension.
    """

    # ========================================================
    # REFERENCE GROUP
    # ========================================================

    reference_group = (
        reference
        .groupby(dimension)[feature]
        .agg(
            [
                "count",
                "mean",
            ]
        )
        .reset_index()
    )

    reference_group = (
        reference_group.rename(
            columns={
                "count": "REFERENCE_COUNT",
                "mean": "REFERENCE_MEAN",
            }
        )
    )

    # ========================================================
    # CURRENT GROUP
    # ========================================================

    current_group = (
        current
        .groupby(dimension)[feature]
        .agg(
            [
                "count",
                "mean",
            ]
        )
        .reset_index()
    )

    current_group = (
        current_group.rename(
            columns={
                "count": "CURRENT_COUNT",
                "mean": "CURRENT_MEAN",
            }
        )
    )

    # ========================================================
    # MERGE REFERENCE + CURRENT
    # ========================================================

    merged = reference_group.merge(
        current_group,
        on=dimension,
        how="outer",
    )

    # ========================================================
    # HANDLE MISSING VALUES
    # ========================================================

    merged["REFERENCE_COUNT"] = (
        merged["REFERENCE_COUNT"]
        .fillna(0)
    )

    merged["CURRENT_COUNT"] = (
        merged["CURRENT_COUNT"]
        .fillna(0)
    )

    merged["REFERENCE_MEAN"] = (
        merged["REFERENCE_MEAN"]
        .fillna(0)
    )

    merged["CURRENT_MEAN"] = (
        merged["CURRENT_MEAN"]
        .fillna(0)
    )

    # ========================================================
    # PERCENTAGE CHANGE
    # ========================================================

    merged["CHANGE_PERCENT"] = 0.0

    non_zero = (
        merged["REFERENCE_MEAN"] != 0
    )

    merged.loc[
        non_zero,
        "CHANGE_PERCENT",
    ] = (
        (
            merged.loc[
                non_zero,
                "CURRENT_MEAN",
            ]
            - merged.loc[
                non_zero,
                "REFERENCE_MEAN",
            ]
        )
        / merged.loc[
            non_zero,
            "REFERENCE_MEAN",
        ].abs()
        * 100
    )

    # ========================================================
    # ABSOLUTE CHANGE
    # ========================================================

    merged["ABS_CHANGE"] = (
        merged["CHANGE_PERCENT"]
        .abs()
    )

    # ========================================================
    # CONTRIBUTION PERCENTAGE
    # ========================================================

    total_change = (
        merged["ABS_CHANGE"]
        .sum()
    )

    if total_change > 0:

        merged["CONTRIBUTION_PERCENT"] = (
            merged["ABS_CHANGE"]
            / total_change
            * 100
        )

    else:

        merged["CONTRIBUTION_PERCENT"] = 0.0

    # ========================================================
    # SORT BY CONTRIBUTION
    # ========================================================

    return merged.sort_values(
        "ABS_CHANGE",
        ascending=False,
    )


def classify_segment(change_percent):
    """Classify RCA severity."""

    absolute_change = abs(
        change_percent
    )

    if absolute_change >= 20:
        return "HIGH"

    if absolute_change >= 10:
        return "MEDIUM"

    return "LOW"


def build_results(
    analysis,
    feature,
    dimension,
):
    """Convert segment analysis into result records."""

    results = []

    for _, row in analysis.iterrows():

        change = float(
            row["CHANGE_PERCENT"]
        )

        contribution = float(
            row["CONTRIBUTION_PERCENT"]
        )

        status = classify_segment(
            change
        )

        # ====================================================
        # DETERMINE DIRECTION
        # ====================================================

        if change > 0:

            direction = "increased"

        elif change < 0:

            direction = "decreased"

        else:

            direction = "remained stable"

        # ====================================================
        # BUILD EXPLANATION
        # ====================================================

        details = (
            f"{feature} for "
            f"{dimension}="
            f"{row[dimension]} "
            f"{direction} by "
            f"{abs(change):.2f}%. "
            f"Reference mean="
            f"{row['REFERENCE_MEAN']:.2f}, "
            f"current mean="
            f"{row['CURRENT_MEAN']:.2f}."
        )

        results.append(
            {
                "feature": feature,

                "dimension": dimension,

                "dimension_value": str(
                    row[dimension]
                ),

                "reference_mean": float(
                    row["REFERENCE_MEAN"]
                ),

                "current_mean": float(
                    row["CURRENT_MEAN"]
                ),

                "change_percent": change,

                "contribution_percent": (
                    contribution
                ),

                "status": status,

                "details": details,
            }
        )

    return results


def save_results(results):
    """Save RCA results into Snowflake."""

    connection = create_connection()

    cursor = connection.cursor()

    try:

        cursor.execute(
            "USE DATABASE SNOWGUARD_DB"
        )

        cursor.execute(
            "USE SCHEMA ANALYTICS"
        )

        # ====================================================
        # CLEAR PREVIOUS RESULTS
        # ====================================================

        cursor.execute(
            "TRUNCATE TABLE ROOT_CAUSE_RESULTS"
        )

        # ====================================================
        # INSERT STATEMENT
        # ====================================================

        insert_sql = """
            INSERT INTO ROOT_CAUSE_RESULTS (
                RCA_ID,
                FEATURE_NAME,
                DIMENSION_NAME,
                DIMENSION_VALUE,
                REFERENCE_MEAN,
                CURRENT_MEAN,
                CHANGE_PERCENT,
                CONTRIBUTION_PERCENT,
                RCA_STATUS,
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
                %s,
                %s,
                %s
            )
        """

        # ====================================================
        # INSERT RESULTS
        # ====================================================

        for result in results:

            cursor.execute(
                insert_sql,
                (
                    str(
                        uuid.uuid4()
                    ),

                    result["feature"],

                    result["dimension"],

                    result[
                        "dimension_value"
                    ],

                    result[
                        "reference_mean"
                    ],

                    result[
                        "current_mean"
                    ],

                    result[
                        "change_percent"
                    ],

                    result[
                        "contribution_percent"
                    ],

                    result["status"],

                    result["details"],
                ),
            )

        connection.commit()

    finally:

        cursor.close()
        connection.close()


def run_root_cause_analysis():
    """Run the complete RCA pipeline."""

    print("\n========================================")
    print("       SNOWGUARD ROOT CAUSE ANALYSIS")
    print("========================================")

    # ========================================================
    # LOAD DATA
    # ========================================================

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

    # ========================================================
    # NORMALIZATION SUMMARY
    # ========================================================

    print("\nNORMALIZED DIMENSIONS")
    print("----------------------------------------")

    for dimension in DIMENSIONS:

        reference_values = (
            reference[dimension]
            .dropna()
            .nunique()
        )

        current_values = (
            current[dimension]
            .dropna()
            .nunique()
        )

        print(
            f"{dimension:<18}"
            f"Reference={reference_values}"
            f" | Current={current_values}"
        )

    # ========================================================
    # RCA ANALYSIS
    # ========================================================

    all_results = []

    for feature in FEATURES:

        print(
            f"\nFEATURE: {feature}"
        )

        print("----------------------------------------")

        for dimension in DIMENSIONS:

            analysis = (
                calculate_segment_analysis(
                    reference,
                    current,
                    feature,
                    dimension,
                )
            )

            results = build_results(
                analysis,
                feature,
                dimension,
            )

            all_results.extend(
                results
            )

            # =================================================
            # TOP 3 CONTRIBUTORS
            # =================================================

            top_results = results[:3]

            for result in top_results:

                print(
                    f"{dimension:<15}"
                    f"{result['dimension_value']:<18}"
                    f"Change="
                    f"{result['change_percent']:+.2f}%"
                    f" | Contribution="
                    f"{result['contribution_percent']:.2f}%"
                    f" | {result['status']}"
                )

    # ========================================================
    # SAVE RESULTS
    # ========================================================

    save_results(
        all_results
    )

    print("\n----------------------------------------")

    print(
        f"RCA records generated : "
        f"{len(all_results)}"
    )

    print(
        "Root cause results saved to "
        "ANALYTICS.ROOT_CAUSE_RESULTS! ✅"
    )

    print("========================================\n")


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    run_root_cause_analysis()