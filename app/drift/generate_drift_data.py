import os

import numpy as np
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv


# ============================================================
# SNOWGUARD - DRIFT DATA GENERATOR
# ============================================================

load_dotenv()

RANDOM_STATE = 42


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


def load_raw_data():
    """Load raw transactions from Snowflake."""

    connection = create_connection()

    cursor = connection.cursor()

    try:
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

        df = pd.DataFrame(
            rows,
            columns=columns,
        )

        return df

    finally:
        cursor.close()
        connection.close()


def create_reference_data(df):
    """Create clean reference distribution."""

    reference = df.copy()

    # --------------------------------------------------------
    # Remove duplicate transaction IDs
    # --------------------------------------------------------

    reference = reference.drop_duplicates(
        subset=["TRANSACTION_ID"]
    )

    # --------------------------------------------------------
    # Remove invalid ages
    # --------------------------------------------------------

    reference = reference[
        reference["AGE"].between(
            18,
            80,
        )
        | reference["AGE"].isna()
    ]

    # --------------------------------------------------------
    # Remove negative transaction amounts
    # --------------------------------------------------------

    reference = reference[
        (reference["AMOUNT"] >= 0)
        | reference["AMOUNT"].isna()
    ]

    # --------------------------------------------------------
    # Reset index
    # --------------------------------------------------------

    reference = reference.reset_index(
        drop=True
    )

    return reference


def create_current_data(reference):
    """
    Create a current dataset with controlled
    distribution changes.
    """

    current = reference.copy()

    rng = np.random.default_rng(
        RANDOM_STATE
    )

    # ========================================================
    # AMOUNT DRIFT
    # Increase transaction amounts by approximately 70%
    # ========================================================

    amount_mask = current["AMOUNT"].notna()

    current.loc[amount_mask, "AMOUNT"] = (
        current.loc[amount_mask, "AMOUNT"]
        * rng.normal(
            1.70,
            0.10,
            amount_mask.sum(),
        )
    )

    # ========================================================
    # INCOME DRIFT
    # Increase income by approximately 25%
    # ========================================================

    income_mask = current["INCOME"].notna()

    current.loc[income_mask, "INCOME"] = (
        current.loc[income_mask, "INCOME"]
        * rng.normal(
            1.25,
            0.05,
            income_mask.sum(),
        )
    )

    # ========================================================
    # AGE DRIFT
    # Shift valid ages slightly upward
    # ========================================================

    age_mask = current["AGE"].notna()

    current.loc[age_mask, "AGE"] = (
        current.loc[age_mask, "AGE"]
        + rng.choice(
            [0, 1, 2, 3],
            size=age_mask.sum(),
            p=[
                0.25,
                0.30,
                0.30,
                0.15,
            ],
        )
    )

    current.loc[age_mask, "AGE"] = (
        current.loc[age_mask, "AGE"]
        .clip(
            18,
            80,
        )
    )

    return current


def convert_to_python_value(value):
    """
    Convert Pandas/NumPy values into values
    that Snowflake Connector can safely handle.

    NaN / NaT / missing values become Python None,
    which Snowflake inserts as SQL NULL.
    """

    if pd.isna(value):
        return None

    if isinstance(
        value,
        np.integer,
    ):
        return int(value)

    if isinstance(
        value,
        np.floating,
    ):
        return float(value)

    if isinstance(
        value,
        np.bool_,
    ):
        return bool(value)

    return value


def prepare_records(df):
    """
    Convert DataFrame into Snowflake-safe records.
    """

    records = []

    for row in df.itertuples(
        index=False
    ):

        record = (
            convert_to_python_value(
                row.TRANSACTION_ID
            ),

            convert_to_python_value(
                row.CUSTOMER_ID
            ),

            convert_to_python_value(
                row.TRANSACTION_DATE
            ),

            convert_to_python_value(
                row.AGE
            ),

            convert_to_python_value(
                row.INCOME
            ),

            convert_to_python_value(
                row.AMOUNT
            ),

            convert_to_python_value(
                row.CATEGORY
            ),

            convert_to_python_value(
                row.CITY
            ),

            convert_to_python_value(
                row.PAYMENT_METHOD
            ),
        )

        records.append(record)

    return records


def upload_dataframe(
    connection,
    df,
    table_name,
):
    """Upload DataFrame records into Snowflake."""

    cursor = connection.cursor()

    try:

        # ----------------------------------------------------
        # Clear previous data
        # ----------------------------------------------------

        cursor.execute(
            f"TRUNCATE TABLE {table_name}"
        )

        # ----------------------------------------------------
        # Insert statement
        # ----------------------------------------------------

        insert_sql = f"""
            INSERT INTO {table_name} (
                TRANSACTION_ID,
                CUSTOMER_ID,
                TRANSACTION_DATE,
                AGE,
                INCOME,
                AMOUNT,
                CATEGORY,
                CITY,
                PAYMENT_METHOD
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
                %s
            )
        """

        # ----------------------------------------------------
        # Convert DataFrame to Snowflake-safe records
        # ----------------------------------------------------

        records = prepare_records(df)

        # ----------------------------------------------------
        # Insert records
        # ----------------------------------------------------

        cursor.executemany(
            insert_sql,
            records,
        )

        connection.commit()

    finally:
        cursor.close()


def verify_tables(connection):
    """Verify uploaded record counts."""

    cursor = connection.cursor()

    try:

        print("\nVERIFYING SNOWFLAKE TABLES")
        print("----------------------------------------")

        tables = [
            "TRANSACTIONS_REFERENCE",
            "TRANSACTIONS_CURRENT",
        ]

        for table in tables:

            cursor.execute(
                f"""
                SELECT COUNT(*)
                FROM {table}
                """
            )

            count = cursor.fetchone()[0]

            print(
                f"{table:<25}: {count}"
            )

    finally:
        cursor.close()


def print_statistics(
    reference,
    current,
):
    """Print reference/current statistics."""

    print("\nDRIFT DATA SUMMARY")
    print("----------------------------------------")

    print(
        f"{'FEATURE':<15}"
        f"{'REFERENCE MEAN':>20}"
        f"{'CURRENT MEAN':>20}"
    )

    print("-" * 55)

    features = [
        "AGE",
        "INCOME",
        "AMOUNT",
    ]

    for feature in features:

        reference_mean = (
            reference[feature]
            .mean()
        )

        current_mean = (
            current[feature]
            .mean()
        )

        print(
            f"{feature:<15}"
            f"{reference_mean:>20.2f}"
            f"{current_mean:>20.2f}"
        )


def main():
    """Generate and upload reference/current datasets."""

    print("\n========================================")
    print("     SNOWGUARD DRIFT DATA GENERATOR")
    print("========================================")

    # ========================================================
    # STEP 1 - Load raw data
    # ========================================================

    raw_data = load_raw_data()

    print(
        f"Raw records loaded : "
        f"{len(raw_data)}"
    )

    # ========================================================
    # STEP 2 - Create reference dataset
    # ========================================================

    reference = create_reference_data(
        raw_data
    )

    # ========================================================
    # STEP 3 - Create shifted current dataset
    # ========================================================

    current = create_current_data(
        reference
    )

    print(
        f"Reference records  : "
        f"{len(reference)}"
    )

    print(
        f"Current records    : "
        f"{len(current)}"
    )

    # ========================================================
    # STEP 4 - Display expected drift
    # ========================================================

    print_statistics(
        reference,
        current,
    )

    # ========================================================
    # STEP 5 - Connect to Snowflake
    # ========================================================

    connection = create_connection()

    try:

        # ----------------------------------------------------
        # Upload reference dataset
        # ----------------------------------------------------

        upload_dataframe(
            connection,
            reference,
            "TRANSACTIONS_REFERENCE",
        )

        # ----------------------------------------------------
        # Upload current dataset
        # ----------------------------------------------------

        upload_dataframe(
            connection,
            current,
            "TRANSACTIONS_CURRENT",
        )

        # ----------------------------------------------------
        # Verify uploaded records
        # ----------------------------------------------------

        verify_tables(
            connection
        )

    finally:
        connection.close()

    # ========================================================
    # SUCCESS
    # ========================================================

    print("----------------------------------------")

    print(
        "Reference and current datasets "
        "uploaded successfully! ✅"
    )

    print("========================================\n")


if __name__ == "__main__":
    main()