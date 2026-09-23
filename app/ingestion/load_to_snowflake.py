import os
from pathlib import Path

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv


# ============================================================
# SNOWGUARD - LOAD CSV INTO SNOWFLAKE
# ============================================================

load_dotenv()


CSV_PATH = Path("data/raw/transactions.csv")


def create_connection():
    """Create a connection to Snowflake."""

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
    """Load transaction CSV data into Snowflake."""

    print("\n========================================")
    print("     SNOWGUARD DATA INGESTION")
    print("========================================")

    # --------------------------------------------------------
    # Load CSV
    # --------------------------------------------------------

    if not CSV_PATH.exists():
        raise FileNotFoundError(
            f"CSV file not found: {CSV_PATH}"
        )

    df = pd.read_csv(CSV_PATH)

    print(f"Local records : {len(df)}")
    print(f"Local columns : {len(df.columns)}")

    # --------------------------------------------------------
    # Connect to Snowflake
    # --------------------------------------------------------

    connection = create_connection()
    cursor = connection.cursor()

    try:
        # ----------------------------------------------------
        # Select database and schema
        # ----------------------------------------------------

        cursor.execute(
            "USE DATABASE SNOWGUARD_DB"
        )

        cursor.execute(
            "USE SCHEMA RAW"
        )

        # ----------------------------------------------------
        # Clear existing data
        # ----------------------------------------------------

        cursor.execute(
            "TRUNCATE TABLE TRANSACTIONS_RAW"
        )

        # ----------------------------------------------------
        # Insert records
        # ----------------------------------------------------

        insert_sql = """
            INSERT INTO TRANSACTIONS_RAW (
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
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s
            )
        """

        records = []

        for row in df.itertuples(index=False):
            records.append(
                (
                    row.transaction_id,
                    row.customer_id,
                    row.transaction_date,
                    None if pd.isna(row.age) else int(row.age),
                    None if pd.isna(row.income) else float(row.income),
                    None if pd.isna(row.amount) else float(row.amount),
                    None if pd.isna(row.category) else row.category,
                    row.city,
                    row.payment_method,
                )
            )

        cursor.executemany(
            insert_sql,
            records,
        )

        connection.commit()

        # ----------------------------------------------------
        # Verify Snowflake count
        # ----------------------------------------------------

        cursor.execute(
            "SELECT COUNT(*) FROM TRANSACTIONS_RAW"
        )

        snowflake_count = cursor.fetchone()[0]

        print(f"Snowflake records: {snowflake_count}")

        print("========================================")
        print("Data loaded successfully! ✅")
        print("========================================\n")

    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    load_data()