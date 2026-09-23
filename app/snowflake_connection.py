import os

import snowflake.connector
from dotenv import load_dotenv


# Load environment variables from .env
load_dotenv()


def create_connection():
    """Create and return a Snowflake connection."""

    connection = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )

    return connection


def test_connection():
    """Test the Snowflake connection."""

    connection = create_connection()

    try:
        cursor = connection.cursor()

        cursor.execute(
            """
            SELECT
                CURRENT_USER(),
                CURRENT_ACCOUNT(),
                CURRENT_ROLE(),
                CURRENT_WAREHOUSE()
            """
        )

        result = cursor.fetchone()

        print("\n========================================")
        print("       SNOWGUARD CONNECTION TEST")
        print("========================================")
        print(f"User      : {result[0]}")
        print(f"Account   : {result[1]}")
        print(f"Role      : {result[2]}")
        print(f"Warehouse : {result[3]}")
        print("========================================")
        print("Connection successful! ✅")
        print("========================================\n")

    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    test_connection()