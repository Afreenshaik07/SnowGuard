import random
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd


# ============================================================
# SNOWGUARD - TRANSACTION DATA GENERATOR
# ============================================================

random.seed(42)
np.random.seed(42)


NUM_RECORDS = 1000


CATEGORIES = [
    "Electronics",
    "Grocery",
    "Fashion",
    "Home",
    "Travel",
]

CITIES = [
    "Hyderabad",
    "Bengaluru",
    "Chennai",
    "Mumbai",
    "Delhi",
    "Pune",
]

PAYMENT_METHODS = [
    "UPI",
    "Credit Card",
    "Debit Card",
    "Net Banking",
]


def generate_transactions():
    """Generate synthetic transaction data."""

    records = []

    start_date = datetime(2026, 1, 1)

    for i in range(NUM_RECORDS):
        transaction_id = f"T{i + 1:05d}"
        customer_id = f"C{random.randint(1, 300):04d}"

        transaction_date = start_date + timedelta(
            days=random.randint(0, 260)
        )

        age = random.randint(18, 65)

        income = round(
            np.random.normal(65000, 25000),
            2,
        )

        income = max(income, 15000)

        amount = round(
            np.random.lognormal(
                mean=7.2,
                sigma=0.65,
            ),
            2,
        )

        category = random.choice(CATEGORIES)
        city = random.choice(CITIES)
        payment_method = random.choice(PAYMENT_METHODS)

        records.append(
            {
                "transaction_id": transaction_id,
                "customer_id": customer_id,
                "transaction_date": transaction_date.date(),
                "age": age,
                "income": income,
                "amount": amount,
                "category": category,
                "city": city,
                "payment_method": payment_method,
            }
        )

    return pd.DataFrame(records)


def introduce_quality_issues(df):
    """Introduce intentional data-quality problems."""

    # --------------------------------------------------------
    # 1. Missing values
    # --------------------------------------------------------

    missing_indices = np.random.choice(
        df.index,
        size=30,
        replace=False,
    )

    df.loc[missing_indices[:10], "age"] = np.nan
    df.loc[missing_indices[10:20], "income"] = np.nan
    df.loc[missing_indices[20:], "category"] = None

    # --------------------------------------------------------
    # 2. Invalid ages
    # --------------------------------------------------------

    invalid_age_indices = np.random.choice(
        df.index,
        size=8,
        replace=False,
    )

    df.loc[invalid_age_indices[:4], "age"] = 12
    df.loc[invalid_age_indices[4:], "age"] = 95

    # --------------------------------------------------------
    # 3. Negative transaction amounts
    # --------------------------------------------------------

    negative_indices = np.random.choice(
        df.index,
        size=8,
        replace=False,
    )

    df.loc[negative_indices, "amount"] *= -1

    # --------------------------------------------------------
    # 4. Extreme transaction amounts
    # --------------------------------------------------------

    anomaly_indices = np.random.choice(
        df.index,
        size=12,
        replace=False,
    )

    df.loc[anomaly_indices, "amount"] = np.random.uniform(
        50000,
        150000,
        size=len(anomaly_indices),
    )

    # --------------------------------------------------------
    # 5. Category inconsistencies
    # --------------------------------------------------------

    category_indices = np.random.choice(
        df.index,
        size=10,
        replace=False,
    )

    inconsistent_categories = [
        "electronics",
        "ELECTRONICS",
        "Grocery ",
        "fashion",
        "HOME",
    ]

    for index in category_indices:
        df.loc[index, "category"] = random.choice(
            inconsistent_categories
        )

    # --------------------------------------------------------
    # 6. Duplicate records
    # --------------------------------------------------------

    duplicates = df.sample(
        n=15,
        random_state=42,
    )

    df = pd.concat(
        [df, duplicates],
        ignore_index=True,
    )

    return df


def save_dataset(df):
    """Save dataset to data/raw."""

    output_dir = Path("data/raw")
    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = output_dir / "transactions.csv"

    df.to_csv(
        output_path,
        index=False,
    )

    print("\n========================================")
    print("       SNOWGUARD DATA GENERATOR")
    print("========================================")
    print(f"Records generated : {len(df)}")
    print(f"Columns           : {len(df.columns)}")
    print(f"Output file       : {output_path}")
    print("========================================")
    print("Dataset generated successfully! ✅")
    print("========================================\n")


def main():
    df = generate_transactions()

    df = introduce_quality_issues(df)

    save_dataset(df)


if __name__ == "__main__":
    main()