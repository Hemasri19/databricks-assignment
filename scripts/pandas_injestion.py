import pandas as pd
import os
from glob import glob
from utils.logger import get_logger

logger = get_logger(__name__)

EXPECTED_COLUMNS = {
    "order_id",
    "order_date",
    "product_id",
    "customer_id",
    "region",
    "quantity",
    "unit_price",
    "cost_price"
}


def validate_schema(df):
    missing_cols = EXPECTED_COLUMNS - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing columns: {missing_cols}")


def run_ingestion():

    # project directories
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze")
    SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")

    os.makedirs(SILVER_DIR, exist_ok=True)

    logger.info("Current working directory: %s", os.getcwd())
    logger.info("Resolved BASE_DIR: %s", BASE_DIR)
    logger.info("Reading raw files from: %s", BRONZE_DIR)

    files = glob(os.path.join(BRONZE_DIR, "*"))
    all_data = []

    logger.info("Reading raw files...")

    for file in files:
        if file.endswith(".csv"):
            df = pd.read_csv(file)
        elif file.endswith(".xlsx"):
            df = pd.read_excel(file)
        else:
            continue

        df.columns = df.columns.str.lower().str.strip()
        validate_schema(df)
        all_data.append(df)

    if not all_data:
        raise ValueError("No valid files found in Bronze layer.")

    df = pd.concat(all_data, ignore_index=True)

    logger.info("Combined shape: %s", df.shape)

    # Cleaning
    df.drop_duplicates(inplace=True)
    df.fillna(0, inplace=True)
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")

    # Derived Metrics
    df["revenue"] = df["quantity"] * df["unit_price"]
    df["profit"] = df["revenue"] - (df["quantity"] * df["cost_price"])
    df["profit_margin"] = df["profit"] / df["revenue"]

    # Pandas GroupBy
    regional_summary = (
        df.groupby("region")
        .agg(
            total_revenue=("revenue", "sum"),
            total_profit=("profit", "sum"),
            total_orders=("order_id", "count")
        )
        .reset_index()
    )

    logger.info("Regional Summary (Pandas):")
    logger.info("\n%s", regional_summary)

    # Fix timestamp precision for Spark
    # -------------------
    for col in df.select_dtypes(include=["datetime64[ns]"]).columns:
        df[col] = df[col].astype("datetime64[us]")

    # -------------------
    # Save Silver Layer
    # -------------------
    silver_path = os.path.join(SILVER_DIR, "sales_silver.parquet")

    df.to_parquet(silver_path, index=False)

    logger.info("Silver layer saved successfully at: %s", silver_path)
    logger.info("Files now in silver folder: %s", os.listdir(SILVER_DIR))

    raw_path = "data/bronze/"
    files = glob(os.path.join(raw_path, "*"))

    all_data = []

    logger.info("Reading raw files...")

    for file in files:
        if file.endswith(".csv"):
            df = pd.read_csv(file)
        elif file.endswith(".xlsx"):
            df = pd.read_excel(file)
        else:
            continue

        df.columns = df.columns.str.lower().str.strip()
        validate_schema(df)
        all_data.append(df)

    if not all_data:
        raise ValueError("No valid files found.")

    df = pd.concat(all_data, ignore_index=True)

    logger.info("Combined shape: %s", df.shape)

    # Cleaning
    df.drop_duplicates(inplace=True)
    df.fillna(0, inplace=True)
    df["order_date"] = pd.to_datetime(df["order_date"])

    # Derived Metrics
    df["revenue"] = df["quantity"] * df["unit_price"]
    df["profit"] = df["revenue"] - (df["quantity"] * df["cost_price"])
    df["profit_margin"] = df["profit"] / df["revenue"]

    # Pandas GroupBy (Mandatory)
    regional_summary = (
        df.groupby("region")
        .agg(
            total_revenue=("revenue", "sum"),
            total_profit=("profit", "sum"),
            total_orders=("order_id", "count")
        )
        .reset_index()
    )

    logger.info("Regional Summary (Pandas):")
    logger.info("\n%s", regional_summary)

    os.makedirs("data/silver", exist_ok=True)
    silver_path = os.path.join("data/silver", "sales_silver.parquet")

    logger.info("Current working directory: %s", os.getcwd())
    logger.info("Files in silver folder: %s", os.listdir("data/silver"))

    # Convert all datetime columns to microsecond precision
    for col in df.select_dtypes(include=["datetime64[ns]"]).columns:
        df[col] = df[col].astype("datetime64[us]")

    df.to_parquet("data/silver/sales_silver.parquet", index=False)

    logger.info("Silver layer created successfully.")