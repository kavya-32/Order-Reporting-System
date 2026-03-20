"""ETL System for Book Publisher: Fetch order data from Amazon, Flipkart, and Meta Ads APIs and generate consolidated Excel report."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import logging
from pathlib import Path
import random
import time

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
LOGGER = logging.getLogger(__name__)

DEFAULT_LIMIT = 10  # Number of orders to fetch per platform
TIMEOUT = (5, 20)
FINAL_COLUMNS = [
    "Platform",
    "Order_ID",
    "Book_Title",
    "Author",
    "ISBN",
    "Unit_Price",
    "Quantity",
    "Total_Value",
    "Order_Date",
    "Customer_Region",
    "Ingested_At_UTC",
]

# Mock data generators for demonstration
# In production, replace with actual API calls

def generate_amazon_orders(limit: int) -> list[dict]:
    """Mock Amazon Selling Partner API data"""
    books = [
        {"title": "Python Programming", "author": "John Doe", "isbn": "978-0123456789", "price": 29.99},
        {"title": "Data Science Handbook", "author": "Jane Smith", "isbn": "978-0987654321", "price": 39.99},
        {"title": "Machine Learning Basics", "author": "Bob Johnson", "isbn": "978-1122334455", "price": 34.99},
    ]
    orders = []
    for i in range(limit):
        book = random.choice(books)
        orders.append({
            "order_id": f"AMZ-{1000+i}",
            "title": book["title"],
            "author": book["author"],
            "isbn": book["isbn"],
            "price": book["price"],
            "quantity": random.randint(1, 5),
            "order_date": datetime.now(timezone.utc).isoformat(),
            "region": random.choice(["US", "EU", "IN"]),
        })
    return orders

def generate_flipkart_orders(limit: int) -> list[dict]:
    """Mock Flipkart API data"""
    books = [
        {"title": "Web Development Guide", "author": "Alice Brown", "isbn": "978-5566778899", "price": 24.99},
        {"title": "AI Fundamentals", "author": "Charlie Wilson", "isbn": "978-6677889900", "price": 31.99},
        {"title": "Database Design", "author": "Diana Lee", "isbn": "978-7788990011", "price": 27.99},
    ]
    orders = []
    for i in range(limit):
        book = random.choice(books)
        orders.append({
            "order_id": f"FK-{2000+i}",
            "title": book["title"],
            "author": book["author"],
            "isbn": book["isbn"],
            "price": book["price"],
            "quantity": random.randint(1, 3),
            "order_date": datetime.now(timezone.utc).isoformat(),
            "region": "IN",
        })
    return orders

def generate_meta_ads_orders(limit: int) -> list[dict]:
    """Mock Meta Ads API data (conversions/purchases from ads)"""
    books = [
        {"title": "Digital Marketing", "author": "Eve Garcia", "isbn": "978-8899001122", "price": 26.99},
        {"title": "Social Media Strategy", "author": "Frank Miller", "isbn": "978-9900112233", "price": 22.99},
    ]
    orders = []
    for i in range(limit):
        book = random.choice(books)
        orders.append({
            "order_id": f"META-{3000+i}",
            "title": book["title"],
            "author": book["author"],
            "isbn": book["isbn"],
            "price": book["price"],
            "quantity": 1,  # Usually single purchases from ads
            "order_date": datetime.now(timezone.utc).isoformat(),
            "region": random.choice(["US", "EU", "IN", "ASIA"]),
        })
    return orders

SOURCES = [
    {
        "platform": "Amazon",
        "fetch_function": generate_amazon_orders,
        "field_map": {
            "order_id": "Order_ID",
            "title": "Book_Title",
            "author": "Author",
            "isbn": "ISBN",
            "price": "Unit_Price",
            "quantity": "Quantity",
            "order_date": "Order_Date",
            "region": "Customer_Region",
        },
    },
    {
        "platform": "Flipkart",
        "fetch_function": generate_flipkart_orders,
        "field_map": {
            "order_id": "Order_ID",
            "title": "Book_Title",
            "author": "Author",
            "isbn": "ISBN",
            "price": "Unit_Price",
            "quantity": "Quantity",
            "order_date": "Order_Date",
            "region": "Customer_Region",
        },
    },
    {
        "platform": "Meta Ads",
        "fetch_function": generate_meta_ads_orders,
        "field_map": {
            "order_id": "Order_ID",
            "title": "Book_Title",
            "author": "Author",
            "isbn": "ISBN",
            "price": "Unit_Price",
            "quantity": "Quantity",
            "order_date": "Order_Date",
            "region": "Customer_Region",
        },
    },
]


def build_session() -> requests.Session:
    """Build a requests session with retry logic (for future real API integration)"""
    retry = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def fetch_one(source: dict, limit: int) -> pd.DataFrame:
    """Fetch order data from a single platform"""
    LOGGER.info("Fetching orders from %s", source["platform"])

    # Simulate API delay
    time.sleep(random.uniform(0.5, 2.0))

    # Use mock data generator (replace with real API calls)
    records = source["fetch_function"](limit)

    if not records:
        LOGGER.warning("No records fetched from %s", source["platform"])
        return pd.DataFrame()

    df = pd.DataFrame.from_records(records)
    df = df.rename(columns=source["field_map"]).copy()
    df["Platform"] = source["platform"]
    df["Total_Value"] = (df["Unit_Price"] * df["Quantity"]).round(2)
    df["Ingested_At_UTC"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    return df


def consolidate(frames: list[pd.DataFrame]) -> pd.DataFrame:
    """Consolidate data from all platforms into a single DataFrame"""
    if not frames:
        raise ValueError("No source data available to consolidate.")

    df = pd.concat(frames, ignore_index=True, copy=False)

    # Data type conversions and cleaning
    df["Order_ID"] = df["Order_ID"].astype("string")
    df["Book_Title"] = df["Book_Title"].astype("string").str.strip()
    df["Author"] = df["Author"].astype("string").str.strip()
    df["ISBN"] = df["ISBN"].astype("string").str.strip()
    df["Unit_Price"] = pd.to_numeric(df["Unit_Price"], errors="coerce")
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(1).clip(lower=1).astype("int64")
    df["Total_Value"] = pd.to_numeric(df["Total_Value"], errors="coerce")
    df["Order_Date"] = pd.to_datetime(df["Order_Date"], errors="coerce").dt.tz_localize(None)
    df["Customer_Region"] = df["Customer_Region"].astype("string").str.strip()
    df["Ingested_At_UTC"] = pd.to_datetime(df["Ingested_At_UTC"], errors="coerce").dt.tz_localize(None)

    # Remove invalid records
    valid = (
        df["Order_ID"].notna()
        & df["Book_Title"].notna()
        & df["Book_Title"].ne("")
        & df["Unit_Price"].notna()
        & df["Total_Value"].notna()
    )
    df = df.loc[valid]

    # Remove duplicates based on Order_ID
    df = df.drop_duplicates(subset=["Platform", "Order_ID"], keep="first")

    # Sort by Order_Date descending
    df = df.sort_values("Order_Date", ascending=False)

    return df[FINAL_COLUMNS]


def write_excel(df: pd.DataFrame, output_path: Path) -> Path:
    try:
        df.to_excel(output_path, index=False)
        return output_path
    except PermissionError:
        alt = output_path.with_name(
            f"{output_path.stem}_{datetime.now().strftime('%Y%m%d_%H%M%S')}{output_path.suffix}"
        )
        df.to_excel(alt, index=False)
        LOGGER.warning("Primary output is locked. Wrote fallback file: %s", alt)
        return alt


def extract_transform_load(limit: int = DEFAULT_LIMIT, output_filename: str = "ECommerce_Consolidated_Demo.xlsx") -> Path:
    LOGGER.info("Starting ETL pipeline")
    with ThreadPoolExecutor(max_workers=min(4, len(SOURCES))) as executor:
        frames = list(executor.map(fetch_one, SOURCES, [limit] * len(SOURCES)))

    result = consolidate(frames)
    output_path = write_excel(result, Path(output_filename).resolve())
    LOGGER.info("ETL complete. Rows: %s | Output: %s", len(result), output_path)
    return output_path


if __name__ == "__main__":
    extract_transform_load()