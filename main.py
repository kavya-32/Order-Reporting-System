"""ETL System for Book Publisher: Fetch order data from Amazon, Flipkart, and Meta Ads APIs and generate consolidated Excel report."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
import logging
from pathlib import Path
import os
import random
import time

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
LOGGER = logging.getLogger(__name__)

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


def get_time_range(time_period: str) -> tuple[datetime, datetime]:
    """Get start and end time based on time period."""
    now = datetime.now(timezone.utc)
    if time_period == "30_min":
        start = now - timedelta(minutes=30)
    elif time_period == "1_hour":
        start = now - timedelta(hours=1)
    elif time_period == "1_day":
        start = now - timedelta(days=1)
    else:
        start = now - timedelta(hours=1)  # Default to 1 hour
    return start, now


# Real API integrations

def fetch_amazon_orders(time_period: str = "1_hour") -> list[dict]:
    """Fetch real Amazon Selling Partner API data"""
    try:
        access_key = os.getenv("AWS_ACCESS_KEY_ID")
        secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        region = os.getenv("AWS_REGION", "us-east-1")
        
        if not access_key or not secret_key:
            LOGGER.warning("AWS credentials not found.")
            return []
        
        # Real Amazon Selling Partner API integration
        # Requires: pip install amazon-selling-partner-api or boto3-stubs
        import boto3
        from botocore.auth import SigV4Auth
        from botocore.awsrequest import AWSRequest
        
        session = boto3.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region
        )
        client = session.client('orders', api_version='v0')
        start_time, end_time = get_time_range(time_period)
        
        # Fetch orders from API
        response = client.get_orders(
            CreatedAfter=start_time.isoformat(),
            CreatedBefore=end_time.isoformat(),
            OrderStatuses=['Pending', 'Unshipped', 'PartiallyShipped', 'Shipped'],
            MaxResultsPerPage=100
        )
        
        orders = []
        for order in response.get('Orders', []):
            order_id = order.get('AmazonOrderId', '')
            purchase_date = order.get('PurchaseDate', '')
            
            # Get order items for book details
            items_response = client.get_order_items(AmazonOrderId=order_id)
            for item in items_response.get('OrderItems', []):
                orders.append({
                    "order_id": order_id,
                    "title": item.get('ProductName', 'Unknown'),
                    "author": "Amazon",
                    "isbn": item.get('ASIN', ''),
                    "price": float(item.get('ItemPrice', {}).get('Amount', 0)),
                    "quantity": int(item.get('Quantity', 1)),
                    "order_date": purchase_date,
                    "region": "US",
                })
        
        LOGGER.info("Fetched %d orders from Amazon API", len(orders))
        return orders
        
    except ImportError:
        LOGGER.error("boto3 not installed. Install with: pip install boto3")
        return []
    except Exception as e:
        LOGGER.error("Error fetching Amazon orders via API: %s", str(e))
        return []


def fetch_flipkart_orders(time_period: str = "1_hour") -> list[dict]:
    """Fetch real Flipkart Seller API data"""
    try:
        client_id = os.getenv("FLIPKART_CLIENT_ID")
        client_secret = os.getenv("FLIPKART_CLIENT_SECRET")
        
        if not client_id or not client_secret:
            LOGGER.warning("Flipkart credentials not found.")
            return []
        
        # Real Flipkart Seller API integration
        # Endpoint: https://api.flipkart.net/sellers/v1/
        session = build_session()
        start_time, end_time = get_time_range(time_period)
        
        headers = {
            "X-FK-APP-ID": client_id,
            "X-FK-APP-TOKEN": client_secret,
            "Content-Type": "application/json"
        }
        
        # Fetch orders from Flipkart API
        url = "https://api.flipkart.net/sellers/v1/orders"
        params = {
            "statuses": ["OMS_CONFIRMED", "OMS_SHIPPED", "OMS_DELIVERED"],
            "startDate": start_time.strftime("%Y-%m-%d"),
            "endDate": end_time.strftime("%Y-%m-%d")
        }
        
        response = session.get(url, headers=headers, params=params, timeout=TIMEOUT)
        response.raise_for_status()
        data = response.json()
        
        orders = []
        for order in data.get('orders', []):
            for item in order.get('items', []):
                orders.append({
                    "order_id": order.get('orderId', ''),
                    "title": item.get('productName', 'Unknown'),
                    "author": "Flipkart",
                    "isbn": item.get('productSku', ''),
                    "price": float(item.get('price', 0)),
                    "quantity": int(item.get('quantity', 1)),
                    "order_date": order.get('orderCreateDate', ''),
                    "region": "IN",
                })
        
        LOGGER.info("Fetched %d orders from Flipkart API", len(orders))
        return orders
        
    except Exception as e:
        LOGGER.error("Error fetching Flipkart orders via API: %s", str(e))
        return []


def fetch_meta_ads_orders(time_period: str = "1_hour") -> list[dict]:
    """Fetch real Meta Conversions API data (purchases from ads)"""
    try:
        access_token = os.getenv("META_ACCESS_TOKEN")
        ad_account_id = os.getenv("META_AD_ACCOUNT_ID")
        
        if not access_token or not ad_account_id:
            LOGGER.warning("Meta credentials not found.")
            return []
        
        # Real Meta Conversions API integration
        # Endpoint: https://graph.instagram.com/v19.0/
        session = build_session()
        start_time, end_time = get_time_range(time_period)
        
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        # Fetch conversion events (purchases) from Meta API
        url = f"https://graph.instagram.com/v19.0/{ad_account_id}/conversions"
        params = {
            "fields": "id,event_name,event_source_url,event_time,custom_data",
            "access_token": access_token,
            "date_start": start_time.strftime("%Y-%m-%d"),
            "date_stop": end_time.strftime("%Y-%m-%d")
        }
        
        response = session.get(url, headers=headers, params=params, timeout=TIMEOUT)
        response.raise_for_status()
        data = response.json()
        
        orders = []
        for event in data.get('conversions', []):
            if event.get('event_name') == 'Purchase':
                custom_data = event.get('custom_data', {})
                orders.append({
                    "order_id": f"META-{event.get('id', '')}",
                    "title": custom_data.get('content_name', 'Book'),
                    "author": "Meta Ads",
                    "isbn": custom_data.get('content_ids', [''])[0],
                    "price": float(custom_data.get('value', 0)),
                    "quantity": int(custom_data.get('content_quantity', 1)),
                    "order_date": datetime.fromtimestamp(
                        int(event.get('event_time', 0)), tz=timezone.utc
                    ).isoformat(),
                    "region": custom_data.get('state', 'Unknown'),
                })
        
        LOGGER.info("Fetched %d purchase events from Meta API", len(orders))
        return orders
        
    except Exception as e:
        LOGGER.error("Error fetching Meta orders via API: %s", str(e))
        return []


# Mock data generators for demonstration

def generate_amazon_orders_mock(limit: int) -> list[dict]:
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


def generate_flipkart_orders_mock(limit: int) -> list[dict]:
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


def generate_meta_ads_orders_mock(limit: int) -> list[dict]:
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
            "quantity": 1,
            "order_date": datetime.now(timezone.utc).isoformat(),
            "region": random.choice(["US", "EU", "IN", "ASIA"]),
        })
    return orders


SOURCES = [
    {
        "platform": "Amazon",
        "fetch_function": fetch_amazon_orders,
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
        "fetch_function": fetch_flipkart_orders,
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
        "fetch_function": fetch_meta_ads_orders,
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
    """Build a requests session with retry logic"""
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


def fetch_one(source: dict, time_period: str) -> pd.DataFrame:
    """Fetch order data from a single platform"""
    LOGGER.info("Fetching orders from %s for period: %s", source["platform"], time_period)

    # Simulate API delay
    time.sleep(random.uniform(0.5, 2.0))

    # Fetch from real or mock API
    records = source["fetch_function"](time_period)

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


def extract_transform_load(time_period: str = "1_hour", output_filename: str = "Book_Orders_Report.xlsx") -> Path:
    LOGGER.info("Starting ETL pipeline for period: %s", time_period)
    with ThreadPoolExecutor(max_workers=min(4, len(SOURCES))) as executor:
        frames = list(executor.map(lambda s: fetch_one(s, time_period), SOURCES))

    result = consolidate(frames)
    output_path = write_excel(result, Path(output_filename).resolve())
    LOGGER.info("ETL complete. Rows: %s | Output: %s", len(result), output_path)
    return output_path


if __name__ == "__main__":
    extract_transform_load(time_period="1_hour")
