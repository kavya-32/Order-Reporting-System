# 📊 Book Publisher Order Reporting System

A real-time ETL system that consolidates order data from multiple sources (using free public APIs for demo) into a single Excel report.

## Features

- ✅ **Multi-Source Integration**: Fetches real data from free public APIs (DummyJSON)
  - **Amazon**: Book products from DummyJSON
  - **Flipkart**: Furniture products from DummyJSON (simulated)
  - **Meta Ads**: General products from DummyJSON (simulated)
- ⏰ **Time-Based Filtering**: Fetch orders from last 30 minutes, 1 hour, or 1 day
- 📊 **Data Consolidation**: Unified Excel reports with clean, structured data
- 🌐 **Streamlit Dashboard**: Web-based UI for easy report generation
- 🔓 **No API Keys Required**: Uses free public DummyJSON API

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Run Locally

```bash
# Run the Streamlit app
streamlit run app.py
```

Visit `http://localhost:8501` in your browser.

## Usage

1. **Select Time Period**: Choose between "Last 1 Hour", "Last 30 Min", or "Last 1 Day"
2. **Optionally Rename Report**: Enter a custom filename (default: `Book_Orders_Report.xlsx`)
3. **Generate Report**: Click "🚀 Generate Report"
4. **View Summary**: See total orders, revenue, and platform breakdown
5. **Download**: Download the Excel file with consolidated order data

## Data Sources

### Free Public APIs Used

- **Amazon Orders**: Books category from [DummyJSON Products API](https://dummyjson.com/products?category=books)
- **Flipkart Orders**: Furniture category from [DummyJSON Products API](https://dummyjson.com/products?category=furniture)
- **Meta Ads Orders**: All products from [DummyJSON Products API](https://dummyjson.com/products)

Each API call is **completely free** and **requires no authentication**.

## Deployment on Streamlit Cloud

1. Push code to GitHub (already done)
2. Go to https://share.streamlit.io
3. Connect your GitHub account
4. Create new app:
   - Repository: `kavya-32/Order-Reporting-System`
   - Branch: `main`
   - Main file: `app.py`
5. Click **Deploy** - it will work immediately (no secrets needed!)

## Project Structure

```
.
├── app.py                    # Streamlit web interface
├── main.py                   # ETL pipeline (fetch, transform, load)
├── requirements.txt          # Python dependencies
├── .gitignore               # Git ignore rules
├── .streamlit/
│   └── config.toml          # Streamlit configuration
└── README.md                # This file
```

## How It Works

### 1. Fetch
- Makes HTTP requests to free DummyJSON API
- DummyJSON provides realistic product data
- Generates synthetic orders by randomly repeating products
- No API authentication needed & 100% free

### 2. Transform
- Standardizes data across platforms
- Converts data types (dates, prices, quantities)
- Validates and removes invalid records
- Deduplicates orders
- Sorts by order date (newest first)

### 3. Load
- Writes consolidated data to Excel
- Handles file lock issues with timestamped fallback names
- Logs all operations

## Using Real APIs (Optional)

To connect to **real e-commerce APIs** instead of dummy data:

### Amazon Selling Partner API
```python
# Install: pip install boto3
AWS_ACCESS_KEY_ID=your_aws_key
AWS_SECRET_ACCESS_KEY=your_aws_secret
AWS_REGION=us-east-1
```

### Flipkart Seller API
```python
FLIPKART_CLIENT_ID=your_flipkart_id
FLIPKART_CLIENT_SECRET=your_flipkart_secret
```

### Meta Conversions API
```python
META_ACCESS_TOKEN=your_meta_token
META_AD_ACCOUNT_ID=your_account_id
```

See `.env.example` for all environment variables.

## Tech Stack

- **Python 3.9+**
- **Streamlit** - Web UI
- **Pandas** - Data processing
- **Requests** - HTTP API calls
- **openpyxl** - Excel export
- **DummyJSON** - Free public API for demo data

## Error Handling

- If API is unavailable → returns empty list (graceful degradation)
- All API timeouts are handled with retry logic
- Comprehensive error logging for debugging

## Development

### Running locally:
```bash
python main.py          # Generate report directly
streamlit run app.py    # Run web interface
```

### Adding new data sources:
1. Create a `fetch_new_source()` function
2. Add it to the `SOURCES` list
3. Include proper error handling

## License

MIT

## Support

For issues:
1. Check logs in Streamlit Cloud → Manage App → Logs
2. Verify internet connection (APIs require network access)
3. Ensure dependencies installed: `pip install -r requirements.txt`

## API Credits

- 🙏 [DummyJSON](https://dummyjson.com) - Free public API providing realistic product data
