# 📊 Book Publisher Order Reporting System

A real-time ETL system that consolidates order data from multiple e-commerce platforms (Amazon, Flipkart, Meta Ads) into a single Excel report.

## Features

- ✅ **Multi-Platform Integration**: Amazon Selling Partner API, Flipkart Marketplace API, Meta Conversions API
- ⏰ **Time-Based Filtering**: Fetch orders from last 30 minutes, 1 hour, or 1 day
- 📊 **Data Consolidation**: Unified Excel reports with clean, structured data
- 🔄 **Real API Support**: Seamless integration with actual APIs (with mock data fallback)
- 🌐 **Streamlit Dashboard**: Web-based UI for easy report generation

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure API Credentials

Create a `.env` file in the project root (copy from `.env.example`):

```bash
cp .env.example .env
```

Then add your actual API credentials:

#### Amazon Selling Partner API
- Visit: https://developer.amazon.com/
- Register your application and get:
  - `AWS_ACCESS_KEY_ID`
  - `AWS_SECRET_ACCESS_KEY`
  - `AWS_REGION`

#### Flipkart API
- Contact: Flipkart seller support or https://seller.flipkart.com/
- Get:
  - `FLIPKART_CLIENT_ID`
  - `FLIPKART_CLIENT_SECRET`

#### Meta Ads API
- Visit: https://developers.facebook.com/
- Create an app and get:
  - `META_ACCESS_TOKEN` (from your app's settings)
  - `META_AD_ACCOUNT_ID` (from Ads Manager)

### 3. Run Locally

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

## Deployment on Streamlit Cloud

1. Push code to GitHub (already done)
2. Go to https://share.streamlit.io
3. Connect your GitHub account
4. Create new app:
   - Repository: `kavya-32/Order-Reporting-System`
   - Branch: `main`
   - Main file: `app.py`
5. **Configure Secrets**:
   - Click "Manage App" → "Secrets"
   - Add all API credentials from `.env`

Example secrets format:
```
AWS_ACCESS_KEY_ID = "your_key_here"
AWS_SECRET_ACCESS_KEY = "your_secret_here"
AWS_REGION = "us-east-1"
FLIPKART_CLIENT_ID = "your_flipkart_id"
FLIPKART_CLIENT_SECRET = "your_flipkart_secret"
META_ACCESS_TOKEN = "your_meta_token"
META_AD_ACCOUNT_ID = "your_account_id"
```

## Project Structure

```
.
├── app.py                    # Streamlit web interface
├── main.py                   # ETL pipeline (fetch, transform, load)
├── requirements.txt          # Python dependencies
├── .env.example             # Example environment variables
├── .gitignore               # Git ignore rules
├── .streamlit/
│   └── config.toml          # Streamlit configuration
└── README.md                # This file
```

## How It Works

### 1. Fetch
- Connects to real APIs or uses mock data
- Filters by time period (1 hour, 30 min, 1 day)
- Handles API errors gracefully with fallback to mock data

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

## Error Handling

If API credentials are missing or invalid, the system automatically:
- Logs a warning
- Falls back to mock data
- Continues processing

This ensures the app never crashes, even without real API access.

## Development

### Running locally with mock data:
```bash
python main.py
```

### Testing with Streamlit:
```bash
streamlit run app.py
```

### Adding new data sources:
1. Create a `fetch_your_api_orders()` function
2. Add it to the `SOURCES` list with proper field mapping
3. Include error handling and mock data fallback

## Tech Stack

- **Python 3.9+**
- **Streamlit** - Web UI
- **Pandas** - Data processing
- **Requests** - API calls
- **openpyxl** - Excel export

## License

MIT

## Support

For issues or questions:
1. Check logs in Streamlit Cloud → Manage App → Logs
2. Verify API credentials in `.env` or Streamlit secrets
3. Ensure dependencies are installed: `pip install -r requirements.txt`
