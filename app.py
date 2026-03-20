import streamlit as st
from main import extract_transform_load
import requests
from datetime import datetime, timedelta

st.title("📊 Book Publisher Order Reporting System")

st.markdown("""
This system fetches real order data from:
- **Amazon**: Sales Partner API
- **Flipkart**: Marketplace API
- **Meta Ads**: Conversions API

Generate a consolidated Excel report with clean, structured data.
""")

# Configuration options
st.subheader("⏰ Select Time Period")
col1, col2, col3, col4 = st.columns(4)
with col1:
    last_hour = st.button("Last 1 Hour")
with col2:
    last_30min = st.button("Last 30 Min")
with col3:
    last_day = st.button("Last 1 Day")
with col4:
    filename = st.text_input("Report filename", value="Book_Orders_Report.xlsx")

# Determine time period
time_period = None
if last_hour:
    time_period = "1_hour"
    st.info("📅 Fetching orders from the last 1 hour...")
elif last_30min:
    time_period = "30_min"
    st.info("📅 Fetching orders from the last 30 minutes...")
elif last_day:
    time_period = "1_day"
    st.info("📅 Fetching orders from the last 1 day...")

if time_period and st.button("🚀 Generate Report", type="primary"):
    with st.spinner("Fetching data from platforms..."):
        try:
            path = extract_transform_load(
                time_period=time_period, 
                output_filename=filename
            )
            st.success(f"✅ Report generated successfully: {path}")

            # Display summary
            import pandas as pd
            df = pd.read_excel(path)
            st.subheader("📊 Report Summary")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Orders", len(df))
            with col2:
                st.metric("Total Revenue", f"${df['Total_Value'].sum():.2f}")
            with col3:
                st.metric("Platforms", df['Platform'].nunique())

            # Platform breakdown
            st.subheader("Platform Breakdown")
            platform_stats = df.groupby('Platform').agg({
                'Order_ID': 'count',
                'Total_Value': 'sum'
            }).rename(columns={'Order_ID': 'Orders', 'Total_Value': 'Revenue'})
            st.dataframe(platform_stats.style.format({'Revenue': '${:.2f}'}))

            # Download button
            with open(path, "rb") as f:
                st.download_button(
                    "📥 Download Excel Report",
                    f,
                    file_name=filename,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

        except Exception as e:
            st.error(f"❌ Error generating report: {str(e)}")

st.markdown("---")
st.markdown("**Note**: This system connects to real APIs. For demo purposes, ensure API credentials are configured in environment variables.")
