import streamlit as st
from main import extract_transform_load
import requests
from datetime import datetime, timedelta
import os

st.set_page_config(page_title="Book Publisher Reports", page_icon="📊", layout="wide")

st.title("📊 Book Publisher Order Reporting System")

st.markdown("""
This system fetches real order data from:
- **Amazon**: Sales Partner API
- **Flipkart**: Marketplace API
- **Meta Ads**: Conversions API

Generate a consolidated Excel report with clean, structured data.
""")

# Initialize session state
if "time_period" not in st.session_state:
    st.session_state.time_period = "1_hour"

# Configuration options
st.subheader("⏰ Select Time Period")
col1, col2, col3 = st.columns([2, 2, 2])
with col1:
    st.session_state.time_period = st.radio(
        "Choose period:",
        options=["30_min", "1_hour", "1_day"],
        format_func=lambda x: {"30_min": "Last 30 Min", "1_hour": "Last 1 Hour", "1_day": "Last 1 Day"}[x],
        key="time_period_radio"
    )
with col2:
    filename = st.text_input("Report filename", value="Book_Orders_Report.xlsx")

# Display selected period
period_labels = {"30_min": "last 30 minutes", "1_hour": "last 1 hour", "1_day": "last 1 day"}
st.info(f"📅 Ready to fetch orders from {period_labels[st.session_state.time_period]}")

if st.button("🚀 Generate Report", type="primary", use_container_width=True):
    with st.spinner("Fetching data from platforms..."):
        try:
            path = extract_transform_load(
                time_period=st.session_state.time_period, 
                output_filename=filename
            )
            st.success(f"✅ Report generated successfully!")

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
            st.dataframe(platform_stats.style.format({'Revenue': '${:.2f}'}), use_container_width=True)

            # Download button
            with open(path, "rb") as f:
                file_data = f.read()
                
            st.download_button(
                label="📥 Download Excel Report",
                data=file_data,
                file_name=filename,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )

        except Exception as e:
            st.error(f"❌ Error generating report: {str(e)}")

st.markdown("---")
st.markdown("**Note**: This system connects to real APIs. For demo, ensure API credentials are in environment variables or Streamlit secrets.")
