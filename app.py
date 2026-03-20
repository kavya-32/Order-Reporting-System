import streamlit as st
from main import extract_transform_load

st.title("� Book Publisher Order Reporting System")

st.markdown("""
This system fetches order data from:
- **Amazon**: Selling Partner API (mock data)
- **Flipkart**: Marketplace API (mock data)
- **Meta Ads**: Conversions API (mock data)

Generate a consolidated Excel report with clean, structured data.
""")

# Configuration options
col1, col2 = st.columns(2)
with col1:
    limit = st.slider("Orders per platform", min_value=5, max_value=50, value=10, step=5)
with col2:
    filename = st.text_input("Report filename", value="Book_Orders_Report.xlsx")

if st.button("🚀 Generate Report", type="primary"):
    with st.spinner("Fetching data from platforms..."):
        try:
            path = extract_transform_load(limit=limit, output_filename=filename)
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
st.markdown("**Note**: This demo uses mock data. For production use, integrate with actual APIs using proper authentication.")