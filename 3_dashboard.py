import streamlit as st
import pandas as pd
from deltalake import DeltaTable
import os

st.set_page_config(page_title="Big Data Social Analytics", layout="wide")
st.title("Interactive YouTube Analytics Dashboard")

CATEGORY_MAPPING = {
    "1": "Film & Animation", "2": "Autos & Vehicles", "10": "Music",
    "15": "Pets & Animals", "17": "Sports", "19": "Travel & Events",
    "20": "Gaming", "22": "People & Blogs", "23": "Comedy",
    "24": "Entertainment", "25": "News & Politics", "26": "Howto & Style",
    "27": "Education", "28": "Science & Technology"
}

@st.cache_data(ttl=60) 
def load_data():
    if not os.path.exists("./delta_youtube_table"):
        return pd.DataFrame()
    dt = DeltaTable("./delta_youtube_table")
    df = dt.to_pandas()
    
    # Time conversion
    df['date'] = pd.to_datetime(df['timestamp'], unit='s').dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata')
    
    # Map categories and immediately DESTROY the "Unknown" data
    df['category_name'] = df['category_id'].astype(str).map(CATEGORY_MAPPING).fillna("Unknown")
    df = df[df['category_name'] != "Unknown"] # <-- This completely bans 'Unknown' from the dashboard
    
    return df

raw_df = load_data()

if not raw_df.empty:
    # ==========================================
    # INTERACTIVE SIDEBAR FILTERS
    # ==========================================
    st.sidebar.header("🔍 Filter Data")
    
    days_to_keep = st.sidebar.slider("Time Range (Days)", min_value=1, max_value=30, value=7)
    cutoff_date = pd.Timestamp.now(tz='Asia/Kolkata') - pd.Timedelta(days=days_to_keep)
    
    search_query = st.sidebar.text_input("Search Video Titles:", "")
    
    categories = ["All"] + list(raw_df['category_name'].unique())
    selected_category = st.sidebar.selectbox("Category", categories)

    # ==========================================
    # APPLY FILTERS & DEDUPLICATION
    # ==========================================
    filtered_df = raw_df[raw_df['date'] >= cutoff_date]
    
    if search_query:
        filtered_df = filtered_df[filtered_df['title'].str.contains(search_query, case=False, na=False)]
    
    if selected_category != "All":
        filtered_df = filtered_df[filtered_df['category_name'] == selected_category]

    # Keep only the newest record for each video
    filtered_df = filtered_df.drop_duplicates(subset=['title'], keep='last').copy()

    # Calculate Engagement Rate: (Likes + Comments) / Views * 100
    filtered_df['engagement_rate'] = ((filtered_df['likes'] + filtered_df['comments']) / filtered_df['views']) * 100

    # ==========================================
    # VISUALIZATIONS
    # ==========================================
    st.success(f"Showing {len(filtered_df):,} unique videos based on your filters.")
    
    # Top Level Metrics
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Filtered Views", f"{int(filtered_df['views'].sum()):,}")
    col2.metric("Total Filtered Likes", f"{int(filtered_df['likes'].sum()):,}")
    col3.metric("Avg Comments per Video", f"{int(filtered_df['comments'].mean() if not filtered_df.empty else 0):,}")

    st.markdown("---")

    # Layout for the new Analytical Charts
    chart_col1, chart_col2 = st.columns(2)

    with chart_col1:
        st.subheader("📊 Average Views by Genre")
        st.caption("Identifies which category reaches the widest audience.")
        if not filtered_df.empty:
            avg_views_df = filtered_df.groupby("category_name")["views"].mean().sort_values(ascending=False)
            st.bar_chart(avg_views_df)

    with chart_col2:
        st.subheader("🔥 Engagement Rate (%) by Genre")
        st.caption("Identifies which category has the most active/loyal fans.")
        if not filtered_df.empty:
            avg_engagement_df = filtered_df.groupby("category_name")["engagement_rate"].mean().sort_values(ascending=False)
            st.bar_chart(avg_engagement_df)

    st.markdown("---")

    st.subheader("📈 Total Views Over Time")
    if not filtered_df.empty:
        time_chart_data = filtered_df.groupby(filtered_df['date'].dt.date)['views'].sum()
        st.line_chart(time_chart_data)

    st.subheader("Filtered Raw Data")
    # Format the engagement rate to 2 decimal places for clean viewing
    display_df = filtered_df[['title', 'category_name', 'views', 'likes', 'engagement_rate', 'date']].sort_values(by="views", ascending=False)
    display_df['engagement_rate'] = display_df['engagement_rate'].round(2).astype(str) + "%"
    
    st.dataframe(display_df.head(50))

else:
    st.warning("No data available. Pipeline is empty.")