import time
from deltalake import DeltaTable

def clean_data_lake():
    print("🧹 Starting Delta Lake Sweeper...")
    
    try:
        dt = DeltaTable("./delta_youtube_table")
        
        # Calculate exactly 30 days ago in seconds
        thirty_days_ago = time.time() - (30 * 24 * 60 * 60)
        
        print("🔍 Searching for records older than 30 days...")
        dt.delete(f"timestamp < {thirty_days_ago}")
        
        print("🗑️ Vacuuming old files to free up disk space...")
        dt.vacuum(retention_hours=0, enforce_retention_duration=False)
        
        print("✅ Data Lake successfully cleaned! Only the last 30 days remain.")

    except Exception as e:
        print(f"🚨 Could not clean data. Is the pipeline running? Error: {e}")

if __name__ == "__main__":
    clean_data_lake()