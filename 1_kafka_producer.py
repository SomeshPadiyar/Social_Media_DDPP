import time
import json
import requests
import concurrent.futures
from datetime import datetime, timedelta
from confluent_kafka import Producer

# ==========================================
# 1. API AND KAFKA CONFIGURATION
# ==========================================
# IMPORTANT: Paste your real AIzaSy... key inside these quotes!
YOUTUBE_API_KEY = '' 
KAFKA_TOPIC = 'youtube_stream'

conf = {
    'bootstrap.servers': 'localhost:9092', 
    'client.id': 'mac-m4-producer'
}
producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")

def fetch_single_category(category):
    """Fetches Top 50 trending for a single category"""
    url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics&chart=mostPopular&regionCode=US&videoCategoryId={category}&maxResults=50&key={YOUTUBE_API_KEY}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json().get('items', [])
    except Exception as e:
        print(f"🚨 Error fetching category {category}: {e}")
    return []

def fetch_youtube_data():
    print("📡 Firing 13 simultaneous API requests to YouTube...")
    categories = ["1", "2", "10", "15", "17", "20", "22", "23", "24", "25", "26", "27", "28"]
    all_videos = []
    
    # Open 13 simultaneous network threads to bypass the speed limit
    with concurrent.futures.ThreadPoolExecutor(max_workers=13) as executor:
        results = executor.map(fetch_single_category, categories)
        for items in results:
            if items:
                all_videos.extend(items)
            
    # Remove redundancy (if a video is trending in both 'Music' and 'Entertainment')
    unique_videos = {v['id']: v for v in all_videos}.values()
    return list(unique_videos)

def stream_data():
    print("🚀 Starting High-Volume Data Stream...")
    
    while True:
        videos = fetch_youtube_data()
        count = 0
        
        for video in videos:
            payload = {
                "video_id": video['id'],
                "title": video['snippet']['title'],
                "category_id": video['snippet']['categoryId'],
                "views": int(video['statistics'].get('viewCount', 0)),
                "likes": int(video['statistics'].get('likeCount', 0)),
                "comments": int(video['statistics'].get('commentCount', 0)),
                "timestamp": time.time() 
            }
            producer.produce(KAFKA_TOPIC, key=payload['video_id'], value=json.dumps(payload).encode('utf-8'), callback=delivery_report)
            count += 1
            
        producer.flush()
        print(f"✅ Pushed {count} unique videos to Kafka.")
        print(f"⏳ Sleeping for 30 minutes... (Next fetch at {(datetime.now() + timedelta(minutes=30)).strftime('%H:%M:%S')})\n")
        time.sleep(1800) 

if __name__ == "__main__":
    stream_data()
