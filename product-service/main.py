from fastapi import FastAPI
import pymongo
import redis
import json
import os

app = FastAPI()

# Kết nối MongoDB
MONGO_URL = os.getenv("MONGO_URL", "mongodb://mongo:27017/")
client = pymongo.MongoClient(MONGO_URL)
db = client["ecommerce"]
collection = db["products"]

# Kết nối Redis
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
redis_client = redis.from_url(REDIS_URL, decode_responses=True)

@app.on_event("startup")
def startup_db_client():
    # Thêm dữ liệu mẫu nếu DB trống
    try:
        if collection.count_documents({}) == 0:
            collection.insert_many([
                {"name": "Laptop Dell XPS 15", "price": 2000},
                {"name": "iPhone 15 Pro Max", "price": 1200},
                {"name": "Bàn phím cơ Keychron", "price": 100}
            ])
            print("Đã thêm dữ liệu mẫu vào MongoDB")
    except Exception as e:
        print(f"Lỗi kết nối MongoDB: {e}")

@app.get("/api/products")
def get_products():
    try:
        cache_key = "products:all"
        
        # 1. Kiểm tra Cache (Redis) trước
        cached_data = redis_client.get(cache_key)
        if cached_data:
            print("[CACHE HIT] Lấy dữ liệu từ Redis")
            return {
                "service": "Product Service (Python FastAPI)",
                "source": "Redis Cache",
                "products": json.loads(cached_data)
            }
            
        print("[CACHE MISS] Lấy dữ liệu từ MongoDB")
        # 2. Nếu Cache Miss, gọi DB (MongoDB)
        products = []
        for p in collection.find():
            p["_id"] = str(p["_id"])
            products.append(p)
            
        # 3. Lưu vào Cache với TTL = 60s
        redis_client.setex(cache_key, 60, json.dumps(products))
        
        return {
            "service": "Product Service (Python FastAPI)",
            "source": "MongoDB Database",
            "products": products
        }
    except Exception as e:
        return {"error": str(e)}
