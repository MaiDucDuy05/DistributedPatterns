from fastapi import FastAPI
import pymongo
import os

app = FastAPI()

# Kết nối MongoDB
MONGO_URL = os.getenv("MONGO_URL", "mongodb://mongo:27017/")
client = pymongo.MongoClient(MONGO_URL)
db = client["ecommerce"]
collection = db["products"]

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
        products = []
        for p in collection.find():
            p["_id"] = str(p["_id"])
            products.append(p)
        return {
            "service": "Product Service (Python FastAPI)",
            "message": "Hello từ Python + MongoDB!",
            "products": products
        }
    except Exception as e:
        return {"error": str(e)}
