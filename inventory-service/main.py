import pika
import time
import os
import sys

def connect_with_retry():
    url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/%2F")
    while True:
        try:
            print("Đang kết nối đến RabbitMQ (Inventory)...")
            connection = pika.BlockingConnection(pika.URLParameters(url))
            return connection
        except Exception as e:
            print(f"Lỗi kết nối RabbitMQ: {e}. Thử lại sau 5s...")
            time.sleep(5)

def main():
    connection = connect_with_retry()
    channel = connection.channel()

    # Khai báo exchange dạng fanout (Pub/Sub)
    exchange_name = 'order_events'
    channel.exchange_declare(exchange=exchange_name, exchange_type='fanout', durable=True)

    # Khai báo queue riêng của Inventory Service
    queue_name = 'inventory_queue'
    channel.queue_declare(queue=queue_name, durable=True)

    # Bind queue vào exchange
    channel.queue_bind(exchange=exchange_name, queue=queue_name)

    print(f"[*] Đã kết nối. Inventory Service đang lắng nghe {queue_name} (bind vào {exchange_name})...")

    def callback(ch, method, properties, body):
        message = body.decode()
        print("\n================================")
        print(f"[+] Inventory nhận sự kiện (Pub/Sub): {message}")
        print("[>] Đang cập nhật cơ sở dữ liệu: Trừ số lượng tồn kho...")
        time.sleep(1) # Giả lập update DB
        print("[v] Đã trừ kho thành công!")
        print("================================\n")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=queue_name, on_message_callback=callback)

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("Interrupted")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)

# ==================================================
# API ĐỒNG BỘ ĐỂ DEMO CIRCUIT BREAKER TỪ SPRING BOOT
# ==================================================
import threading
import uvicorn
from fastapi import FastAPI, HTTPException
import random

app = FastAPI()

@app.get("/api/inventory/check")
def check_inventory():
    print("\n[FastAPI] Nhận request kiểm tra kho từ Order Service...")
    # Cố tình ném lỗi 50% số lần hoặc chạy chậm để Spring Boot Circuit Breaker phải ngắt Cầu Dao
    chance = random.random()
    if chance < 0.3:
        print("[FastAPI] (Cố tình) Ném lỗi 500!")
        raise HTTPException(status_code=500, detail="Lỗi DB ngẫu nhiên do hệ thống giả lập.")
    elif chance < 0.6:
        print("[FastAPI] (Cố tình) Ngủ 6 giây để Spring Boot timeout!")
        time.sleep(6)
    
    print("[FastAPI] Trả về: Hàng còn trong kho!")
    return {"status": "ok", "message": "Hàng còn trong kho"}

def start_fastapi():
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="error")

if __name__ == '__main__':
    # Chạy FastAPI ở background thread
    t = threading.Thread(target=start_fastapi, daemon=True)
    t.start()
    
    # Chạy RabbitMQ consumer ở thread chính
    main()
