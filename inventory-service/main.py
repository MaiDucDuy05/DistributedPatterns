import pika
import time
import os
import sys
import json
import random

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

    # 1. Khai báo Exchange cho Saga Pattern
    exchange_name = 'saga_exchange'
    channel.exchange_declare(exchange=exchange_name, exchange_type='topic', durable=True)

    # 2. Khai báo Queue riêng của Inventory để nghe lệnh order.created
    queue_name = 'inventory_saga_queue'
    channel.queue_declare(queue=queue_name, durable=True)

    # 3. Bind queue vào exchange với routing_key 'order.created'
    channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key='order.created')

    print(f"[*] Đã kết nối. Inventory Service đang lắng nghe sự kiện trên Queue {queue_name}...")

    def callback(ch, method, properties, body):
        try:
            event = json.loads(body.decode())
            order_id = event.get('orderId')
            product = event.get('product')

            print("\n================================")
            print(f"[+] Inventory nhận sự kiện Saga: 'order.created' cho Order: {order_id}, Product: {product}")
            print("[>] Đang kiểm tra cơ sở dữ liệu kho...")
            time.sleep(1) # Giả lập check DB

            # Giả lập 20% khả năng HẾT HÀNG
            chance = random.random()
            
            response_event = {
                "orderId": order_id,
                "product": product
            }

            if chance < 0.2:
                print("\033[91m[X] KHO BÁO HẾT HÀNG! Bắn sự kiện 'inventory.failed' \033[0m")
                response_event["type"] = "inventory.failed"
                routing_key = 'inventory.failed'
            else:
                print("\033[92m[v] TRỪ KHO THÀNH CÔNG! Bắn sự kiện 'inventory.deducted' \033[0m")
                response_event["type"] = "inventory.deducted"
                routing_key = 'inventory.deducted'

            # Phản hồi lại sự kiện lên saga_exchange để Order Service nghe
            channel.basic_publish(
                exchange=exchange_name,
                routing_key=routing_key,
                body=json.dumps(response_event),
                properties=pika.BasicProperties(
                    content_type='application/json',
                    delivery_mode=2, # make message persistent
                )
            )

            print("================================\n")
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print(f"Lỗi xử lý message: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

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

if __name__ == '__main__':
    main()
