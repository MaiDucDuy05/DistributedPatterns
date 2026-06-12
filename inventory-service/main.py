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

if __name__ == '__main__':
    main()
