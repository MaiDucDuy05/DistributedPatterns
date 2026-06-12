# RabbitMQ Microservices Architecture

Dự án này minh họa việc sử dụng **RabbitMQ** để giao tiếp giữa các Microservices bằng cách kết hợp cả hai mô hình **Pub/Sub** (Publish/Subscribe) và **Point-to-Point** (Message Queue).

## Kiến trúc hệ thống
Hệ thống bao gồm các thành phần sau:
- **API Gateway (Traefik)**: Điều hướng request.
- **Order Service (Java Spring Boot)**: Publisher. Khi có đơn hàng, phát event `order.created` vào Fanout Exchange.
- **Inventory Service (Python)**: Subscriber. Lắng nghe event từ Fanout Exchange để trừ số lượng tồn kho.
- **Notification Service (Node.js)**: Subscriber & Producer. Lắng nghe event từ Fanout Exchange, sau đó đẩy một Message (SMS Task) vào Message Queue Point-to-Point.
- **SMS Workers (Node.js)**: Consumer. Các worker chạy song song lấy task từ Message Queue để thực thi việc gửi SMS.
- **Databases**: MongoDB & PostgreSQL.

## Kiến thức đã áp dụng
1. Cấu hình cơ bản Docker Compose cho Microservices.
2. Xử lý lỗi kết nối Docker daemon từ Traefik bằng cách map đúng Unix socket (`/var/run/docker.sock`).
3. Sửa lỗi Default Exchange của RabbitMQ khi queue không tồn tại (Spring Boot AMQP).
4. Áp dụng mô hình **Pub/Sub** (Fanout Exchange) để phát sóng sự kiện độc lập cho nhiều Domain khác nhau (Inventory & Notification).
5. Áp dụng mô hình **Point-to-Point MQ** (Direct Queue) để chia tải công việc nặng (Gửi SMS) ra nhiều background workers.
