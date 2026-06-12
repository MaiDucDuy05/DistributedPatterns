const amqp = require('amqplib');

const rabbitmqUrl = process.env.RABBITMQ_URL || 'amqp://guest:guest@rabbitmq:5672';
const exchangeName = 'order_events';
const queueName = 'notification_queue';
const smsQueueName = 'sms_task_queue';

async function connectQueue() {
    try {
        console.log('Đang kết nối đến RabbitMQ (Notification)...');
        const connection = await amqp.connect(rabbitmqUrl);
        const channel = await connection.createChannel();
        
        // 1. Lắng nghe từ Pub/Sub (Exchange)
        await channel.assertExchange(exchangeName, 'fanout', { durable: false });
        await channel.assertQueue(queueName, { durable: true });
        await channel.bindQueue(queueName, exchangeName, ''); // Bind queue vào exchange

        // 2. Khai báo Queue Point-to-Point để giao việc cho SMS Worker
        await channel.assertQueue(smsQueueName, { durable: true });

        console.log(`[*] Đã kết nối! Đang lắng nghe từ Pub/Sub (exchange: ${exchangeName}) qua queue: ${queueName}`);

        channel.consume(queueName, (msg) => {
            if (msg !== null) {
                const content = msg.content.toString();
                console.log(`\n================================`);
                console.log(`[+] Nhận event từ Order: ${content}`);
                
                // Đẩy task vào queue của SMS Worker (Point-to-Point MQ)
                const smsJob = `Nhiệm vụ gửi SMS cho đơn hàng: ${content}`;
                channel.sendToQueue(smsQueueName, Buffer.from(smsJob), { persistent: true });
                console.log(`[>] Đã đẩy task gửi SMS vào MQ (queue: ${smsQueueName}) cho Worker xử lý`);
                console.log(`================================\n`);
                
                channel.ack(msg);
            }
        });
    } catch (error) {
        console.error('[-] Lỗi kết nối RabbitMQ, thử lại sau 5 giây:', error.message);
        setTimeout(connectQueue, 5000);
    }
}

connectQueue();
