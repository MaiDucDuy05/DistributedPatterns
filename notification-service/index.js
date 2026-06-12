const amqp = require('amqplib');

const rabbitmqUrl = process.env.RABBITMQ_URL || 'amqp://guest:guest@rabbitmq:5672';
const queueName = 'order_queue';

async function connectQueue() {
    try {
        console.log('Đang kết nối đến RabbitMQ...');
        const connection = await amqp.connect(rabbitmqUrl);
        const channel = await connection.createChannel();
        
        await channel.assertQueue(queueName, { durable: true });

        console.log(`[*] Đã kết nối! Đang lắng nghe message từ queue: ${queueName}`);

        channel.consume(queueName, (msg) => {
            if (msg !== null) {
                const content = msg.content.toString();
                console.log(`\n================================`);
                console.log(`[+] Nhận message: ${content}`);
                console.log(`[>] Giả lập gửi SMS thông báo...`);
                setTimeout(() => {
                    console.log(`[v] Gửi SMS thành công!`);
                    console.log(`================================\n`);
                    channel.ack(msg);
                }, 1000); // Giả lập delay gửi SMS
            }
        });
    } catch (error) {
        console.error('[-] Lỗi kết nối RabbitMQ, thử lại sau 5 giây:', error.message);
        setTimeout(connectQueue, 5000);
    }
}

connectQueue();
