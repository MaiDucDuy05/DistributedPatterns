const amqp = require('amqplib');
const os = require('os');

const rabbitmqUrl = process.env.RABBITMQ_URL || 'amqp://guest:guest@rabbitmq:5672';
const queueName = 'sms_task_queue';
const workerName = os.hostname();

async function connectQueue() {
    try {
        console.log(`[${workerName}] Đang kết nối đến RabbitMQ...`);
        const connection = await amqp.connect(rabbitmqUrl);
        const channel = await connection.createChannel();
        
        // Khai báo queue gửi tin nhắn (Point-to-Point)
        await channel.assertQueue(queueName, { durable: true });

        // Worker chỉ lấy 1 job tại một thời điểm
        channel.prefetch(1);

        console.log(`[*] [${workerName}] Đã kết nối! Đang đợi công việc từ queue: ${queueName}`);

        channel.consume(queueName, (msg) => {
            if (msg !== null) {
                const content = msg.content.toString();
                console.log(`\n================================`);
                console.log(`[+] [${workerName}] Bắt đầu xử lý Job: ${content}`);
                
                // Giả lập xử lý chậm (ví dụ: mất 3 giây để gọi API nhà mạng)
                setTimeout(() => {
                    console.log(`[v] [${workerName}] Đã hoàn thành Job SMS!`);
                    console.log(`================================\n`);
                    channel.ack(msg);
                }, 3000); 
            }
        });
    } catch (error) {
        console.error('[-] Lỗi kết nối RabbitMQ, thử lại sau 5 giây:', error.message);
        setTimeout(connectQueue, 5000);
    }
}

connectQueue();
