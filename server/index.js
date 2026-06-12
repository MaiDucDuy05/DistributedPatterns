const express = require('express');
const { createClient } = require('redis');

const app = express();
const PORT = process.env.PORT || 8000;
const SERVER_NAME = process.env.SERVER_NAME || 'unknown';

// Kết nối Redis (dùng chung cho cả S1 và S2)
const redis = createClient({
  url: `redis://${process.env.REDIS_HOST || 'redis'}:6379`
});

redis.connect().catch(console.error);

// Endpoint chính — trả tên server + counter từ Redis
app.get('/api', async (req, res) => {
  const count = await redis.incr('global_counter');
  res.json({
    server: SERVER_NAME,          // Để biết request đang vào S1 hay S2
    counter: count,               // Counter dùng chung qua Redis
    timestamp: new Date().toISOString()
  });
});

// Health check endpoint cho Nginx
app.get('/health', (req, res) => {
  res.json({ status: 'ok', server: SERVER_NAME });
});

app.listen(PORT, () => {
  console.log(`${SERVER_NAME} running on port ${PORT}`);
});
