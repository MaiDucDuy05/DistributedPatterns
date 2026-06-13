const express = require('express');
const { Pool } = require('pg');

const app = express();
app.use(express.json());

// Khởi tạo 2 connection pool (Application-Level Routing)
const writePool = new Pool({
  connectionString: process.env.DATABASE_WRITE_URL || 'postgres://postgres:password@postgres-primary:5432/ecommerce'
});

const readPool = new Pool({
  connectionString: process.env.DATABASE_READ_URL || 'postgres://postgres:password@postgres-replica:5432/ecommerce'
});

// Chờ DB sẵn sàng rồi mới tạo bảng tạm (demo)
setTimeout(() => {
  writePool.query(`
    CREATE TABLE IF NOT EXISTS users (
      id SERIAL PRIMARY KEY,
      username VARCHAR(50) NOT NULL
    );
  `).catch(console.error);
}, 5000);

app.get('/api/users', async (req, res) => {
  try {
    console.log('[Routing] Gửi truy vấn SELECT tới DATABASE_READ_URL (Replica)');
    const result = await readPool.query('SELECT * FROM users');
    res.json({
      service: 'User Service (Node.js)',
      users: result.rows,
      message: 'Hello từ Node.js + PostgreSQL!'
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/users', async (req, res) => {
  try {
    const { username } = req.body;
    console.log('[Routing] Gửi truy vấn INSERT tới DATABASE_WRITE_URL (Primary)');
    const result = await writePool.query('INSERT INTO users(username) VALUES($1) RETURNING *', [username || 'user_' + Date.now()]);
    res.json(result.rows[0]);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

const PORT = 8000;
app.listen(PORT, () => console.log(`User Service running on port ${PORT}`));
