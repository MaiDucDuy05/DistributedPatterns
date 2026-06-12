const express = require('express');
const { Pool } = require('pg');

const app = express();
app.use(express.json());

// Kết nối PostgreSQL
const pool = new Pool({
  connectionString: process.env.DATABASE_URL || 'postgres://postgres:password@postgres:5432/ecommerce'
});

// Chờ DB sẵn sàng rồi mới tạo bảng tạm (demo)
setTimeout(() => {
  pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      id SERIAL PRIMARY KEY,
      username VARCHAR(50) NOT NULL
    );
  `).catch(console.error);
}, 5000);

app.get('/api/users', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM users');
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
    const result = await pool.query('INSERT INTO users(username) VALUES($1) RETURNING *', [username || 'user_' + Date.now()]);
    res.json(result.rows[0]);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

const PORT = 8000;
app.listen(PORT, () => console.log(`User Service running on port ${PORT}`));
