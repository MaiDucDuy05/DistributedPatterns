const express = require('express');
const { Pool } = require('pg');
const crypto = require('crypto');

const app = express();
app.use(express.json());

// ==========================================
// THUẬT TOÁN CONSISTENT HASHING RING
// ==========================================
class ConsistentHashingRouter {
  constructor(nodes, virtualNodes = 100) {
    this.nodes = nodes; // Các cụm Shard (vd: 'Shard_A', 'Shard_B', 'Shard_C')
    this.virtualNodes = virtualNodes; // Số lượng node ảo để phân phối đều
    this.ring = new Map();
    this.keys = [];
    
    this._buildRing();
  }

  _hash(str) {
    // Mã hóa chuỗi thành mã hex sử dụng MD5
    return crypto.createHash('md5').update(str).digest('hex');
  }

  _buildRing() {
    for (const node of this.nodes) {
      for (let i = 0; i < this.virtualNodes; i++) {
        const hash = this._hash(`${node}:${i}`);
        this.ring.set(hash, node);
        this.keys.push(hash);
      }
    }
    // Sắp xếp các điểm trên vòng tròn băm
    this.keys.sort();
  }

  getShard(key) {
    if (this.keys.length === 0) return null;
    const hash = this._hash(key);
    
    // Tìm điểm (node) đầu tiên trên vòng tròn có giá trị hash >= hash của key
    for (const k of this.keys) {
      if (hash <= k) {
        return this.ring.get(k);
      }
    }
    // Nếu vượt qua điểm cuối cùng, quay lại điểm đầu tiên (Ring)
    return this.ring.get(this.keys[0]);
  }
}

// ==========================================
// TÍCH HỢP REDIS (Cache Aside)
// ==========================================
const { createClient } = require('redis');
const redisClient = createClient({
  url: process.env.REDIS_URL || 'redis://redis:6379/0'
});

redisClient.on('error', (err) => console.log('Redis Client Error', err));
redisClient.connect().then(() => console.log('Connected to Redis'));

// ==========================================
// CẤU HÌNH CÁC SHARD THỰC TẾ
// ==========================================
const shards = {
  'Shard_A': {
    write: new Pool({ connectionString: process.env.DATABASE_SHARD_A_WRITE_URL || 'postgres://postgres:password@shard-a-primary:5432/ecommerce' }),
    read: new Pool({ connectionString: process.env.DATABASE_SHARD_A_READ_URL || 'postgres://postgres:password@shard-a-replica:5432/ecommerce' })
  },
  'Shard_B': {
    write: new Pool({ connectionString: process.env.DATABASE_SHARD_B_WRITE_URL || 'postgres://postgres:password@shard-b-primary:5432/ecommerce' }),
    read: new Pool({ connectionString: process.env.DATABASE_SHARD_B_READ_URL || 'postgres://postgres:password@shard-b-replica:5432/ecommerce' })
  },
  'Shard_C': {
    write: new Pool({ connectionString: process.env.DATABASE_SHARD_C_WRITE_URL || 'postgres://postgres:password@shard-c-primary:5432/ecommerce' }),
    read: new Pool({ connectionString: process.env.DATABASE_SHARD_C_READ_URL || 'postgres://postgres:password@shard-c-replica:5432/ecommerce' })
  }
};

// Khởi tạo Bộ định tuyến với 3 Shard
const router = new ConsistentHashingRouter(Object.keys(shards));

// Khởi tạo bảng dữ liệu trên TẤT CẢ các Shard
setTimeout(() => {
  for (const [shardName, shardPools] of Object.entries(shards)) {
    shardPools.write.query(`
      CREATE TABLE IF NOT EXISTS sharded_users (
        id VARCHAR(36) PRIMARY KEY,
        username VARCHAR(50) NOT NULL
      );
    `).then(() => console.log(`[Init] Đã tạo bảng sharded_users trên ${shardName}`))
      .catch(console.error);
  }
}, 5000);


// ==========================================
// API ENDPOINTS
// ==========================================

// TẠO USER (Sinh UUID & Định tuyến Ghi)
app.post('/api/users', async (req, res) => {
  try {
    const { username } = req.body;
    
    // Bước 1: Tầng Code chủ động sinh UUID (Distributed ID)
    const userId = crypto.randomUUID(); 
    
    // Bước 2: Băm UUID để tìm ra Shard đích
    const targetShard = router.getShard(userId); 
    
    console.log(`[Sharding] UUID: ${userId} -> Thuộc về: ${targetShard}`);
    console.log(`[Routing] Gửi lệnh INSERT tới ${targetShard} (Primary)`);
    
    // Lấy đúng Write Pool của Shard đó
    const pool = shards[targetShard].write;
    const result = await pool.query(
      'INSERT INTO sharded_users(id, username) VALUES($1, $2) RETURNING *', 
      [userId, username || 'user_' + Date.now()]
    );
    
    res.json({ 
      message: 'Created successfully',
      shard: targetShard, 
      user: result.rows[0] 
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// LẤY THÔNG TIN 1 USER (Tìm Shard, Cache Aside & Định tuyến Đọc)
app.get('/api/users/:id', async (req, res) => {
  try {
    const userId = req.params.id;

    // BƯỚC 1: Bọc Lót Bộ Nhớ Đệm (Cache Aside Pattern)
    const cacheKey = `user:${userId}`;
    const cachedUser = await redisClient.get(cacheKey);
    if (cachedUser) {
      console.log(`[Cache] HIT - Trả về dữ liệu từ Redis cho UUID: ${userId}`);
      return res.json({ 
        message: 'Fetched from Cache',
        source: 'Redis',
        user: JSON.parse(cachedUser) 
      });
    }

    console.log(`[Cache] MISS - Không tìm thấy UUID: ${userId} trong Redis. Đi xuống Database.`);
    
    // BƯỚC 2: Nếu Cache Miss, tìm Shard bằng hàm Băm giống hệt lúc tạo
    const targetShard = router.getShard(userId);
    
    console.log(`[Sharding] Tìm UUID: ${userId} -> Nằm ở: ${targetShard}`);
    console.log(`[Routing] Gửi lệnh SELECT tới ${targetShard} (Replica)`);
    
    // BƯỚC 3: Lấy đúng Read Pool của Shard đó
    const pool = shards[targetShard].read;
    const result = await pool.query('SELECT * FROM sharded_users WHERE id = $1', [userId]);
    
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'User not found in ' + targetShard });
    }
    
    const user = result.rows[0];

    // BƯỚC 4: Ghi ngược lại lên Redis với TTL 60 giây
    await redisClient.set(cacheKey, JSON.stringify(user), { EX: 60 });
    console.log(`[Cache] Lưu dữ liệu vào Redis cho UUID: ${userId} (TTL: 60s)`);

    res.json({ 
      message: 'Fetched from Database',
      source: targetShard, 
      user: user 
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

const PORT = 8000;
app.listen(PORT, () => console.log(`User Service running on port ${PORT}`));
