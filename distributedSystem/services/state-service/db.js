const { Pool } = require("pg");

const pool = new Pool({
  user: "admin",
  host: "localhost",   // keep this
  database: "machines",
  password: "admin",
  port: 5433,          // your docker mapped port
});

pool.connect()
  .then(() => console.log("✅ DB CONNECTED SUCCESSFULLY"))
  .catch(err => console.error("❌ DB CONNECTION FAILED:", err.message));

module.exports = pool;