const sql = require('mssql');

const config = {
  server: process.env.AZURE_SQL_SERVER,
  database: process.env.AZURE_SQL_DATABASE,
  authentication: {
    type: 'default',
    options: {
      userName: process.env.AZURE_SQL_USER,
      password: process.env.AZURE_SQL_PASSWORD,
    },
  },
  options: {
    encrypt: true,
    trustServerCertificate: false,
    connectTimeout: 15000,
    requestTimeout: 120000, // 2 min — needed for LTS aggregation MERGE queries
  },
  pool: {
    max: 5,  // stay well under the DB's 30-connection limit
    min: 0,
    idleTimeoutMillis: 30000,
  },
};

// Module-level pool — reused across warm Azure Function invocations
let pool = null;

async function getPool() {
  if (!pool) {
    pool = await sql.connect(config);
  }
  return pool;
}

module.exports = { getPool, sql };
