const sql = require('mssql');
const fs = require('fs');
const path = require('path');

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
    requestTimeout: 30000,
  },
};

async function run() {
  console.log(`Connecting to ${config.server}/${config.database}...`);
  const pool = await sql.connect(config);
  console.log('Connected.\n');

  // Ensure migration tracking table exists
  await pool.request().query(`
    IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'schema_migrations')
    CREATE TABLE schema_migrations (
      filename   NVARCHAR(255) NOT NULL PRIMARY KEY,
      applied_at DATETIMEOFFSET NOT NULL DEFAULT SYSDATETIMEOFFSET()
    )
  `);

  // Run all migration files in order
  const migrationsDir = path.join(__dirname, '../migrations');
  const files = fs.readdirSync(migrationsDir)
    .filter(f => f.endsWith('.sql'))
    .sort();

  for (const file of files) {
    // Check if already applied
    const check = await pool.request()
      .input('filename', sql.NVarChar(255), file)
      .query('SELECT 1 FROM schema_migrations WHERE filename = @filename');

    if (check.recordset.length > 0) {
      console.log(`  ~ skipped (already applied): ${file}`);
      continue;
    }

    console.log(`  Running: ${file}`);
    const raw = fs.readFileSync(path.join(migrationsDir, file), 'utf8');

    // Remove comment lines, split on GO batch separators then semicolons
    const stripped = raw.replace(/--[^\n]*/g, '');
    // Split on GO (T-SQL batch separator) first, then on semicolons within each batch
    const statements = stripped
      .split(/\bGO\b/i)
      .flatMap(batch => batch.split(';'))
      .map(s => s.trim())
      .filter(s => s.length > 0);

    let failed = false;
    for (const stmt of statements) {
      try {
        await pool.request().query(stmt);
        const preview = stmt.slice(0, 60).replace(/\n/g, ' ');
        console.log(`    ✓ ${preview}...`);
      } catch (err) {
        if (
          err.message.includes('already an object named') ||
          err.message.includes('already exists') ||
          err.message.includes('Column already has')
        ) {
          console.log(`    ~ skipped (already exists): ${stmt.slice(0, 50)}...`);
        } else {
          console.error(`    ✗ FAILED: ${stmt.slice(0, 80)}`);
          console.error(`      ${err.message}`);
          failed = true;
          break;
        }
      }
    }

    if (failed) {
      console.error(`\nMigration failed at: ${file}`);
      process.exit(1);
    }

    // Mark as applied
    await pool.request()
      .input('filename', sql.NVarChar(255), file)
      .query('INSERT INTO schema_migrations (filename) VALUES (@filename)');

    console.log(`  ✓ Applied: ${file}\n`);
  }

  console.log('All migrations complete.');
  await pool.close();
}

run().catch(err => {
  console.error('Fatal:', err.message);
  process.exit(1);
});
