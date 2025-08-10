// orchestrator.js  (PostgreSQL)
import { spawn } from 'child_process';
import { Pool } from 'pg';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// --- DB config (Railway Postgres)
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl:
    process.env.DATABASE_URL && /sslmode=require|true/i.test(process.env.DATABASE_URL)
      ? { rejectUnauthorized: false }
      : undefined,
});

const BOOT_TIMEOUT_MS = 120_000;
const POLL_MS = 1500;

const SCRIPTS = {
  PRICE_FETCHER: path.join(__dirname, 'Script1.js'),
  EXECUTION_ENGINE: path.join(__dirname, 'solana-logic.js'),
  RISK_MANAGER: path.join(__dirname, 'MasterScript.js'),
};

// ---- health checks (note: cast COUNT(*) so pg returns numbers)
async function coinsSeeded() {
  const { rows } = await pool.query('SELECT COUNT(*)::int AS n FROM coins');
  return rows[0].n > 0;
}

// rename column from "timestamp" to "ts" if you can; otherwise quote it: "timestamp"
async function pricesFlowing() {
  const { rows } = await pool.query(
    `SELECT COUNT(*)::int AS n
     FROM current_prices
     WHERE ts > (NOW() - INTERVAL '2 minutes')`
  );
  return rows[0].n > 0;
}

async function engineLive() {
  const a = await pool.query(
    `SELECT COUNT(*)::int AS n FROM buy_signals
     WHERE created_at > (NOW() - INTERVAL '2 minutes')`
  );
  const b = await pool.query(
    `SELECT COUNT(*)::int AS n FROM trade_logs
     WHERE created_at > (NOW() - INTERVAL '2 minutes')`
  );
  return a.rows[0].n > 0 || b.rows[0].n > 0;
}

async function waitFor(condFn, timeoutMs, label) {
  const start = Date.now();
  for (;;) {
    if (await condFn()) return true;
    if (Date.now() - start > timeoutMs) throw new Error(`Timeout: ${label}`);
    await new Promise(r => setTimeout(r, POLL_MS));
  }
}

function spawnService(name, file, env = {}) {
  const child = spawn(process.execPath, [file], {
    stdio: ['ignore', 'pipe', 'pipe'],
    env: { ...process.env, NODE_OPTIONS: '--enable-source-maps', ...env },
  });
  child.stdout.on('data', d => process.stdout.write(`[${name}] ${d}`));
  child.stderr.on('data', d => process.stderr.write(`[${name}][ERR] ${d}`));
  child.on('exit', (code, sig) => {
    console.error(`[${name}] exited code=${code} sig=${sig}`);
    process.exitCode = 1;
  });
  return child;
}

async function main() {
  await pool.query('SELECT 1'); // connectivity probe

  const pf = spawnService('PRICE_FETCHER', SCRIPTS.PRICE_FETCHER);
  await waitFor(coinsSeeded, BOOT_TIMEOUT_MS, 'coins seeded');
  await waitFor(pricesFlowing, BOOT_TIMEOUT_MS, 'live prices flowing');

  const ex = spawnService('EXECUTION_ENGINE', SCRIPTS.EXECUTION_ENGINE);
  await waitFor(engineLive, BOOT_TIMEOUT_MS, 'execution engine live');

  const rm = spawnService('RISK_MANAGER', SCRIPTS.RISK_MANAGER);

  const shutdown = () => {
    [pf, ex, rm].forEach(p => { try { p.kill('SIGINT'); } catch {} });
    setTimeout(() => process.exit(0), 3000);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
  console.log('All services started.');
}

main().catch(e => { console.error('Orchestrator fatal:', e); process.exit(1); });
