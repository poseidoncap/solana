import axios from 'axios';
import Big from 'big.js';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { Wallet } from '@project-serum/anchor';
import { Connection, Keypair, } from '@solana/web3.js';
import bs58 from 'bs58';
import dotenv from 'dotenv';
import https from 'https';
import { Pool } from 'pg';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const FETCH_INTERVAL = 12000;  // 12 seconds
const SAVE_INTERVAL = 20000;   // 20 seconds
const MAX_PRICE_HISTORY = 1000;
const COOLDOWN_PERIOD = 20 * 60 * 1000;  // 20 minutes in milliseconds
const STOP_LOSS_LIMIT = 5;  // Maximum stop-loss trades before triggering the circuit breaker
const MAX_ACTIVE_TRADES = 10; // Maximum allowed active buy trades
let buyCoolDownActive = false; // Cooldown state flag

export const coinData = COINS.map(coin => ({
    ...coin,
    priceHistory: [],  // Initialize price history
    rsiHistory: [coin.initialRSI?.toFixed(4) || '50.0000'],  // Default RSI if missing
    currentRSI: new Big(coin.initialRSI || 50),  // Default RSI to 50 as Big.js
    lastPrice: null,  // Will be updated with the latest price
    ma50: new Big(coin.initialMA50 || 0),  // Default MA50 to 0
    ma200: new Big(coin.initialMA200 || 0),  // Default MA200 to 0
    trades: [],  // Store trades related to the coin
    ma200CrossLog: [],  // Track MA200 crossings
    ma200CrossedAbove: false,  // Boolean flag for MA200 cross above
    ma200CrossedBelow: false,  // Boolean flag for MA200 cross below
    lastTrend: 'Neutral',  // Store the last known trend
    lastBuySignalTime: null,  // Track the last buy signal timestamp
    inputMint: coin.inputMint || 'UNKNOWN_INPUT',  // Default inputMint
    outputMint: coin.outputMint || 'UNKNOWN_OUTPUT',  // Default outputMint
    decimals: coin.decimals || 9  // Ensure decimals is assigned, default to 9
}));

// Delay function to pause execution for a given number of milliseconds
function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

dotenv.config();

// after dotenv.config()
const pool = new Pool({
  connectionString: process.env.DATABASE_URL, // or use PGHOST/PGUSER/etc
  ssl: process.env.PGSSL === 'true' ? { rejectUnauthorized: false } : false,
});

type DbCoin = {
  name: string;
  poolId: string;
  inputMint: string;
  outputMint: string;
  decimals: number;
  initialRSI: number;
  initialMA50: number;
  initialMA200: number;
  rsiPeriod: number;
};

async function loadCoinsFromDatabase(): Promise<DbCoin[]> {
  const { rows } = await pool.query(`
    SELECT
      name,
      pool_id        AS "poolId",
      input_mint     AS "inputMint",
      output_mint    AS "outputMint",
      COALESCE(decimals, 9)            AS "decimals",
      COALESCE(initial_rsi, 50)        AS "initialRSI",
      COALESCE(initial_ma50, 0)        AS "initialMA50",
      COALESCE(initial_ma200, 0)       AS "initialMA200",
      COALESCE(rsi_period, 14)         AS "rsiPeriod"
    FROM coins
    WHERE enabled = TRUE
    ORDER BY priority NULLS LAST, name;
  `);
  return rows;
}

// runtime container; no more export const coinData = COINS.map(...)
let coinData: any[] = [];

async function primeCoinData() {
  const coins = await loadCoinsFromDatabase();
  coinData = coins.map(coin => ({
    ...coin,
    priceHistory: [],
    rsiHistory: [new Big(coin.initialRSI).toFixed(4)],
    currentRSI: new Big(coin.initialRSI),
    lastPrice: null as Big | null,
    ma50: new Big(coin.initialMA50),
    ma200: new Big(coin.initialMA200),
    trades: [],
    ma200CrossLog: [],
    ma200CrossedAbove: false,
    ma200CrossedBelow: false,
    lastTrend: 'Neutral',
    lastBuySignalTime: null,
  }));
}

// runtime container; no more export const coinData = COINS.map(...)
let coinData: any[] = [];

async function primeCoinData() {
  const coins = await loadCoinsFromDatabase();
  coinData = coins.map(coin => ({
    ...coin,
    priceHistory: [],
    rsiHistory: [new Big(coin.initialRSI).toFixed(4)],
    currentRSI: new Big(coin.initialRSI),
    lastPrice: null as Big | null,
    ma50: new Big(coin.initialMA50),
    ma200: new Big(coin.initialMA200),
    trades: [],
    ma200CrossLog: [],
    ma200CrossedAbove: false,
    ma200CrossedBelow: false,
    lastTrend: 'Neutral',
    lastBuySignalTime: null,
  }));
}

// Initialize a connection to the Solana cluster
const connection = new Connection('https://api.mainnet-beta.solana.com');
const agent = new https.Agent({
    minVersion: 'TLSv1.2',
    maxVersion: 'TLSv1.3',
});

let wallet;

try {
    // Ensure PRIVATE_KEY is loaded from the environment variable
    if (!process.env.PRIVATE_KEY) {
        throw new Error("PRIVATE_KEY environment variable is not set.");
    }

    // Decode the private key from base58
    const secretKey = bs58.decode(process.env.PRIVATE_KEY);
    const keypair = Keypair.fromSecretKey(secretKey);
    wallet = new Wallet(keypair);

    console.log("Wallet successfully initialized:");
} catch (error) {
    console.error("Error initializing wallet:", error.message);
    process.exit(1);  // Exit the process if the wallet cannot be initialized
}

// Example of wallet usage in your logic:
async function initializeTrading() {
    try {
        const walletPublicKey = wallet.publicKey;
        const solThreshold = 0.005;  // Minimum SOL balance before halting buy signals

        console.log(`Starting wallet monitoring with a SOL threshold of ${solThreshold}.`);

        // Start checking the wallet balance periodically
        await checkWallet(walletPublicKey, solThreshold);

        // Continue your trading logic...
        // Example: processBuySignal or other trading operations
    } catch (error) {
        console.error("Error initializing trading:", error);
    }
}

// Function to check wallet balance, stop-loss, and active buy trades
async function checkWallet(walletPublicKey, solThreshold) {
    try {
        // Fetch the wallet balance in lamports (1 SOL = 1e9 lamports)
        const balanceLamports = await connection.getBalance(walletPublicKey);
        const balanceSOL = lamportsToSol(balanceLamports);

        console.log(`Wallet balance: ${balanceSOL} SOL`);

        // Check if balance is below the specified threshold
        if (balanceSOL < solThreshold) {
            console.log(`Wallet balance below threshold (${solThreshold} SOL). Halting buy signals.`);

            // Disable buy signals globally
            global.buySignalsEnabled = false;

            // Recheck wallet balance every 30 minutes (1800000 milliseconds)
            setTimeout(async () => {
                console.log(`Rechecking wallet balance in 30 minutes...`);
                await checkWallet(walletPublicKey, solThreshold);
            }, 70000);  // 2 minutes

            return false;
        }

        console.log(`Wallet balance is sufficient: ${balanceSOL} SOL. Checking stop-loss conditions...`);
        
        // Call stop-loss check and cooldown enforcement
        await checkStopLossAndEnforceCooldown();  // Integrate the stop-loss check
        
        // If cooldown is active, halt buy signals
        if (buyCoolDownActive) {
            console.log("Stop-loss cooldown is active. Halting buy signals.");
            global.buySignalsEnabled = false;
            
            // Recheck stop-loss and wallet balance after the cooldown period (30 minutes)
            setTimeout(async () => {
                console.log(`Rechecking stop-loss and wallet balance after cooldown...`);
                await checkWallet(walletPublicKey, solThreshold);
            }, 70000);  // 2 minutes

            return false;
        }

        // Check the number of active buy trades
        const activeBuyTrades = await countActiveBuyTradesFromDatabase();
        console.log(`Number of active buy trades: ${activeBuyTrades}`);

        if (activeBuyTrades >= 10) {
            console.log(`Maximum active buy trades reached (10). Halting new buy signals.`);
            global.buySignalsEnabled = false;

            // Recheck wallet balance, stop-loss, and active buy trades every 30 minutes
            setTimeout(async () => {
                console.log(`Rechecking active buy trades and wallet balance in 30 minutes...`);
                await checkWallet(walletPublicKey, solThreshold);
            }, 70000);  // 2 minutes

            return false;
        }

        console.log(`Wallet balance, stop-loss conditions, and active buy trades are fine. Buy signals are enabled.`);
        global.buySignalsEnabled = true;

        return true;
    } catch (error) {
        console.error("Error checking wallet balance, stop-loss, or active buy trades:", error);
        throw error;
    }
}

async function saveStopLossEvent(coin: string, price: number, condition: string) {
  await pool.query(
    `INSERT INTO stop_loss_log (coin, ts, price, condition) VALUES ($1, NOW(), $2, $3)`,
    [coin, price, condition]
  );
}

async function countActiveBuyTradesFromDatabase() {
  const { rows } = await pool.query(
    `SELECT COUNT(*)::int AS "activeBuyCount"
     FROM trade_logs
     WHERE action = 'BUY' AND status = 'active'`
  );
  return rows[0]?.activeBuyCount ?? 0;
}

async function checkStopLossAndEnforceCooldown() {
  const { rows } = await pool.query(
    `SELECT COUNT(*)::int AS "stopLossCount"
     FROM stop_loss_log
     WHERE ts > NOW() - INTERVAL '1 day'`
  );
  const stopLossCount = rows[0]?.stopLossCount ?? 0;

  const activeBuyTrades = await countActiveBuyTradesFromDatabase();

  if (stopLossCount >= STOP_LOSS_LIMIT || activeBuyTrades >= MAX_ACTIVE_TRADES) {
    await saveStopLossEvent('ALL', 0, `Cooldown: ${stopLossCount} stop-loss / ${activeBuyTrades} active`);
    buyCoolDownActive = true;
    global.buySignalsEnabled = false;
    setTimeout(() => { buyCoolDownActive = false; global.buySignalsEnabled = true; }, COOLDOWN_PERIOD);
  }
}


// Initialize trading process
initializeTrading();

// Utility function to convert lamports to SOL
function lamportsToSol(lamports) {
    return lamports / 1e9;  // 1 SOL = 1e9 lamports
}

export const getPriceData = () => {
    return coinData.map(coin => ({
        name: coin.name,
        lastPrice: coin.lastPrice ? coin.lastPrice.toString() : null
    }));
};

async function exportCurrentPrices() {
    const prices = getPriceData(); // Fetch the price data

    const currentTime = Date.now();

    // Add a delay before the function repeats
    await delay(7000);
}

const BUY_COOLDOWN_PERIOD = 5 * 60 * 1000; // 5 minutes in milliseconds

async function generateBuySignal(coin) {
    const currentTime = new Date().getTime();
    
    // Check if buy signals are globally enabled
    if (!global.buySignalsEnabled) {
        console.log(`Buy signals are globally disabled. No buy signal generated for ${coin.name}.`);
        return;  // Prevent any buy signals if globally disabled
    }

    // Check if the global buy cooldown is active
    if (buyCoolDownActive) {
        console.log(`Global buy cooldown is active. No buy signal generated for ${coin.name}.`);
        return;  // Prevent any buy signals while the cooldown is active
    }

    // Check if we're still in the cooldown period for this specific coin
    if (coin.lastBuySignalTime && (currentTime - coin.lastBuySignalTime) < BUY_COOLDOWN_PERIOD) {
        console.log(`Skipping buy signal check for ${coin.name}. Cooldown: ${((BUY_COOLDOWN_PERIOD - (currentTime - coin.lastBuySignalTime)) / 1000).toFixed(2)} seconds remaining`);
        return;
    }

    // Add a small randomized delay to stagger the buy signals
    const randomDelay = Math.floor(Math.random() * 5000) + 1000;  // Random delay between 1 to 5 seconds
    await delay(randomDelay);

    // Check the number of active trades
    const activeTrades = await countActiveTrades(coin.name);
    if (activeTrades >= 10) {
        console.log(`Max trade limit reached for ${coin.name}. No new buy signal generated.`);
        return;
    }

    // Separate buy signal triggers based on independent conditions

    // 1. RSI-Based Buy Signal (Oversold Condition)
    const rsiCondition = coin.currentRSI.lt(30);
    if (rsiCondition) {
        console.log(`RSI-based BUY signal generated for ${coin.name}: RSI < 30`);
        try {
            await sendBuySignalToMaster(coin);
            coin.lastBuySignalTime = currentTime;
            console.log(`Buy signal for ${coin.name} successfully sent and processed based on RSI.`);
        } catch (error) {
            console.error(`Error sending RSI-based buy signal for ${coin.name}:`, error);
        }
        return; // Prevent multiple signals in one cycle
    }

    // 2. Moving Averages and Price Action Buy Signal
    const maCondition = coin.ma50.gt(coin.ma200);  // MA50 > MA200 indicates bullish strength
    const trendCondition = coin.lastTrend === 'Bullish';
    const priceAboveMA200 = coin.lastPrice.gte(coin.ma200);
    const priceRelativeToMA200 = coin.lastPrice.div(coin.ma200);  // Calculate price relative to MA200

    if (maCondition && trendCondition && priceAboveMA200) {
        if (priceRelativeToMA200.gt(1.45)) {  // Check if the price is more than 45% above the MA200
            console.log(`Skipping buy signal for ${coin.name} because the price is more than 50% above the MA200.`);
            return;
        }

        console.log(`MA-based BUY signal generated for ${coin.name} based on MA50 > MA200 and price above MA200`);
        try {
            await sendBuySignalToMaster(coin);
            coin.lastBuySignalTime = currentTime;
            console.log(`Buy signal for ${coin.name} successfully sent and processed based on MA and trend.`);
        } catch (error) {
            console.error(`Error sending MA-based buy signal for ${coin.name}:`, error);
        }
        return;
    }

    // 3. MA200 Crossing Condition
    const ma200CrossCondition = await trackMA200Crossing(coin);
    if (ma200CrossCondition) {
        console.log(`BUY signal generated for ${coin.name} based on MA200 crossing`);
        try {
            await sendBuySignalToMaster(coin);
            coin.lastBuySignalTime = currentTime;
            coin.ma200CrossedBelow = false; // Reset this flag after generating a buy signal
            console.log(`Buy signal for ${coin.name} successfully sent and processed based on MA200 crossing.`);
        } catch (error) {
            console.error(`Error sending MA200 crossing-based buy signal for ${coin.name}:`, error);
        }
    } else {
        console.log(`No buy signal for ${coin.name}. Conditions not met.`);
    }
}

async function loadTradesFromDatabase() {
  const { rows } = await pool.query(
    `SELECT coin, action, status, amount::float8 AS amount, price::float8 AS price, ts
     FROM trade_logs`
  );
  return rows.map(r => ({ coin: r.coin, action: r.action, status: r.status, amount: r.amount, price: r.price, timestamp: r.ts }));
}

async function countActiveTrades(coinName: string) {
  const { rows } = await pool.query(
    `SELECT COUNT(*)::int AS n FROM trade_logs WHERE coin = $1 AND is_closed = FALSE`,
    [coinName]
  );
  return rows[0]?.n ?? 0;
}

function convertToLamports(amountInSol) {
    const lamports = Math.floor(amountInSol * 1e9);  // 1 SOL = 1e9 lamports
    console.log(`Converting ${amountInSol} SOL to ${lamports} lamports`);
    return lamports;
}

export async function sendBuySignalToMaster(coin: any, trend = 'neutral') {
  const amountInLamports = convertToLamports(0.00001);
  const currentPrice = coin.lastPrice ? coin.lastPrice.toString() : null;
  await pool.query(
    `INSERT INTO buy_signals (coin, price, rsi, input_mint, output_mint, amount, ts, trend)
     VALUES ($1, $2, $3, $4, $5, $6, NOW(), $7)`,
    [
      coin.name,
      currentPrice,
      new Big(coin.currentRSI).toFixed(2),
      coin.inputMint,
      coin.outputMint,
      amountInLamports.toString(),
      trend || coin.lastTrend || 'neutral',
    ]
  );
}

async function saveMA200Crossing(coin: any, direction: 'above'|'below') {
  const price = coin.lastPrice?.toString();
  const ma50 = coin.ma50?.toString();
  const ma200 = coin.ma200?.toString();
  const trend = coin.lastTrend || 'Neutral';

  await pool.query(
    `INSERT INTO ma200_cross_log (coin, direction, ts, price, ma50, ma200, trend)
     VALUES ($1, $2, NOW(), $3, $4, $5, $6)
     ON CONFLICT (coin, direction)
     DO UPDATE SET ts = EXCLUDED.ts, price = EXCLUDED.price, ma50 = EXCLUDED.ma50, ma200 = EXCLUDED.ma200, trend = EXCLUDED.trend`,
    [coin.name, direction, price, ma50, ma200, trend]
  );
}


async function trackMA200Crossing(coin) {
    const belowThreshold = coin.ma200.times(0.99);
    const aboveThreshold = coin.ma200.times(1.01);

    if (coin.lastPrice.lte(belowThreshold)) {
        if (!coin.ma200CrossedBelow) {
            coin.ma200CrossedBelow = true;
            coin.ma200CrossedAbove = false;

            await saveMA200Crossing(coin, 'below');
            console.log(`MA200 crossed below for ${coin.name} at ${coin.lastPrice.toFixed(5)}`);
        }
    } else if (coin.lastPrice.gte(aboveThreshold)) {
        if (coin.ma200CrossedBelow && !coin.ma200CrossedAbove) {
            coin.ma200CrossedAbove = true;
            coin.ma200CrossedBelow = false;

            await saveMA200Crossing(coin, 'above');
            console.log(`MA200 crossed above for ${coin.name} at ${coin.lastPrice.toFixed(5)}`);
        }
    }
}


// Updated saveCurrentPrice function to insert into the MySQL `current_prices` table
async function saveCurrentPrice(coin: any) {
  await pool.query(
    `INSERT INTO current_prices (coin, last_price, ts)
     VALUES ($1, $2, NOW())
     ON CONFLICT (coin) DO UPDATE SET last_price = EXCLUDED.last_price, ts = NOW()`,
    [coin.name, coin.lastPrice?.toString() ?? null]
  );
}

async function fetchPrice(coin) {
    try {
        console.log(`Fetching price for ${coin.name}`);
        
        // Fetch price data from the API
        const response = await axios.get(`https://api.geckoterminal.com/api/v2/networks/solana/pools/${coin.poolId}`);
        const data = response?.data?.data;

        // Validate the presence of the price field
        if (data && data.attributes && data.attributes.base_token_price_usd) {
            const price = parseFloat(data.attributes.base_token_price_usd);

            if (!isNaN(price) && price > 0) {
                const bigPrice = new Big(price);
                coin.lastPrice = bigPrice;

                // Ensure price history exists and maintain limited history
                coin.priceHistory = coin.priceHistory || [];
                if (coin.priceHistory.length >= MAX_PRICE_HISTORY) {
                    coin.priceHistory.shift(); // Remove the oldest entry
                }
                coin.priceHistory.push(bigPrice);

                // Update moving averages
                updateMovingAverages(coin, bigPrice);

                // Calculate and update RSI if enough history is available
                if (coin.priceHistory.length >= coin.rsiPeriod) {
                    coin.currentRSI = calculateRSI(coin.priceHistory.slice(-coin.rsiPeriod), coin.rsiPeriod, coin.currentRSI);
                    coin.rsiHistory = coin.rsiHistory || [];
                    coin.rsiHistory.push(coin.currentRSI.toFixed(4));

                    if (coin.rsiHistory.length > 5) {
                        coin.rsiHistory = coin.rsiHistory.slice(-5);
                    }
                    console.log(`RSI updated for ${coin.name}: ${coin.currentRSI.toFixed(4)}`);
                }

                // Consolidated log output
                console.log(`
========== ${coin.name} Update ==========
Price: $${bigPrice.toFixed(6)}
RSI: ${coin.currentRSI ? coin.currentRSI.toFixed(4) : 'N/A'}
MA50: ${coin.ma50 ? coin.ma50.toFixed(4) : 'N/A'}
MA200: ${coin.ma200 ? coin.ma200.toFixed(4) : 'N/A'}
Trend: ${coin.lastTrend || 'N/A'}
RSI History: [${coin.rsiHistory ? coin.rsiHistory.join(', ') : 'N/A'}]
=========================================
                `);

                // Generate buy signal if applicable
                const buySignal = await generateBuySignal(coin);
                if (buySignal) {
                    console.log(`Buy signal triggered for ${coin.name}`);
                }

                // Track MA200 crossing events and save logs
                await trackMA200Crossing(coin);

                // Save the latest price to the database
                await saveCurrentPrice(coin);

            } else {
                console.error(`Invalid price data for ${coin.name}: ${price}`);
            }
        } else {
            console.error(`No valid data returned for ${coin.name} from API.`);
        }
    } catch (error) {
        console.error(`Error fetching price for ${coin.name}:`, error.message);
    }
}

function calculateRSI(prices, period, startingRSI) {
    let gain = new Big(0);
    let loss = new Big(0);

    for (let i = 1; i < prices.length; i++) {
        const difference = prices[i].minus(prices[i - 1]);
        if (difference.gt(0)) {
            gain = gain.plus(difference);
        } else if (difference.lt(0)) {
            loss = loss.plus(difference.abs());
        }
    }

    let avgGain = gain.div(period);
    let avgLoss = loss.div(period);

    if (avgLoss.eq(0)) {
        return avgGain.eq(0) ? startingRSI : startingRSI.plus(new Big(5));
    }

    const rs = avgGain.div(avgLoss);
    const calculatedRSI = new Big(100).minus(new Big(100).div(rs.plus(1)));

    const priceChangeFactor = prices.length < 1500 ? prices.length / 1500 : 0.45;
    const adjustedWeight = priceChangeFactor < 0.085 ? 0.085 : priceChangeFactor;
    const accumulatedDataWeight = new Big(adjustedWeight);

    return calculatedRSI.times(accumulatedDataWeight).plus(startingRSI.times(new Big(1).minus(accumulatedDataWeight)));
}

function updateMovingAverages(coin, newPrice) {
    const ma50Multiplier = new Big(2).div(new Big(50).plus(1));
    const ma200Multiplier = new Big(2).div(new Big(200).plus(1));

    // Ensure MA50 and MA200 are initialized
    if (!coin.ma50 || !coin.ma200) {
        console.warn(`MA values not initialized for ${coin.name}. Using initial values.`);
        coin.ma50 = new Big(coin.initialMA50 || 0);
        coin.ma200 = new Big(coin.initialMA200 || 0);
    }

    // Update MA50 and MA200 using exponential moving average formula
    coin.ma50 = newPrice.minus(coin.ma50).times(ma50Multiplier).plus(coin.ma50);
    coin.ma200 = newPrice.minus(coin.ma200).times(ma200Multiplier).plus(coin.ma200);

    console.log(
        `Updated MA for ${coin.name}: MA50=${coin.ma50.toFixed(5)}, MA200=${coin.ma200.toFixed(5)}, Price=${newPrice.toFixed(5)}`
    );

    // Determine the trend based on MA50 and MA200
    let currentTrend = coin.lastTrend || 'Neutral';
    if (coin.ma50.gt(coin.ma200)) {
        currentTrend = 'Bullish';
    } else if (coin.ma50.lt(coin.ma200)) {
        currentTrend = 'Bearish';
    } else if (coin.ma50.minus(coin.ma200).abs().div(coin.ma200).lt(0.05)) {
        currentTrend = 'Neutral';
    }

    // Log and update trend if it changes
    if (coin.lastTrend !== currentTrend) {
        console.log(`Trend changed for ${coin.name}: ${coin.lastTrend} → ${currentTrend}`);
        coin.lastTrend = currentTrend;
    }
}


async function saveTradesToDatabase() {
    try {
        for (const coin of coinData) {
            for (const trade of coin.trades) {
                await db.execute(`
                    INSERT INTO trade_logs (coin, action, status, amount, price, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON DUPLICATE KEY UPDATE
                    action = VALUES(action),
                    status = VALUES(status),
                    amount = VALUES(amount),
                    price = VALUES(price),
                    timestamp = VALUES(timestamp)
                `, [coin.name, trade.action, trade.status, trade.amount, trade.price, trade.timestamp]);
            }
        }
        console.log('Trades saved successfully.');
    } catch (error) {
        console.error('Error saving trades to database:', error.message);
    }
}

async function saveMA200CrossLogsToDatabase() {
    try {
        for (const coin of coinData) {
            for (const log of coin.ma200CrossLog) {
                await db.execute(`
                    INSERT INTO ma200_cross_log (coin, timestamp, price, direction, ma50, ma200, trend)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON DUPLICATE KEY UPDATE
                    price = VALUES(price),
                    direction = VALUES(direction),
                    ma50 = VALUES(ma50),
                    ma200 = VALUES(ma200),
                    trend = VALUES(trend)
                `, [coin.name, log.timestamp, log.price.toString(), log.direction, log.ma50.toFixed(5), log.ma200.toFixed(5), log.trend]);
            }
        }
        console.log('MA200 crossing logs saved successfully.');
    } catch (error) {
        console.error('Error saving MA200 crossing logs:', error.message);
    }
}


async function loadMA200CrossLogsFromDatabase() {
    try {
        const [rows] = await db.execute(`
            SELECT coin, timestamp, price, direction
            FROM ma200_cross_log
        `);
        for (const coin of coinData) {
            coin.ma200CrossLog = rows
                .filter(row => row.coin === coin.name)
                .map(row => ({
                    timestamp: row.timestamp,
                    price: parseFloat(row.price),
                    direction: row.direction
                }));
        }
        console.log('MA200 crossing logs loaded successfully.');
    } catch (error) {
        console.error('Error loading MA200 cross logs:', error.message);
    }
}

async function main() {
  try {
    console.log("Init DB + load coins...");
    await primeCoinData();

    console.log("Loading trade data...");
    const trades = await loadTradesFromDatabase();
    console.log(`Loaded ${trades.length} trades.`);

    console.log("Loading MA200 crossing logs...");
    await loadMA200CrossLogsFromDatabase();

    console.log("Start price loop...");
    await startFetchingPrices();

    console.log("Bot started.");
  } catch (err:any) {
    console.error("Critical init error:", err.message);
    process.exit(1);
  }
}


// Function to start periodic price fetching and logging
async function startFetchingPrices() {
    try {
        await loadMA200CrossLogsFromDatabase();  // Ensure MA200 logs are preloaded

        for (const coin of coinData) {
          setInterval(() => fetchPrice(coin), FETCH_INTERVAL);
    }

        setInterval(saveMA200CrossLogsToDatabase, SAVE_INTERVAL);  // Periodically save MA200 logs
        setInterval(exportCurrentPrices, SAVE_INTERVAL);           // Periodically export prices

        console.log("Periodic price fetching and logging tasks initiated.");
    } catch (error) {
        console.error("Error during periodic task initialization:", error.message);
        throw error; // Rethrow the error to let main handle it if necessary
    }
}

// Handle graceful shutdown on interrupt signals
process.on('SIGINT', async () => {
    console.log('Caught interrupt signal');
    await saveTradesToDatabase(); // Save remaining trades to database before exit
    process.exit();
});

// Start the script by calling the main function
main();

