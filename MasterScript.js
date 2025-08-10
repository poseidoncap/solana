import path from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import Big from 'big.js';
import winston from 'winston';
import mysql from 'mysql2/promise';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const db = mysql.createPool({
    host: 'localhost',
    user: 'root',
    password: 'LFGB!!7771M!',
    database: 'trading_bot_data',
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0,
});

// Configuration
const config = {
    STOP_LOSS_THRESHOLD: 0.975, // 2.5% below purchase price
    ROI_THRESHOLD: 1.45,  // 1.5% ROI
    CHECK_INTERVAL: 6000,  // 6 seconds
    SOLANA_SCRIPT_PATH: path.join(__dirname, 'solana-logic.js'),
};

// Setup logger
const logger = winston.createLogger({
    level: 'info',
    format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.json()
    ),
    transports: [
        new winston.transports.File({ filename: 'error.log', level: 'error' }),
        new winston.transports.File({ filename: 'combined.log' }),
        new winston.transports.Console()
    ]
});

// Function to fetch coins from the MySQL database
async function getCoins() {
    const [coins] = await db.execute(`SELECT * FROM coins`);
    return coins;
}

// Initialize COINS data from MySQL database
async function initializeCoins() {
    try {
        const coinsData = await getCoins();
        if (Array.isArray(coinsData) && coinsData.length > 0) {
            console.log('COINS data successfully loaded from the database:', coinsData);
            coinsData.forEach(coin => {
                console.log(`Coin: ${coin.name}, Input Mint: ${coin.input_mint}, Output Mint: ${coin.output_mint}`);
            });
            return coinsData;
        } else {
            console.warn('No COINS data found in the database.');
            return [];
        }
    } catch (error) {
        console.error('Error initializing COINS from database:', error);
        return [];
    }
}

// Load COINS data into a variable for use in the script
const COINS = await initializeCoins();

if (COINS && COINS.length > 0) {
    console.log('COINS data loaded and ready for use.');
    // You can now access the `COINS` array throughout your script
    COINS.forEach(coin => {
        console.log(`Coin: ${coin.name}, Input Mint: ${coin.input_mint}, Output Mint: ${coin.output_mint}`);
    });
}


// Function to convert lamports to SOL
function lamportsToSol(lamports) {
    return new Big(lamports).div(1e9).toFixed(8);  // Convert lamports to SOL and round to 8 decimal places
}


async function logStopLoss(trade) {
    try {
        // Insert the stop-loss event directly into the stop_loss_log table
        await db.execute(`
            INSERT INTO stop_loss_log (coin, trade_id, price, timestamp, reason)
            VALUES (?, ?, ?, NOW(), ?)
        `, [trade.coin, trade.tradeId, trade.price, 'Stop Loss']);

        console.log(`Stop-loss logged for ${trade.coin} (Trade ID: ${trade.tradeId})`);
    } catch (error) {
        console.error(`Error logging stop-loss for ${trade.coin}: ${error.message}`);
    }
}


const currentPrices = await getCurrentPrices();

async function getCurrentPrices() {
    const [currentPrices] = await db.execute(`SELECT * FROM current_prices`);
    return currentPrices;
}

const trades = await getTrades();

async function getTrades() {
    const [trades] = await db.execute(`SELECT * FROM trade_logs WHERE status = 'active'`);
    return trades;
}

async function monitorROI() {
    try {
        // Fetch unsold trades
        const trades = await getTrades();
        if (!trades || trades.length === 0) {
            console.log('No unsold trades found. Skipping ROI monitoring.');
            return;
        }

        // Fetch current prices for relevant coins
        const coinsToCheck = trades.map(trade => trade.coin);
        const currentPrices = await getCurrentPricesForCoins(coinsToCheck);

        if (!currentPrices || currentPrices.length === 0) {
            console.error('No current prices available for active trades.');
            return;
        }

        // Iterate over trades
        for (const trade of trades) {
            // Validate trade fields
            if (!trade.coin || !trade.price || !trade.tradeId) {
                console.error(`Skipping trade due to missing data:`, trade);
                continue;
            }

            // Skip already sold or pending trades
            if (trade.sold || trade.pending) {
                continue;
            }

            // Find current price data for the trade's coin
            const currentPriceData = currentPrices.find(price => price.coin === trade.coin);
            if (!currentPriceData || !currentPriceData.last_price) {
                console.error(`No price data available for ${trade.coin}.`);
                continue;
            }

            // Ensure we are working with valid numbers
            const currentPrice = new Big(currentPriceData.last_price);
            const tradePrice = new Big(trade.price);

            if (tradePrice.eq(0) || currentPrice.eq(0)) {
                console.error(`Invalid price data for ${trade.coin}: Trade Price = ${tradePrice}, Current Price = ${currentPrice}. Skipping ROI check.`);
                continue;
            }

            // Calculate ROI
            const profit = currentPrice.minus(tradePrice).div(tradePrice).times(100);

            // Check ROI thresholds
            if (profit.gte(config.ROI_THRESHOLD)) {
                console.log(`ROI met for ${trade.coin} (Trade ID: ${trade.tradeId}). Profit: ${profit.toFixed(6)}%. Generating sell signal.`);
                await generateSellSignal(trade.coin, currentPrice.toFixed(8), 'ROI Target', trade);
                trade.pending = true; // Mark trade as pending
            } else if (currentPrice.lte(tradePrice.times(config.STOP_LOSS_THRESHOLD))) {
                console.log(`Stop-loss triggered for ${trade.coin} (Trade ID: ${trade.tradeId}). Loss: ${profit.toFixed(6)}%. Generating sell signal.`);
                await generateSellSignal(trade.coin, currentPrice.toFixed(8), 'Stop Loss', trade);
                trade.pending = true; // Mark trade as pending
                await logStopLoss(trade); // Log the stop-loss event
            }
        }
    } catch (error) {
        console.error(`Error monitoring ROI:`, error);
    }
}

async function generateSellSignal(coin, currentPrice, reason, trade) {
    if (!trade.tradeId) {
        throw new Error(`Cannot generate sell signal: missing tradeId from the buy trade.`);
    }

    // Insert the sell signal directly into the sell_signals table in MySQL
    await db.execute(`
        INSERT INTO sell_signals (coin, price, amount, reason, trade_id, timestamp)
        VALUES (?, ?, ?, ?, ?, NOW())
    `, [coin.name, currentPrice.toString(), trade.amountInSol, reason, trade.tradeId]);

    console.log(`Sell signal generated for ${coin.name}: ${reason}`);

    // Update the trade status to pending in the database to prevent further processing
    await db.execute(`
        UPDATE trade_logs SET status = 'pending' WHERE trade_id = ?
    `, [trade.tradeId]);

    console.log(`Trade status updated to 'pending' for Trade ID: ${trade.tradeId}`);
}


async function mainLoop(COINS) {
    while (true) {
        // Monitor ROI and manage positions based on COINS data
        await monitorROI(COINS);  // Pass COINS to monitorROI

        // Sleep for the configured interval before repeating the loop
        await new Promise(resolve => setTimeout(resolve, config.CHECK_INTERVAL));
    }
}

// Main entry point
async function main() {
    try {
        logger.info("Master script starting...");
        
        // Initialize COINS
        const COINS = await initializeCoins();
        if (!Array.isArray(COINS) || COINS.length === 0) {
            throw new Error("Failed to initialize COINS or COINS is empty.");
        }
        
        // Start the main loop for monitoring ROI and managing positions
        logger.info("Starting main loop for ROI monitoring and position management.");
        await mainLoop(COINS);
    } catch (error) {
        logger.error('Error in main function:', error);
        throw error; // Re-throw the error to be caught by the outer catch block
    }
}

// Global error handling to catch any uncaught exceptions or promise rejections
process.on('uncaughtException', (error) => {
    logger.error('Uncaught Exception:', error);
    process.exit(1); // Exit the process after an uncaught exception
});

process.on('unhandledRejection', (reason, promise) => {
    logger.error('Unhandled Rejection at:', promise, 'reason:', reason);
    process.exit(1); // Exit the process after an unhandled rejection
});

// Start the script by running the main function
main().catch(error => {
    logger.error('Fatal error in main function:', error);
    process.exit(1);  // Exit process on fatal error
});