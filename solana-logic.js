import { Connection, Keypair, VersionedTransaction } from '@solana/web3.js';
import axios from 'axios';
import { Wallet } from '@project-serum/anchor';
import bs58 from 'bs58';
import dotenv from 'dotenv';
import https from 'https';
import Big from 'big.js';
import pLimit from 'p-limit';
import mysql from 'mysql2/promise';

// Load environment variables
dotenv.config();

const db = mysql.createPool({
    host: 'localhost',
    user: 'root',
    password: 'LFGB!!7771M!',
    database: 'trading_bot_data',
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0,
});

console.log("Script initializing...");

if (!process.env.PRIVATE_KEY) {
    console.error("PRIVATE_KEY environment variable is not set.");
    process.exit(1);
}

let wallet;
try {
    const secretKey = bs58.decode(process.env.PRIVATE_KEY);
    wallet = new Wallet(Keypair.fromSecretKey(secretKey));
} catch (error) {
    console.error("Error decoding PRIVATE_KEY:", error.message);
    process.exit(1);
}

const connection = new Connection('https://api.mainnet-beta.solana.com');
const agent = new https.Agent({
    minVersion: 'TLSv1.2',
    maxVersion: 'TLSv1.3',
});

// Create a custom Axios instance with a rate-limit interceptor
const axiosInstance = axios.create({
    baseURL: 'https://quote-api.jup.ag/v6/',
    timeout: 10000,
    // Disable retries explicitly (only relevant if using retry logic from axios-retry or similar)
    maxRedirects: 0,  // Prevent redirects
    validateStatus: function (status) {
        return status < 400;  // Reject only if status >= 400 (except 429)
    }
});

// Axios interceptor to handle 429 errors
axiosInstance.interceptors.response.use(
    response => {
        return response;  // Return the response if it's successful
    },
    error => {
        if (error.response && error.response.status === 429) {
            console.error(`429 Error detected. No retries will be made: ${error.message}`);
            return Promise.reject(error);  // Reject the promise without retrying
        }
        return Promise.reject(error);  // Reject other errors as well
    }
);

export { axiosInstance };

// Trade queue management
let isProcessingQueue = false;
let globalQueue = [];
let isProcessingBuySignal = false;  // Prevents multiple buy signals from being processed at the same time
let isProcessingSellSignal = false; // Prevents multiple sell signals from being processed at the same time
let activeBuySignals = {};  // In-memory map to track active buy signals
let activeSellSignals = {}; // In-memory map to track active sell signals

const pendingSignals = [];

// Updated checkForSignals function
async function checkForSignals() {
    // Process pending signals first
    while (pendingSignals.length > 0) {
        const signal = pendingSignals.shift();
        if (signal.type === 'BUY') {
            await processBuySignal(signal.data);
        } else if (signal.type === 'SELL') {
            await processSellSignal(signal.data);
        }
    }

    // Fetch unprocessed signals from MySQL tables
    const [buySignals] = await db.execute(`SELECT * FROM buy_signals WHERE processed = 0`);
    const [sellSignals] = await db.execute(`SELECT * FROM sell_signals WHERE processed = 0`);

    // Process new buy signals
    for (const buySignal of buySignals) {
        if (!pendingSignals.find(signal => signal.data.tradeId === buySignal.tradeId)) {
            pendingSignals.push({ type: 'BUY', data: buySignal });
            await processBuySignal(buySignal); // Process the signal immediately
        } else {
            console.log(`Duplicate buy signal ignored for Trade ID: ${buySignal.tradeId}`);
        }
    }

    // Process new sell signals
    for (const sellSignal of sellSignals) {
        if (!pendingSignals.find(signal => signal.data.tradeId === sellSignal.tradeId)) {
            pendingSignals.push({ type: 'SELL', data: sellSignal });
            await processSellSignal(sellSignal); // Process the signal immediately
        } else {
            console.log(`Duplicate sell signal ignored for Trade ID: ${sellSignal.tradeId}`);
        }
    }
}


const MAX_CONCURRENT_OPERATIONS = 10;
const limit = pLimit(MAX_CONCURRENT_OPERATIONS);

// Ensure buyQueue, sellQueue, and retry queues are defined
const buyQueue = [];
const sellQueue = [];
const retryBuyQueue = [];
const retrySellQueue = [];

function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function processQueues() {
    setInterval(async () => {
        try {
            const isBuyQueueProcessed = buyQueue.length > 0 || retryBuyQueue.length > 0;
            const isSellQueueProcessed = sellQueue.length > 0 || retrySellQueue.length > 0;

            if (isBuyQueueProcessed) {
                await processBuyQueue();
            }

            if (isSellQueueProcessed) {
                await processSellQueue();
            }

            // Delay if nothing is being processed
            if (!isBuyQueueProcessed && !isSellQueueProcessed) {
                await delay(2000);
            }
        } catch (error) {
            console.error("Error in queue processing:", error);
        }
    }, 2000);
}

const RATE_LIMIT_DELAY = 6000; // 6 seconds between each request

let lastApiCallTimestamp = 0;

async function throttleApiCall() {
    const now = Date.now();
    const timeSinceLastCall = now - lastApiCallTimestamp;

    if (timeSinceLastCall < RATE_LIMIT_DELAY) {
        const delayTime = RATE_LIMIT_DELAY - timeSinceLastCall;
        console.log(`Throttling API call. Delaying for ${delayTime} ms.`);
        await delay(delayTime);
    }

    lastApiCallTimestamp = Date.now(); // Update timestamp for the last API call
}

async function processTask(task) {
    const { operation, tradeId, attempt = 1, signal, price, coin } = task;
    const maxAttempts = 3;

    try {
        console.log(`Executing task for trade ID: ${tradeId}, Attempt: ${attempt}`);

        // Ensure that the 'coin' object is defined
        if (!coin || !coin.name) {
            throw new Error(`Coin object is not defined or invalid for trade ID: ${tradeId}`);
        }

        // Check if this tradeId has already been processed
        if (processedTrades.has(tradeId)) {
            console.log(`Task for trade ID: ${tradeId} has already been processed. Skipping.`);
            return { success: true }; // Mark as successful to avoid retries
        }

        // Rate-limiting to avoid overwhelming the API
        await throttleApiCall();

        // Execute the operation (swap or trade)
        const result = await operation();  // This includes the call to getSwapEstimate
        console.log(`Raw result for trade ID ${tradeId}:`, JSON.stringify(result));

        // Handle result by checking if transaction ID (txid) is present in the result
        if (result && result.txid) {
            console.log(`Transaction sent: https://solscan.io/tx/${result.txid}`);

            // Ensure signal is defined before calling toUpperCase()
            const actionSignal = signal ? signal.toUpperCase() : 'UNKNOWN';  // Fallback to 'UNKNOWN' if signal is undefined

            // Log the trade only if it hasn't been logged
            if (!processedTrades.has(tradeId)) {
                await logTrade(
                    coin.name,  // Use 'coin.name' now that it's passed in task
                    actionSignal,
                    price,
                    result.amount,
                    result.outputToken,
                    "completed",  // Assuming this is a buy or sell
                    tradeId
                );
                processedTrades.add(tradeId); // Mark trade as processed
                console.log(`Trade successfully logged and completed. Trade ID: ${tradeId}`);
            }

            return { success: true, result };

        } else {
            const errorMsg = `Operation completed but no transaction ID was returned. Trade ID: ${tradeId}`;
            console.error(errorMsg);
            return { success: false, retry: true, error: errorMsg };  // Retryable error
        }

    } catch (error) {
        console.error(`Error processing task for trade ID ${tradeId}: ${error.message}`);

        // Retry logic based on the number of attempts
        if (attempt < maxAttempts) {
            console.log(`Retrying trade with increased slippage... Attempt ${attempt + 1}`);
            await delay(3000);  // Optional delay before retrying
            return {
                success: false,
                retry: true,
                error: error.message
            };
        } else {
            console.error(`Max retry attempts reached for trade ${tradeId}. Logging as failed trade.`);
            await logFailedTrade(tradeId, signal, price, error.message);
            failedTrades.add(tradeId);  // Add to set of failed trades
            return { success: false, retry: false, error: error.message };
        }
    }
}

const retryQueue = [];
const failedTrades = new Set();
const delayBetweenTasks = 5000;  // Add a delay between tasks (in milliseconds)

async function processGlobalQueue() {
    console.log("Starting to process global queue...");

    if (isProcessingQueue) {
        console.log("Queue is already being processed. Will process new items in the next cycle.");
        return;
    }

    isProcessingQueue = true;

    try {
        while (globalQueue.length > 0 || retryQueue.length > 0) {
            console.log(`Starting to process tasks... Global Queue: ${globalQueue.length}, Retry Queue: ${retryQueue.length}`);

            const tasks = [...globalQueue, ...retryQueue];
            globalQueue.length = 0;
            retryQueue.length = 0;

            for (const task of tasks) {
                try {
                    // Check if task has already been processed or failed
                    if (failedTrades.has(task.tradeId) || processedTrades.has(task.tradeId)) {
                        console.log(`Trade ID ${task.tradeId} has already been processed or failed. Skipping task.`);
                        continue;
                    }

                    console.log(`Processing task for trade ID: ${task.tradeId}, Attempt: ${task.attempt}`);

                    // Execute task
                    const result = await processTask(task);

                    if (result.success) {
                        console.log(`Task ${task.tradeId} successfully processed.`);
                    } else if (result.retry && task.attempt < 3) {
                        const nextAttempt = task.attempt + 1;
                        console.log(`Requeueing trade ${task.tradeId} for retry. Attempt ${nextAttempt}`);
                        retryQueue.push({ ...task, attempt: nextAttempt });
                    } else {
                        console.error(`Task ${task.tradeId} failed permanently after max retries.`);
                        failedTrades.add(task.tradeId);  // Mark as failed to prevent requeueing
                    }

                } catch (error) {
                    console.error(`Error processing task ${task.tradeId}: ${error.message}`);
                    failedTrades.add(task.tradeId);  // Mark as failed in case of error
                }

                // Apply a delay between task executions to respect API rate limits
                await delay(delayBetweenTasks);
            }

            console.log("Finished processing tasks.");
            await delay(1000);  // Short delay before checking the queue again
        }
    } catch (error) {
        console.error("Error processing global queue:", error);
    } finally {
        isProcessingQueue = false;
        console.log("Finished processing the global queue.");
        console.log("Awaiting additional signals...");
    }
}

let isBuyQueueProcessing = false;
let isSellQueueProcessing = false;

async function processTradeSignal(signal, tradeData) {
    console.log('Trade Data:', JSON.stringify(tradeData, null, 2));

    const { coin: coinName, amount, price, trend = "Neutral", tradeId } = tradeData || {};

    if (!coinName) {
        console.error(`Error: coinName is undefined in tradeData: ${JSON.stringify(tradeData)}`);
        return null;
    }

    console.log(`Processing ${signal} signal for ${coinName}`);

    try {
        // Load the coin configuration
        const coinsConfig = await readJsonFile(COINS_CONFIG_FILE);
        const coinConfig = coinsConfig.find((c) => c.name === coinName);

        if (!coinConfig) {
            throw new Error(`Coin configuration not found for ${coinName}`);
        }

        const coin = {
            name: coinName,
            inputMint: coinConfig.inputMint,
            outputMint: coinConfig.outputMint,
            decimals: coinConfig.decimals || 9,
        };

        console.log(`Coin configuration: ${JSON.stringify(coin)}`);

        let amountInSmallestUnit;
        let previousBuyPrice;
        let previousBuyAmount;
        
        // Handle SELL signal specifics
        if (signal === "SELL") {
            const tradeLog = await readJsonFile(TRADE_LOG_FILE);
            
            // Match using tradeId to ensure the correct buy trade is found for this sell
            const previousBuy = tradeLog.find(
                (trade) => trade.coin === coinName && trade.action === "BUY" && trade.tradeId === tradeId
            );
        
            // If no previous buy found or it's missing the necessary data, throw an error
            if (!previousBuy || !previousBuy.amountInSol || !previousBuy.price) {
                throw new Error(`No previous BUY found in trade_log for coin ${coinName} with trade ID ${tradeId}. Cannot execute SELL.`);
            }
        
            // Safely assign previousBuyPrice and previousBuyAmount
            previousBuyPrice = previousBuy["price"];  // Alternatively, you can keep previousBuy.price
            previousBuyAmount = previousBuy.amountInSol;
        
            // Convert the previous buy amount into the smallest unit for the trade
            amountInSmallestUnit = new Big(previousBuyAmount)
                .times(new Big(10).pow(coin.decimals))
                .toFixed(0);
        
            console.log(`Previous buy found - Price: ${previousBuyPrice}, Amount: ${previousBuyAmount}`);
        } else {
            // Handle BUY signal
            amountInSmallestUnit = new Big(amount)
                .times(1e9) // Convert SOL to lamports
                .round(0, 0)
                .toString();
        }

        console.log(`Amount in smallest unit: ${amountInSmallestUnit}`);

        // Queue Locking
        if (signal === 'SELL') {
            if (isSellQueueProcessing) {
                console.log('Sell queue is already being processed. Will process new items in the next cycle.');
                return;
            }
            isSellQueueProcessing = true;
        } else if (signal === 'BUY') {
            if (isBuyQueueProcessing) {
                console.log('Buy queue is already being processed. Will process new items in the next cycle.');
                return;
            }
            isBuyQueueProcessing = true;
        }

        // Execute the trade by adding it to the trade queue
        const result = await addToTradeQueue(signal, coin, amountInSmallestUnit, tradeData);

        // Handle the result of the trade execution
        if (result && result.txid) {
            console.log(`Transaction confirmed successfully: ${result.txid}`);
        } else {
            throw new Error(`Trade execution failed for ${coinName}.`);
        }

        // Unlock the queue after processing
        if (signal === 'SELL') {
            isSellQueueProcessing = false;
        } else if (signal === 'BUY') {
            isBuyQueueProcessing = false;
        }

        return result;
    } catch (error) {
        console.error(`Error processing ${signal} signal for ${coinName}: ${error.message}`);
        const errorPrice = signal === "SELL" ? previousBuyPrice : price;
        await logFailedTrade(coinName, signal, errorPrice, error.message);

        // Unlock the queue after an error
        if (signal === 'SELL') {
            isSellQueueProcessing = false;
        } else if (signal === 'BUY') {
            isBuyQueueProcessing = false;
        }

        return null;
    }
}

async function getSwapEstimate(inputMint, outputMint, amount, outputMintDecimals = 9, options = {}) {
    try {
        if (!inputMint || !outputMint || !amount || isNaN(amount) || new Big(amount).lte(0)) {
            throw new Error(`Invalid parameters passed to getSwapEstimate: inputMint=${inputMint}, outputMint=${outputMint}, amount=${amount}`);
        }

        console.log(`Initiating swap estimate request for inputMint: ${inputMint}, outputMint: ${outputMint}, amount: ${amount}`);

        const slippageBps = options.slippageBps || 50;
        console.log(`Using slippage: ${slippageBps} bps`);

        const response = await axiosInstance.get("/quote", {
            params: {
                inputMint,
                outputMint,
                amount: amount.toString(),
                slippageBps: slippageBps.toString(),
                enforceMEVProtection: options.enforceMEVProtection ? 'true' : 'false',
            },
            httpsAgent: agent,
        });

        console.log(`Received response from swap estimate API`);

        if (!response.data || !response.data.outAmount) {
            throw new Error(`Invalid response from swap estimate API. Missing outAmount for swap: ${inputMint} to ${outputMint}`);
        }

        const { inAmount, outAmount } = response.data;
        console.log(`Swap estimate details - inAmount: ${inAmount}, outAmount: ${outAmount}`);

        const inputInSol = new Big(inAmount).div(1e9);
        const outAmountInUnits = new Big(outAmount).div(new Big(10).pow(outputMintDecimals));

        const pricePerToken = inputInSol.div(outAmountInUnits);
        console.log(`Calculated price per token: ${pricePerToken.toFixed(8)} SOL`);

        const result = {
            ...response.data,
            pricePerToken: pricePerToken.toFixed(8),
            inputInSol: inputInSol.toFixed(9),
            outAmountInUnits: outAmountInUnits.toFixed(outputMintDecimals),
        };

        console.log(`Swap estimate processed successfully. Returning result.`);
        return result;
    } catch (error) {
        console.error(`Error in getSwapEstimate for ${inputMint} to ${outputMint}:`, error);
        throw new Error(`Failed to get swap estimate: ${error.message}. Input: ${inputMint}, Output: ${outputMint}, Amount: ${amount}`);
    }
}

async function addToTradeQueue(signal, coin, amount, tradeData = null, attempt = 1, slippageBps = 60) {
    console.log(`Adding to trade queue: Signal=${signal}, Coin=${JSON.stringify(coin)}, Amount=${amount}, Attempt=${attempt}, Initial Slippage=${slippageBps}bps`);

    if (signal === "SELL") {
        attempt = 1;  // Always reset the attempt to 1 when processing a sell signal
    }

    const tradeId = tradeData?.tradeId || Date.now().toString();
    const price = tradeData?.price ? parseFloat(tradeData.price).toFixed(8) : null;
    const trend = tradeData?.trend || "Neutral";

    if (failedTrades.has(tradeId)) {
        console.log(`Trade ${tradeId} has already failed. Skipping.`);
        return;
    }

    if (!signal || !coin?.inputMint || !coin?.outputMint || !amount || isNaN(amount) || new Big(amount).lte(0)) {
        throw new Error(`Invalid parameters for addToTradeQueue: Signal=${signal}, Coin=${JSON.stringify(coin)}, Amount=${amount}`);
    }

    const maxAttempts = 3;
    const confirmationTimeout = 30000;  // Adjusted confirmation timeout to 40 seconds

    const operation = async () => {
        for (let currentAttempt = attempt; currentAttempt <= maxAttempts; currentAttempt++) {
            try {
                console.log(`Processing trade: Signal=${signal}, Coin=${coin.name}, Amount=${amount}, Trade ID: ${tradeId}, Attempt=${currentAttempt}, Slippage=${slippageBps}bps`);

                // Fetch swap estimate
                const swapEstimate = await getSwapEstimate(
                    signal === "SELL" ? coin.outputMint : coin.inputMint,
                    signal === "SELL" ? coin.inputMint : coin.outputMint,
                    amount,
                    coin.decimals,
                    { onlyDirectRoutes: false, slippageBps }
                );

                if (!swapEstimate?.outAmount) {
                    throw new Error("Failed to get a valid swap estimate.");
                }

                // Prepare and send swap request
                const swapRequestData = {
                    quoteResponse: swapEstimate,
                    userPublicKey: wallet.publicKey.toString(),
                    wrapUnwrapSOL: true,
                };

                console.log("Sending swap request...");
                const swapResponse = await axiosInstance.post("https://quote-api.jup.ag/v6/swap", swapRequestData);

                if (!swapResponse.data?.swapTransaction) {
                    throw new Error("Swap transaction is missing or invalid in the response.");
                }

                // Sign and send transaction
                const transaction = VersionedTransaction.deserialize(Buffer.from(swapResponse.data.swapTransaction, "base64"));
                transaction.sign([wallet.payer]);

                const txid = await connection.sendRawTransaction(transaction.serialize(), { skipPreflight: true, maxRetries: 2 });
                console.log(`Transaction sent: https://solscan.io/tx/${txid}`);

                // Confirm transaction
                const confirmation = await Promise.race([
                    connection.confirmTransaction(txid, { commitment: "confirmed" }),
                    new Promise((_, reject) => setTimeout(() => reject(new Error("Confirmation timeout")), confirmationTimeout))
                ]);

                if (!confirmation || confirmation.value?.err) {
                    throw new Error(`Transaction failed or timed out: ${txid}`);
                }

                console.log(`Transaction confirmed successfully: ${txid}`);

                // Process successful trade
                const decimals = signal === "SELL" ? 9 : coin.decimals;
                const outAmountInNative = new Big(swapResponse.data.outAmount || swapEstimate.outAmount).div(new Big(10).pow(decimals));

                console.log(`Trade executed successfully. Out amount: ${outAmountInNative.toFixed(decimals)} ${signal === "SELL" ? "SOL" : coin.name}`);

                // Log trade
                await logTrade(
                    coin.name,
                    signal.toUpperCase(),
                    price,
                    outAmountInNative.toFixed(decimals),
                    trend,
                    signal === "SELL" ? "sold" : "completed",
                    tradeId
                );

                // Return the result object with txid
                return {
                    txid,
                    tradeId,
                    amount: outAmountInNative.toFixed(decimals),
                    outputToken: signal === "SELL" ? "SOL" : coin.name,
                    price
                };

            } catch (error) {
                console.error(`Error executing trade for ${coin.name} (Attempt ${currentAttempt}): ${error.message}`);

                if (currentAttempt < maxAttempts) {
                    console.log(`Retrying trade with increased slippage...`);
                    slippageBps += 15;  // Increase slippage for next attempt
                    await delay(3000);  // Delay before retrying
                } else {
                    console.error(`Max retry attempts reached for ${coin.name}. Logging as failed trade.`);
                    await logFailedTrade(coin.name, signal, price, `Max retry attempts reached: ${error.message}`);
                    failedTrades.add(tradeId);  // Mark this trade as failed
                    return { success: false };  // Return failure result
                }
            }
        }
    };

    return new Promise((resolve, reject) => {
        const operationWrapper = async () => {
            try {
                const result = await operation();  // This is your main operation logic
                console.log(`Operation result for trade ${tradeId}:`, JSON.stringify(result));
                if (result && result.txid) {
                    console.log(`Trade executed successfully. Trade ID: ${tradeId}, Transaction ID: ${result.txid}`);
                    resolve(result);
                    return result;  // Ensure the result is returned
                } else if (result && result.success === false) {
                    console.log(`Trade ${tradeId} has failed after max retries. Not requeueing.`);
                    reject(new Error(`Trade ${tradeId} failed after maximum retries.`));
                }
            } catch (error) {
                console.error(`Trade execution failed. Trade ID: ${tradeId}. Error:`, error);
                await logFailedTrade(coin.name, signal, price, error.message);
                reject(error);
            }
        };

        if (!failedTrades.has(tradeId)) {  // Only push to the queue if not already failed
            globalQueue.push({
                operation: operationWrapper,
                tradeId,
                attempt,
                signal,
                coin,
                price
            });
        } else {
            console.log(`Trade ID ${tradeId} has already failed and will not be added to the queue.`);
        }
        processGlobalQueue();
    });
}


async function logFailedTrade(tradeId, signal, price, errorMessage) {
    try {
        // Validate and format the trade ID
        const validTradeId = tradeId ? tradeId.toString() : "UNKNOWN_TRADE_ID";

        // Log the failed trade by updating the status and adding an error message
        await db.execute(`
            UPDATE trade_logs
            SET status = 'FAILED', error = ?, price = ?, timestamp = NOW()
            WHERE trade_id = ?
        `, [
            errorMessage || "Unknown error",      // Provide fallback error message
            price ? price.toString() : "N/A",     // Ensure price is stored as a string
            validTradeId
        ]);

        console.log(`Failed trade logged for Trade ID: ${validTradeId}`);

        // Add the failed trade ID to the set to prevent requeuing
        failedTrades.add(validTradeId);
    } catch (error) {
        console.error(`Error logging failed trade for Trade ID: ${tradeId}`, error);
    }
}


async function logTrade(coin, action, price, amount, trend, tradeStatus, tradeId) {
    console.log(`Attempting to log trade: Coin=${coin}, Action=${action}, Price=${price}, Amount=${amount}, Trend=${trend}, TradeStatus=${tradeStatus}, Trade ID=${tradeId}`);

    // Check if tradeId has already been logged to avoid duplicate entries
    if (processedTrades.has(tradeId)) {
        console.log(`Trade ID ${tradeId} has already been logged. Skipping logging.`);
        return;
    }

    try {
        // Validate price and amount
        if (!price || isNaN(parseFloat(price))) {
            throw new Error(`Invalid price: ${price}`);
        }

        if (!amount || isNaN(parseFloat(amount))) {
            throw new Error(`Invalid amount: ${amount}`);
        }

        if (!tradeId) {
            throw new Error(`Invalid or missing tradeId: ${tradeId}`);
        }

        const priceBig = new Big(parseFloat(price)).toFixed(8);
        const amountBig = new Big(parseFloat(amount)).toFixed(9);

        if (new Big(priceBig).lte(0) || new Big(amountBig).lte(0)) {
            throw new Error(`Invalid price or amount: price=${price}, amount=${amount}`);
        }

        // Insert the new trade directly into the trade_logs table
        await db.execute(`
            INSERT INTO trade_logs (trade_id, coin, action, price, amount, trend, status, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, NOW())
        `, [
            tradeId,
            coin,
            action.toUpperCase(),
            priceBig,
            amountBig,
            trend || "Neutral",
            tradeStatus || "completed"
        ]);

        console.log(`Trade successfully logged for ${coin}, Trade ID: ${tradeId} in the database`);

        // Add tradeId to the processedTrades set to prevent future logging of the same trade
        processedTrades.add(tradeId);

        // Return the trade data if needed
        return {
            tradeId,
            coin,
            action: action.toUpperCase(),
            price: priceBig,
            amountInSol: amountBig,
            trend: trend || "Neutral",
            status: tradeStatus || "completed",
            timestamp: new Date().toISOString(),
        };
    } catch (error) {
        console.error(`Error processing trade log for ${coin}: ${error.message}`);
        throw error; // Re-throw to allow caller to handle the error
    }
}

export { logTrade };

async function processBuySignal(buySignal) {
    if (isProcessingBuySignal) {
        pendingSignals.push({ type: 'BUY', data: buySignal });
        return;
    }

    isProcessingBuySignal = true;
    let signalKey;

    try {
        if (buySignal && buySignal.coin && !buySignal.processed) {
            const { coin, price, amount } = buySignal;
            signalKey = `${coin}-${parseFloat(price).toFixed(8)}-${parseFloat(amount).toFixed(9)}`;

            if (activeBuySignals[signalKey]) {
                console.log(`Buy signal for ${coin} at price ${price} and amount ${amount} is already being processed, skipping...`);
                return;
            }

            activeBuySignals[signalKey] = true;
            buySignal.processed = true;

            console.log("[solana-logic] Buy signal detected. Adding to queue...");

            const defaultSlippageBps = 65;
            const result = await addToTradeQueue("BUY", {
                name: buySignal.coin,
                inputMint: buySignal.inputMint,
                outputMint: buySignal.outputMint,
                decimals: buySignal.decimals || 9,
            }, buySignal.amount, buySignal, 1, defaultSlippageBps);

            if (result && result.txid) {
                console.log(`Buy trade executed successfully: ${result.txid}`);
            } else {
                console.log("Buy trade failed or no transaction ID returned.");
            }

            await delay(4000);  // Adding a delay to prevent overlapping executions

        }
    } catch (error) {
        console.error("Error processing buy signal:", error);
        await logFailedTrade(buySignal.coin, "BUY", buySignal.price, error.message);
    } finally {
        if (signalKey) {
            delete activeBuySignals[signalKey];
        }
        isProcessingBuySignal = false;

        // Check if there are pending signals, and process the next one
        if (pendingSignals.length > 0) {
            const nextSignal = pendingSignals.shift();
            await processBuySignal(nextSignal.data);
        }
    }
}

async function processSellSignal(sellSignal) {
    if (isProcessingSellSignal) {
        pendingSignals.push({ type: 'SELL', data: sellSignal });
        return;
    }

    isProcessingSellSignal = true;
    let signalKey;

    try {
        if (sellSignal && sellSignal.coin && !sellSignal.processed) {
            const { coin, price, amount } = sellSignal;
            signalKey = `${coin}-${parseFloat(price).toFixed(8)}-${parseFloat(amount).toFixed(9)}`;

            if (activeSellSignals[signalKey]) {
                console.log(`Sell signal for ${coin} at price ${price} and amount ${amount} is already being processed, skipping...`);
                return;
            }

            activeSellSignals[signalKey] = true;

            // Immediately mark the sell signal as processed
            sellSignal.processed = true;

            console.log("Sell signal detected:", sellSignal);

            // Reset attempt count for the sell signal
            sellSignal.attempt = 1;

            const result = await processTradeSignal("SELL", sellSignal);

            if (result && result.txid) {
                console.log(`Sell trade executed successfully: ${result.txid}`);
                // Reset the attempt count since the sell was successful
                sellSignal.attempt = 0;
            } else {
                console.log("Sell trade failed or no transaction ID returned.");
            }

            await delay(4000); // Delay to prevent overlapping executions

        }
    } catch (error) {
        console.error("Error processing sell signal:", error);

        // Log the failed trade but ensure it logs the proper tradeId and not the coin name
        await logFailedTrade(sellSignal.tradeId || sellSignal.coin, "SELL", sellSignal.price, error.message);
    } finally {
        if (signalKey) {
            delete activeSellSignals[signalKey];
        }
        isProcessingSellSignal = false;

        // Check if there are pending signals, and process the next one
        if (pendingSignals.length > 0) {
            const nextSignal = pendingSignals.shift();
            await processSellSignal(nextSignal.data);
        }
    }
}

const processedTrades = new Set();

function resetSignalTracking() {
    activeBuySignals = {};
    activeSellSignals = {};
    processedTrades.clear();  // Also clear previously processed trades from memory
}

async function main() {
    console.log('Initializing trading system...');

    // Initialize signal tracking
    console.log('Initializing signal tracking...');
    resetSignalTracking();
    console.log('Signal tracking initialized.');

    // Start queue processing
    processQueues();

    console.log('Entering main processing loop...');
    while (true) {
        try {
            await checkForSignals();
        } catch (error) {
            console.error("Error during signal processing:", error.message);
        }
        await delay(5000);
    }
}

// Execute the main function and handle any potential errors
main().catch((error) => {
    console.error("Fatal error in main function:", error);
    process.exit(1);
});

process.on("SIGINT", async () => {
    console.log("Caught interrupt signal. Performing graceful shutdown...");

    try {
        // Save state of in-process trades and queues to the database

        // 1. Save globalQueue and retryQueue to database (as pending trades)
        for (const task of globalQueue) {
            await db.execute(`
                INSERT INTO pending_tasks (task_id, type, coin, action, amount, price, status)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE status = 'pending'
            `, [
                task.id,
                task.type || 'unknown',
                task.coin || 'unknown',
                task.action || 'unknown',
                task.amount || 0,
                task.price || 0,
                'pending'
            ]);
        }

        for (const task of retryQueue) {
            await db.execute(`
                INSERT INTO pending_tasks (task_id, type, coin, action, amount, price, status)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE status = 'retry-pending'
            `, [
                task.id,
                task.type || 'retry',
                task.coin || 'unknown',
                task.action || 'unknown',
                task.amount || 0,
                task.price || 0,
                'retry-pending'
            ]);
        }

        // 2. Update processedTrades to reflect any trades that are in-process
        for (const tradeId of processedTrades) {
            await db.execute(`
                UPDATE trade_logs SET status = 'in-process'
                WHERE trade_id = ?
            `, [tradeId]);
        }

        console.log("State of queues and in-process trades saved to the database successfully.");
    } catch (error) {
        console.error("Error during graceful shutdown:", error.message);
    } finally {
        process.exit();
    }
});