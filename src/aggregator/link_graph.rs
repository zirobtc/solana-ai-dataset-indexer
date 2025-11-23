use crate::aggregator::graph_schema::{
    BundleTradeLink, BurnedLink, CoordinatedActivityLink, CopiedTradeLink, LockedSupplyLink,
    MintedLink, ProvidedLiquidityLink, SnipedLink, TopTraderOfLink, TransferLink, WhaleOfLink,
};
use crate::handlers::constants::{
    NATIVE_MINT, PROTOCOL_PUMPFUN_LAUNCHPAD, USD1_MINT, USDC_MINT, USDT_MINT,
};
use crate::types::{
    BurnRow, EventPayload, EventType, LiquidityRow, MintRow, SupplyLockRow, TradeRow, TransferRow,
};
use anyhow::{Result, anyhow};
use chrono::Utc;
use clickhouse::{Client, Row};
use futures::stream::{self, StreamExt};
use itertools::Itertools;
use neo4rs::{BoltType, Graph, query};
use once_cell::sync::Lazy;
use redis::FromRedisValue;
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, Client as RedisClient, aio::MultiplexedConnection};
use serde::Deserialize;
use solana_sdk::native_token::LAMPORTS_PER_SOL;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::Instant;
use tokio::time::sleep;
use tokio::try_join;

fn decimals_for_quote(mint: &str) -> u8 {
    if mint == NATIVE_MINT {
        9
    } else if mint == USDC_MINT || mint == USDT_MINT || mint == USD1_MINT {
        6
    } else {
        9 // default assumption if unknown
    }
}

static LINK_GRAPH_DLQ_STREAM: Lazy<String> = Lazy::new(|| {
    std::env::var("LINK_GRAPH_DLQ_STREAM").unwrap_or_else(|_| "link_graph_dlq".to_string())
});

// Serialize Neo4j writes when multiple LinkGraph workers are running.
static NEO4J_WRITE_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

#[derive(Debug)]
struct LinkGraphConfig {
    time_window_seconds: u32,
    queue_threshold: i64,
    copied_trade_window_seconds: i64,
    sniper_rank_threshold: u64,
    whale_rank_threshold: u64,
    min_top_trader_pnl: f32,
    min_trade_total_usd: f64,
    ath_price_threshold_usd: f64,
    redis_read_count: usize,
    redis_block_ms: u64,
    redis_drain_threshold: i64,
    chunk_size_large: usize,
    chunk_size_historical: usize,
    chunk_size_mint_small: usize,
    chunk_size_mint_large: usize,
    chunk_size_token: usize,
}

static LINK_GRAPH_CONFIG: Lazy<LinkGraphConfig> = Lazy::new(|| LinkGraphConfig {
    time_window_seconds: env_parse("LINK_GRAPH_TIME_WINDOW_SECONDS", 120_u32),
    queue_threshold: env_parse("LINK_GRAPH_QUEUE_THRESHOLD", 40_000_i64),
    copied_trade_window_seconds: env_parse("LINK_GRAPH_COPIED_TRADE_WINDOW_SECONDS", 60_i64),
    sniper_rank_threshold: env_parse("LINK_GRAPH_SNIPER_RANK_THRESHOLD", 45_u64),
    whale_rank_threshold: env_parse("LINK_GRAPH_WHALE_RANK_THRESHOLD", 5_u64),
    min_top_trader_pnl: env_parse("LINK_GRAPH_MIN_TOP_TRADER_PNL", 1.0_f32),
    min_trade_total_usd: env_parse("LINK_GRAPH_MIN_TRADE_TOTAL_USD", 20.0_f64),
    ath_price_threshold_usd: env_parse("LINK_GRAPH_ATH_PRICE_THRESHOLD_USD", 0.0002000_f64),
    redis_read_count: env_parse("LINK_GRAPH_REDIS_READ_COUNT", 5000_usize),
    redis_block_ms: env_parse("LINK_GRAPH_REDIS_BLOCK_MS", 100_u64),
    redis_drain_threshold: env_parse("LINK_GRAPH_REDIS_DRAIN_THRESHOLD", 5000_i64),
    chunk_size_large: env_parse("LINK_GRAPH_CHUNK_SIZE_LARGE", 3000_usize),
    chunk_size_historical: env_parse("LINK_GRAPH_CHUNK_SIZE_HISTORICAL", 1000_usize),
    chunk_size_mint_small: env_parse("LINK_GRAPH_CHUNK_SIZE_MINT_SMALL", 1500_usize),
    chunk_size_mint_large: env_parse("LINK_GRAPH_CHUNK_SIZE_MINT_LARGE", 3000_usize),
    chunk_size_token: env_parse("LINK_GRAPH_CHUNK_SIZE_TOKEN", 3000_usize),
});

fn env_parse<T: FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<T>().ok())
        .unwrap_or(default)
}

#[derive(Row, Clone, Debug, Deserialize)]
struct HistoricalTrade {
    signature: String,
    timestamp: u32,
    maker: String,
    total: f64,
    slippage: f32,
}

enum FollowerLink {
    Copied(CopiedTradeLink),
    Coordinated(CoordinatedActivityLink),
}

pub struct LinkGraph {
    db_client: Client,
    neo4j_client: Arc<Graph>,
    redis_conn: MultiplexedConnection,
}

#[derive(Row, Deserialize, Debug)]
struct Ping {
    alive: u8,
}
#[derive(Row, Deserialize, Debug)]
struct CountResult {
    count: u64,
}

impl LinkGraph {
    pub async fn new(
        db_client: Client,
        neo4j_client: Arc<Graph>,
        redis_client: RedisClient,
    ) -> Result<Self> {
        let _: Ping = db_client.query("SELECT 1 as alive").fetch_one().await?;
        neo4j_client.run(query("MATCH (n) RETURN count(n)")).await?;
        let redis_conn = redis_client.get_multiplexed_async_connection().await?;
        println!("[WalletGraph] ✔️ Connected to ClickHouse, Neo4j, and Redis.");
        Ok(Self {
            db_client,
            neo4j_client,
            redis_conn,
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        let stream_key = "link_graph_queue";
        let group_name = "link_graph_analyzers";
        let consumer_name = format!("consumer-{}", uuid::Uuid::new_v4());

        let result: redis::RedisResult<()> = self
            .redis_conn
            .xgroup_create_mkstream(stream_key, group_name, "0")
            .await;

        if let Err(e) = result {
            if !e.to_string().contains("BUSYGROUP") {
                return Err(anyhow!(
                    "Failed to create or connect to consumer group: {}",
                    e
                ));
            }
            println!(
                "[LinkGraph] Consumer group '{}' already exists. Resuming.",
                group_name
            );
        } else {
            println!(
                "[LinkGraph] Created new consumer group '{}' on stream '{}'.",
                group_name, stream_key
            );
        }

        let mut message_buffer: Vec<(String, EventPayload)> = Vec::new();
        let cfg = &*LINK_GRAPH_CONFIG;

        loop {
            let queue_len: i64 = self.redis_conn.xlen(stream_key).await.unwrap_or(0);
            if queue_len < cfg.queue_threshold {
                println!(
                    "[LinkGraph] Queue size ({}) is below threshold ({}). Waiting...",
                    queue_len, cfg.queue_threshold
                );
                tokio::time::sleep(Duration::from_secs(15)).await;
                continue;
            }

            // --- CORRECTED COLLECTION PHASE ---
            loop {
                // First, check if the buffer we already have contains a full window.
                if !message_buffer.is_empty() {
                    // Ensure buffer is sorted before checking timestamps
                    message_buffer.sort_by_key(|(_, p)| p.timestamp);
                    let first_ts = message_buffer.first().unwrap().1.timestamp;
                    let last_ts = message_buffer.last().unwrap().1.timestamp;
                    // If the duration is sufficient, break to process it.
                    if last_ts >= first_ts + cfg.time_window_seconds {
                        break;
                    }
                }

                // If the window is not yet full, fetch more messages.
                let new_messages = collect_events_from_stream(
                    &self.redis_conn,
                    stream_key,
                    group_name,
                    &consumer_name,
                )
                .await?;

                // =========================================================================
                // >> THE CRITICAL FIX <<
                // If Redis returns nothing, we DON'T break. We just loop again,
                // which will either re-check the buffer or wait if the queue is drained.
                // This prevents processing incomplete windows.
                // =========================================================================
                if new_messages.is_empty() {
                    // If the stream is truly empty and our buffer is still not full,
                    // it means we've reached the end of the data stream. We should process what we have.
                    if queue_len < cfg.redis_drain_threshold {
                        // Check against the Redis read size
                        println!("[LinkGraph] Stream is draining. Processing final partial batch.");
                        break;
                    }
                    // Otherwise, just wait briefly to avoid busy-looping on a fast stream.
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                }

                message_buffer.extend(new_messages);
            }

            if message_buffer.is_empty() {
                continue;
            }

            // --- PARTITIONING PHASE ---
            message_buffer.sort_by_key(|(_, p)| p.timestamp);
            let window_start_ts = message_buffer.first().unwrap().1.timestamp;

            let split_index = message_buffer
                .partition_point(|(_, p)| p.timestamp < window_start_ts + cfg.time_window_seconds);

            // Enforce a minimum batch size; collect more if below threshold.
            if split_index < cfg.queue_threshold as usize {
                println!(
                    "[LinkGraph] [Debug] Window size {} below min threshold {}. Collecting more...",
                    split_index, cfg.queue_threshold
                );
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }

            let messages_to_process = message_buffer.drain(..split_index).collect::<Vec<_>>();
            let (window_message_ids, window_payloads): (Vec<_>, Vec<_>) =
                messages_to_process.into_iter().unzip();

            // --- PROCESSING PHASE ---
            let payload_count = window_payloads.len();
            if payload_count == 0 {
                continue;
            }

            if payload_count < 100 {
                let queue_len_now: i64 = self.redis_conn.xlen(stream_key).await.unwrap_or(-1);
                println!(
                    "[LinkGraph] [Debug] Small window: {} events, buffer_remaining={}, queue_len={}",
                    payload_count,
                    message_buffer.len(),
                    queue_len_now
                );
            }

            let first_ts = window_payloads.first().map_or(0, |p| p.timestamp);
            let last_ts = window_payloads.last().map_or(0, |p| p.timestamp);
            let on_chain_duration_secs = if last_ts > first_ts {
                last_ts - first_ts
            } else {
                0
            };

            let batch_start_time = Instant::now();
            println!(
                "[LinkGraph] ⚙️ Processing window of {} events (covering {}s on-chain)...",
                payload_count, on_chain_duration_secs
            );

            let mut attempts = 0;
            let max_retries = 3;
            let mut handled = false;
            while attempts <= max_retries {
                match self.process_batch(window_payloads.clone()).await {
                    Ok(_) => {
                        let result: redis::RedisResult<i64> = self
                            .redis_conn
                            .xack(stream_key, group_name, &window_message_ids)
                            .await;
                        if let Err(e) = result {
                            eprintln!("[LinkGraph] 🔴 FAILED to acknowledge messages: {}", e);
                        } else if let Err(e) = self
                            .redis_conn
                            .xdel::<_, _, i64>(stream_key, &window_message_ids)
                            .await
                        {
                            eprintln!("[LinkGraph] 🔴 FAILED to delete messages: {}", e);
                        }
                        handled = true;
                        break;
                    }
                    Err(e) => {
                        let err_str = e.to_string();
                        if err_str.contains("DeadlockDetected") && attempts < max_retries {
                            attempts += 1;
                            let backoff_ms = 200 * attempts;
                            eprintln!(
                                "[LinkGraph] ⚠️ Deadlock detected, retrying {}/{} after {}ms",
                                attempts, max_retries, backoff_ms
                            );
                            sleep(Duration::from_millis(backoff_ms as u64)).await;
                            continue;
                        } else {
                            eprintln!(
                                "[LinkGraph] ❌ Failed to process batch (no ACK). Error: {}",
                                err_str
                            );
                            // Attempt to move to DLQ to avoid blocking the stream.
                            let mut conn = self.redis_conn.clone();
                            let mut dlq_ok = true;
                            for payload in window_payloads.iter() {
                                match bincode::serialize(payload) {
                                    Ok(data) => {
                                        if let Err(dlq_err) = conn
                                            .xadd::<_, _, _, _, ()>(
                                                &*LINK_GRAPH_DLQ_STREAM,
                                                "*",
                                                &[("payload", data)],
                                            )
                                            .await
                                        {
                                            eprintln!(
                                                "[LinkGraph] 🔴 Failed to write to DLQ: {}",
                                                dlq_err
                                            );
                                            dlq_ok = false;
                                            break;
                                        }
                                    }
                                    Err(se) => {
                                        eprintln!(
                                            "[LinkGraph] 🔴 Failed to serialize payload for DLQ: {}",
                                            se
                                        );
                                        dlq_ok = false;
                                        break;
                                    }
                                }
                            }
                            if dlq_ok {
                                let result: redis::RedisResult<i64> = self
                                    .redis_conn
                                    .xack(stream_key, group_name, &window_message_ids)
                                    .await;
                                if let Err(e) = result {
                                    eprintln!(
                                        "[LinkGraph] 🔴 FAILED to acknowledge messages after DLQ: {}",
                                        e
                                    );
                                } else if let Err(e) = self
                                    .redis_conn
                                    .xdel::<_, _, i64>(stream_key, &window_message_ids)
                                    .await
                                {
                                    eprintln!(
                                        "[LinkGraph] 🔴 FAILED to delete messages after DLQ: {}",
                                        e
                                    );
                                }
                                handled = true;
                            }
                            break;
                        }
                    }
                }
            }
            if !handled {
                eprintln!("[LinkGraph] ⚠️ Batch left pending due to repeated failures.");
            }

            let elapsed_secs = batch_start_time.elapsed().as_secs_f64();
            if elapsed_secs > 0.0 {
                let eps = payload_count as f64 / elapsed_secs;
                println!(
                    "[LinkGraph] ⏱️ Batch processed in {:.2}s ({:.2} events/sec).",
                    elapsed_secs, eps
                );
            }
        }
    }

    async fn process_time_window(&self, payloads: &[EventPayload]) -> Result<()> {
        let cfg = &*LINK_GRAPH_CONFIG;
        let mut unique_wallets = HashSet::new();
        let mut unique_tokens = HashSet::new();
        let mut trades = Vec::new();
        let mut transfers = Vec::new();
        let mut mints = Vec::new();
        let mut supply_locks = Vec::new();
        let mut burns = Vec::new();
        let mut liquidity_events = Vec::new();

        for payload in payloads {
            match &payload.event {
                EventType::Trade(trade) => {
                    // Skip dust trades to reduce noise in downstream links/datasets
                    if trade.total_usd >= cfg.min_trade_total_usd {
                        unique_wallets.insert(trade.maker.clone());
                        unique_tokens.insert(trade.base_address.clone());
                        trades.push(trade.clone());
                    }
                }
                EventType::Transfer(transfer) => {
                    unique_wallets.insert(transfer.source.clone());
                    unique_wallets.insert(transfer.destination.clone());
                    transfers.push(transfer.clone());
                }
                EventType::Mint(mint) => {
                    unique_wallets.insert(mint.creator_address.clone());
                    unique_tokens.insert(mint.mint_address.clone());
                    mints.push(mint.clone());
                }
                EventType::SupplyLock(lock) => {
                    unique_wallets.insert(lock.sender.clone());
                    unique_wallets.insert(lock.recipient.clone());
                    unique_tokens.insert(lock.mint_address.clone());
                    supply_locks.push(lock.clone());
                }
                EventType::Burn(burn) => {
                    unique_wallets.insert(burn.source.clone());
                    unique_tokens.insert(burn.mint_address.clone());
                    burns.push(burn.clone());
                }
                EventType::Liquidity(liquidity) => {
                    if liquidity.change_type == 0 {
                        // 0 = Add Liquidity
                        unique_wallets.insert(liquidity.lp_provider.clone());
                        liquidity_events.push(liquidity.clone());
                    }
                }
                _ => {}
            }
        }

        // Run link detection in parallel; writes remain serialized by the global Neo4j lock.
        let parallel_start = Instant::now();
        try_join!(
            self.process_mints(&mints, &trades),
            self.process_transfers_and_funding(&transfers),
            self.process_supply_locks(&supply_locks),
            self.process_burns(&burns),
            self.process_liquidity_events(&liquidity_events),
            self.process_trade_patterns(&trades, &mints),
        )?;
        println!(
            "[LinkGraph] [TimeWindow] Parallel link processing finished in: {:?}",
            parallel_start.elapsed()
        );
        Ok(())
    }

    async fn process_batch(&self, mut payloads: Vec<EventPayload>) -> Result<()> {
        if payloads.is_empty() {
            return Ok(());
        }

        // Payloads are already a complete time-window. We just need to sort them.
        payloads.sort_by_key(|p| p.timestamp);

        // Process the entire batch as a single logical unit.
        self.process_time_window(&payloads).await?;

        println!(
            "[LinkGraph] Finished processing batch of {} events.",
            payloads.len()
        );
        Ok(())
    }

    // --- Main Logic for Pattern Detection ---
    async fn process_mints(
        &self,
        mints: &[MintRow],
        all_trades_in_batch: &[TradeRow],
    ) -> Result<()> {
        let start = Instant::now();
        if mints.is_empty() {
            return Ok(());
        }
        let mut links = Vec::new();

        for mint in mints {
            let dev_buy = all_trades_in_batch.iter().find(
                |t| {
                    t.maker == mint.creator_address
                        && t.base_address == mint.mint_address
                        && t.trade_type == 0
                }, // 0 = Buy
            );
            let buy_amount_decimals = dev_buy.map_or(0.0, |t| {
                let quote_decimals = decimals_for_quote(&t.quote_address);
                t.quote_amount as f64 / 10f64.powi(quote_decimals as i32)
            });
            links.push(MintedLink {
                signature: mint.signature.clone(),
                timestamp: mint.timestamp as i64,
                buy_amount: buy_amount_decimals,
            });
        }
        self.write_minted_links(&links, mints).await?;
        println!("[LinkGraph] [Profile] process_mints: {} mints in {:?}", mints.len(), start.elapsed());
        Ok(())
    }

    async fn process_supply_locks(&self, locks: &[SupplyLockRow]) -> Result<()> {
        let start = Instant::now();
        if locks.is_empty() {
            return Ok(());
        }
        let links: Vec<_> = locks
            .iter()
            .map(|l| LockedSupplyLink {
                signature: l.signature.clone(),
                amount: l.total_locked_amount as f64,
                timestamp: l.timestamp as i64,
                unlock_timestamp: l.final_unlock_timestamp,
            })
            .collect();
        self.write_locked_supply_links(&links, locks).await?;
        println!("[LinkGraph] [Profile] process_supply_locks: {} locks in {:?}", locks.len(), start.elapsed());
        Ok(())
    }

    async fn process_burns(&self, burns: &[BurnRow]) -> Result<()> {
        let start = Instant::now();
        if burns.is_empty() {
            return Ok(());
        }
        let links: Vec<_> = burns
            .iter()
            .map(|b| BurnedLink {
                signature: b.signature.clone(),
                amount: b.amount_decimal,
                timestamp: b.timestamp as i64,
            })
            .collect();
        self.write_burned_links(&links, burns).await?;
        println!("[LinkGraph] [Profile] process_burns: {} burns in {:?}", burns.len(), start.elapsed());
        Ok(())
    }

    async fn process_transfers_and_funding(&self, transfers: &[TransferRow]) -> Result<()> {
        let start = Instant::now();
        if transfers.is_empty() {
            return Ok(());
        }

        // Directly map every TransferRow to a TransferLink without any extra logic.
        let transfer_links: Vec<TransferLink> = transfers
            .iter()
            .map(|transfer| TransferLink {
                source: transfer.source.clone(),
                destination: transfer.destination.clone(),
                signature: transfer.signature.clone(),
                mint: transfer.mint_address.clone(),
                timestamp: transfer.timestamp as i64,
                amount: transfer.amount_decimal,
            })
            .collect();

        self.write_transfer_links(&transfer_links).await?;
        println!("[LinkGraph] [Profile] process_transfers: {} transfers in {:?}", transfers.len(), start.elapsed());
        Ok(())
    }

    async fn process_trade_patterns(
        &self,
        trades: &[TradeRow],
        mints_in_batch: &[MintRow],
    ) -> Result<()> {
        let start = Instant::now();
        if trades.is_empty() {
            return Ok(());
        }

        let creator_map: HashMap<String, String> = mints_in_batch
            .iter()
            .map(|m| (m.mint_address.clone(), m.creator_address.clone()))
            .collect();

        let mut processed_pairs = HashSet::new();

        let bundle_links = self.detect_bundle_trades(trades, &mut processed_pairs);
        if !bundle_links.is_empty() {
            self.write_bundle_trade_links(&bundle_links).await?;
        }

        let follower_links = self
            .detect_follower_activity(trades, &mut processed_pairs)
            .await?;
        if !follower_links.is_empty() {
            let mut copied_links = Vec::new();
            let mut coordinated_links = Vec::new();
            for link in follower_links {
                match link {
                    FollowerLink::Copied(l) => copied_links.push(l),
                    FollowerLink::Coordinated(l) => coordinated_links.push(l),
                }
            }
            if !copied_links.is_empty() {
                self.write_copied_trade_links(&copied_links).await?;
            }
            if !coordinated_links.is_empty() {
                self.write_coordinated_activity_links(&coordinated_links)
                    .await?;
            }
        }

        self.detect_and_write_snipes(trades, creator_map).await?;
        self.detect_and_write_whale_links(trades).await?;
        self.detect_and_write_top_trader_links(trades).await?;

        println!(
            "[LinkGraph] [Profile] process_trade_patterns: {} trades in {:?}",
            trades.len(),
            start.elapsed()
        );
        Ok(())
    }

    async fn detect_and_write_snipes(
        &self,
        _trades: &[TradeRow],
        creator_map: HashMap<String, String>,
    ) -> Result<()> {
        let start = Instant::now();
        let cfg = &*LINK_GRAPH_CONFIG;
        let mut links: Vec<SnipedLink> = Vec::new();
        let mut snipers_map: HashMap<String, (String, String)> = HashMap::new();
        // Limit sniper detection to Pump.fun launchpad trades only.
        let pump_trades: Vec<&TradeRow> = _trades
            .iter()
            .filter(|t| t.protocol == PROTOCOL_PUMPFUN_LAUNCHPAD)
            .collect();
        if pump_trades.is_empty() {
            return Ok(());
        }

        let unique_mints: HashSet<String> =
            pump_trades.iter().map(|t| t.base_address.clone()).collect();
        if unique_mints.is_empty() {
            return Ok(());
        }

        // This pre-flight check remains the same
        #[derive(Row, Deserialize, Debug)]
        struct TokenHolderInfo {
            token_address: String,
            unique_holders: u32,
        }

        let holder_check_query = "
            SELECT token_address, argMax(unique_holders, updated_at) AS unique_holders
            FROM token_metrics
            WHERE token_address IN ?
            GROUP BY token_address
        ";
        let mut holder_infos: Vec<TokenHolderInfo> = Vec::new();
        let unique_mints_vec: Vec<_> = unique_mints.iter().cloned().collect();

        for chunk in unique_mints_vec.chunks(cfg.chunk_size_large) {
            let mut chunk_results = self
                .db_client
                .query(holder_check_query)
                .bind(chunk) // Bind the smaller chunk
                .fetch_all()
                .await
                .map_err(|e| anyhow!("DB ERROR in Snipes-HolderCheck chunk: {}", e))?;
            holder_infos.append(&mut chunk_results);
        }

        let token_holder_map: HashMap<String, u32> = holder_infos
            .into_iter()
            .map(|t| (t.token_address, t.unique_holders))
            .collect();

        #[derive(Row, Deserialize, Clone, Debug)]
        struct SniperInfo {
            maker: String,
            first_sig: String,
            first_total: f64,
            first_ts: u32,
        }

        #[derive(Row, Deserialize, Debug)]
        struct TokenCreator {
            creator_address: String,
        }

        // OPTIMIZATION: Parallelize the database queries for each mint.
        let query_futures = unique_mints
            .into_iter()
            .filter(|mint| {
                // Pre-filter mints that are too established
                let holder_count = token_holder_map.get(mint).cloned().unwrap_or(0);
                holder_count <= cfg.sniper_rank_threshold as u32
            })
            .map(|mint| {
                let db_client = self.db_client.clone();
                let creator_map_clone = creator_map.clone();
                // Create an async block (a future) for each query
                async move {
                    let snipers_query = "
                        SELECT maker,
                               argMin(signature, timestamp) as first_sig,
                               argMin(total, timestamp) as first_total,
                               min(toUInt32(timestamp)) as first_ts
                        FROM trades WHERE base_address = ? AND trade_type = 0
                        GROUP BY maker ORDER BY min(timestamp) ASC LIMIT ?
                    ";

                    let result = db_client
                        .query(snipers_query)
                        .bind(mint.clone()) // Keep this bind
                        .bind(cfg.sniper_rank_threshold) // And this one
                        .fetch_all::<SniperInfo>()
                        .await
                        .map_err(|e| {
                            anyhow!(
                                "[SNIPER_FAIL]: Sniper fetch for mint '{}' failed. Error: {}",
                                mint,
                                e
                            )
                        });

                    (mint, result)
                }
            });

        // Execute the futures concurrently with a limit of 20 at a time.
        let results = stream::iter(query_futures)
            .buffer_unordered(20) // CONCURRENCY LIMIT
            .collect::<Vec<_>>()
            .await;

        // Process the results after they have all completed
        for (mint, result) in results {
            match result {
                Ok(sniper_candidates) => {
                    for (i, sniper) in sniper_candidates.iter().enumerate() {
                        links.push(SnipedLink {
                            timestamp: sniper.first_ts as i64,
                            signature: sniper.first_sig.clone(),
                            rank: (i + 1) as i64,
                            sniped_amount: sniper.first_total,
                        });
                        snipers_map.insert(
                            sniper.first_sig.clone(),
                            (sniper.maker.clone(), mint.clone()),
                        );
                    }
                }
                Err(e) => eprintln!("[Snipers] Error processing mint {}: {}", mint, e),
            }
        }

        if !links.is_empty() {
            self.write_sniped_links(&links, &snipers_map).await?;
        }
        println!(
            "[LinkGraph] [Profile] detect_and_write_snipes: {} links in {:?}",
            links.len(),
            start.elapsed()
        );
        Ok(())
    }

    fn detect_bundle_trades(
        &self,
        trades: &[TradeRow],
        processed_pairs: &mut HashSet<(String, String)>,
    ) -> Vec<BundleTradeLink> {
        let mut links = Vec::new();
        let trades_by_slot_mint = trades
            .iter()
            .into_group_map_by(|t| (t.slot, t.base_address.clone()));

        for ((slot, mint), trades_in_bundle) in trades_by_slot_mint {
            let unique_makers: Vec<_> =
                trades_in_bundle.iter().map(|t| &t.maker).unique().collect();
            if unique_makers.len() <= 1 {
                continue;
            }

            // Leader Election: Find the trade with the max `quote_amount`.
            // Includes a deterministic tie-breaker using the wallet address.
            let leader_trade = match trades_in_bundle.iter().max_by(|a, b| {
                match a.quote_amount.cmp(&b.quote_amount) {
                    std::cmp::Ordering::Equal => b.maker.cmp(&a.maker),
                    other => other,
                }
            }) {
                Some(trade) => trade,
                None => continue,
            };
            let leader_wallet = &leader_trade.maker;

            let all_bundle_signatures: Vec<String> = trades_in_bundle
                .iter()
                .map(|t| t.signature.clone())
                .collect();

            for follower_trade in trades_in_bundle
                .iter()
                .filter(|t| &t.maker != leader_wallet)
            {
                let follower_wallet = &follower_trade.maker;

                let mut combo_sorted = vec![leader_wallet.clone(), follower_wallet.clone()];
                combo_sorted.sort();
                let pair_key = (combo_sorted[0].clone(), combo_sorted[1].clone());

                // Populate the processed_pairs set and create the link.
                if processed_pairs.insert(pair_key) {
                    links.push(BundleTradeLink {
                        signatures: all_bundle_signatures.clone(),
                        wallet_a: leader_wallet.clone(),
                        wallet_b: follower_wallet.clone(),
                        mint: mint.clone(),
                        slot: slot as i64,
                        timestamp: leader_trade.timestamp as i64,
                    });
                }
            }
        }
        links
    }

    async fn detect_follower_activity(
        &self,
        trades: &[TradeRow],
        processed_pairs: &mut HashSet<(String, String)>,
    ) -> Result<Vec<FollowerLink>> {
        let cfg = &*LINK_GRAPH_CONFIG;
        let mut links = Vec::new();
        let min_usd_value = cfg.min_trade_total_usd;

        let significant_trades: Vec<&TradeRow> = trades
            .iter()
            .filter(|t| t.total_usd >= min_usd_value)
            .collect();

        if significant_trades.len() < 2 {
            return Ok(links);
        }

        let unique_pairs: Vec<(String, String)> = significant_trades
            .iter()
            .map(|t| (t.maker.clone(), t.base_address.clone()))
            .unique()
            .collect();

        #[derive(Row, Deserialize, Clone)]
        struct FullHistTrade {
            maker: String,
            base_address: String,
            timestamp: u32,
            signature: String,
            trade_type: u8,
            total_usd: f64,
            slippage: f32,
        }

        let mut historical_trades_map: HashMap<(String, String), Vec<FullHistTrade>> =
            HashMap::new();

        if !unique_pairs.is_empty() {
            let historical_query = "
                SELECT maker, base_address, toUnixTimestamp(timestamp) as timestamp, signature, trade_type, total_usd, slippage
                FROM trades
                WHERE (maker, base_address) IN ?
            ";
            for chunk in unique_pairs.chunks(cfg.chunk_size_historical) {
                let chunk_results: Vec<FullHistTrade> = self
                    .db_client
                    .query(historical_query)
                    .bind(chunk)
                    .fetch_all()
                    .await
                    .map_err(|e| {
                        anyhow!(
                            "[FOLLOWER_FAIL]: Historical trade fetch failed. Error: {}",
                            e
                        )
                    })?;

                for trade in chunk_results {
                    historical_trades_map
                        .entry((trade.maker.clone(), trade.base_address.clone()))
                        .or_default()
                        .push(trade);
                }
            }
        }

        let trades_by_mint = significant_trades
            .into_iter()
            .into_group_map_by(|t| t.base_address.clone());

        for (mint, trades_in_batch) in trades_by_mint {
            if trades_in_batch.len() < 2 {
                continue;
            }

            let Some(leader_trade) = trades_in_batch.iter().min_by_key(|t| t.timestamp) else {
                continue;
            };
            let leader_wallet = &leader_trade.maker;

            for follower_trade in trades_in_batch.iter().filter(|t| &t.maker != leader_wallet) {
                let follower_wallet = &follower_trade.maker;

                let mut pair_key_vec = vec![leader_wallet.to_string(), follower_wallet.to_string()];
                pair_key_vec.sort();
                let pair_key = (pair_key_vec[0].clone(), pair_key_vec[1].clone());
                if processed_pairs.contains(&pair_key) {
                    continue;
                }

                if let (Some(leader_hist_ref), Some(follower_hist_ref)) = (
                    historical_trades_map.get(&(leader_wallet.clone(), mint.clone())),
                    historical_trades_map.get(&(follower_wallet.clone(), mint.clone())),
                ) {
                    let mut leader_hist = leader_hist_ref.clone();
                    let mut follower_hist = follower_hist_ref.clone();
                    leader_hist.sort_by_key(|t| t.timestamp);
                    follower_hist.sort_by_key(|t| t.timestamp);

                    let leader_first_trade = leader_hist.get(0);
                    let follower_first_trade = follower_hist.get(0);

                    // --- THE CRITICAL FIX ---
                    // Base the decision on the very first interaction.
                    if let (Some(l1), Some(f1)) = (leader_first_trade, follower_first_trade) {
                        let first_gap = (f1.timestamp as i64 - l1.timestamp as i64).abs();

                        if first_gap > 0 && first_gap <= cfg.copied_trade_window_seconds {
                            processed_pairs.insert(pair_key); // Process this pair only once

                            // A) If the FIRST trades are BOTH BUYS, it's a COPIED_TRADE.
                            if l1.trade_type == 0 && f1.trade_type == 0 {
                                let l_buy = l1; // Already have the first buy
                                let f_buy = f1; // Already have the first buy

                                let leader_sells: Vec<_> =
                                    leader_hist.iter().filter(|t| t.trade_type == 1).collect();
                                let follower_sells: Vec<_> =
                                    follower_hist.iter().filter(|t| t.trade_type == 1).collect();
                                let leader_sell_total: f64 =
                                    leader_sells.iter().map(|t| t.total_usd).sum();
                                let follower_sell_total: f64 =
                                    follower_sells.iter().map(|t| t.total_usd).sum();
                                let leader_pnl = if l_buy.total_usd > 0.0 {
                                    (leader_sell_total - l_buy.total_usd) / l_buy.total_usd
                                } else {
                                    0.0
                                };
                                let follower_pnl = if f_buy.total_usd > 0.0 {
                                    (follower_sell_total - f_buy.total_usd) / f_buy.total_usd
                                } else {
                                    0.0
                                };
                                let leader_first_sell =
                                    leader_sells.iter().min_by_key(|t| t.timestamp);
                                let follower_first_sell =
                                    follower_sells.iter().min_by_key(|t| t.timestamp);

                                let (sell_gap, l_sell_sig, f_sell_sig, f_sell_slip) =
                                    if let (Some(l_sell), Some(f_sell)) =
                                        (leader_first_sell, follower_first_sell)
                                    {
                                        (
                                            (f_sell.timestamp as i64 - l_sell.timestamp as i64)
                                                .abs(),
                                            l_sell.signature.clone(),
                                            f_sell.signature.clone(),
                                            f_sell.slippage,
                                        )
                                    } else {
                                        (0, "".to_string(), "".to_string(), 0.0)
                                    };

                                links.push(FollowerLink::Copied(CopiedTradeLink {
                                    timestamp: f_buy.timestamp as i64,
                                    follower: follower_wallet.clone(),
                                    leader: leader_wallet.clone(),
                                    mint: mint.clone(),
                                    time_gap_on_buy_sec: first_gap, // Use the already calculated gap
                                    time_gap_on_sell_sec: sell_gap,
                                    leader_pnl,
                                    follower_pnl,
                                    leader_buy_sig: l_buy.signature.clone(),
                                    leader_sell_sig: l_sell_sig,
                                    follower_buy_sig: f_buy.signature.clone(),
                                    follower_sell_sig: f_sell_sig,
                                    leader_buy_total: l_buy.total_usd,
                                    leader_sell_total,
                                    follower_buy_total: f_buy.total_usd,
                                    follower_sell_total,
                                    follower_buy_slippage: f_buy.slippage,
                                    follower_sell_slippage: f_sell_slip,
                                }));
                            }
                            // B) ELSE, if the first trades are not both buys, it's a COORDINATED_ACTIVITY.
                            else {
                                let leader_second_trade = leader_hist.get(1);
                                let follower_second_trade = follower_hist.get(1);

                                let (l2_sig, f2_sig, second_gap) = if let (Some(l2), Some(f2)) =
                                    (leader_second_trade, follower_second_trade)
                                {
                                    (
                                        l2.signature.clone(),
                                        f2.signature.clone(),
                                        (f2.timestamp as i64 - l2.timestamp as i64).abs(),
                                    )
                                } else {
                                    ("".to_string(), "".to_string(), 0)
                                };

                                links.push(FollowerLink::Coordinated(CoordinatedActivityLink {
                                    timestamp: l1.timestamp as i64,
                                    leader: leader_wallet.clone(),
                                    follower: follower_wallet.clone(),
                                    mint: mint.clone(),
                                    leader_first_sig: l1.signature.clone(),
                                    follower_first_sig: f1.signature.clone(),
                                    time_gap_on_first_sec: first_gap,
                                    leader_second_sig: l2_sig,
                                    follower_second_sig: f2_sig,
                                    time_gap_on_second_sec: second_gap,
                                }));
                            }
                        }
                    }
                }
            }
        }
        Ok(links)
    }

    async fn detect_and_write_top_trader_links(&self, trades: &[TradeRow]) -> Result<()> {
        let start = Instant::now();
        let cfg = &*LINK_GRAPH_CONFIG;
        let active_trader_pairs: Vec<(String, String)> = trades
            .iter()
            .map(|t| (t.maker.clone(), t.base_address.clone()))
            .unique()
            .collect();

        if active_trader_pairs.is_empty() {
            return Ok(());
        }

        // --- NEW: CONFIDENCE FILTER ---
        // 1. Get all unique mints from the active pairs.
        let unique_mints: Vec<String> = active_trader_pairs
            .iter()
            .map(|(_, mint)| mint.clone())
            .unique()
            .collect();

        #[derive(Row, Deserialize, Debug)]
        struct MintCheck {
            mint_address: String,
        }
        let mint_query = "SELECT DISTINCT mint_address FROM mints WHERE mint_address IN ?";

        let mut fully_tracked_mints = HashSet::new();
        let mint_chunk_small = cfg.chunk_size_mint_small;

        for chunk in unique_mints.chunks(mint_chunk_small) {
            let mut cursor = self
                .db_client
                .query(mint_query)
                .bind(chunk)
                .fetch::<MintCheck>()?;
            while let Some(mint_row) = cursor.next().await? {
                fully_tracked_mints.insert(mint_row.mint_address);
            }
        }

        // 2. Filter the active pairs to only include those for fully tracked tokens.
        let confident_trader_pairs: Vec<(String, String)> = active_trader_pairs
            .into_iter()
            .filter(|(_, mint)| fully_tracked_mints.contains(mint))
            .collect();

        if confident_trader_pairs.is_empty() {
            return Ok(());
        }
        // --- END CONFIDENCE FILTER ---

        let mints_to_query: Vec<String> = fully_tracked_mints.iter().cloned().collect();
        if mints_to_query.is_empty() {
            return Ok(());
        }

        let ath_map = self.fetch_latest_ath_map(&mints_to_query).await?;
        if ath_map.is_empty() {
            return Ok(());
        }

        #[derive(Row, Deserialize, Debug)]
        struct TraderContextInfo {
            wallet_address: String,
            mint_address: String,
            realized_profit_pnl: f32,
        }

        let pnl_query = "
            SELECT 
                wh.wallet_address, wh.mint_address, wh.realized_profit_pnl
            FROM wallet_holdings AS wh
            WHERE wh.mint_address IN ? 
              AND wh.realized_profit_pnl > ? 
            QUALIFY ROW_NUMBER() OVER (PARTITION BY wh.mint_address ORDER BY wh.realized_profit_pnl DESC) = 1
        ";

        let mut top_traders: Vec<TraderContextInfo> = Vec::new();

        for chunk in mints_to_query.chunks(cfg.chunk_size_mint_large) {
            let chunk_results = self
                .db_client
                .query(pnl_query)
                .bind(chunk)
                .bind(cfg.min_top_trader_pnl)
                .fetch_all()
                .await
                .map_err(|e| anyhow!("[TOPTRADER_FAIL]: Top-1 PNL fetch failed. Error: {}", e))?;
            top_traders.extend(chunk_results);
        }

        let links: Vec<TopTraderOfLink> = top_traders
            .into_iter()
            .filter_map(|trader| {
                ath_map
                    .get(&trader.mint_address)
                    .filter(|ath| **ath >= cfg.ath_price_threshold_usd)
                    .map(|ath| TopTraderOfLink {
                        timestamp: Utc::now().timestamp(),
                        wallet: trader.wallet_address,
                        token: trader.mint_address,
                        pnl_at_creation: trader.realized_profit_pnl as f64,
                        ath_usd_at_creation: *ath,
                    })
            })
            .collect();

        if !links.is_empty() {
            self.write_top_trader_of_links(&links).await?;
        }

        println!(
            "[LinkGraph] [Profile] detect_and_write_top_trader_links: {} links in {:?}",
            links.len(),
            start.elapsed()
        );
        Ok(())
    }

    async fn process_liquidity_events(&self, liquidity_adds: &[LiquidityRow]) -> Result<()> {
        let cfg = &*LINK_GRAPH_CONFIG;
        if liquidity_adds.is_empty() {
            return Ok(());
        }
        let unique_pools: HashSet<String> = liquidity_adds
            .iter()
            .map(|l| l.pool_address.clone())
            .collect();
        if unique_pools.is_empty() {
            return Ok(());
        }

        #[derive(Row, Deserialize, Debug)]
        struct PoolInfo {
            pool_address: String,
            base_address: String,
            base_decimals: Option<u8>,
            quote_decimals: Option<u8>,
        }

        let pool_query = "SELECT pool_address, base_address, base_decimals, quote_decimals FROM pool_creations WHERE pool_address IN ?";
        let mut pools_info: Vec<PoolInfo> = Vec::new();
        let unique_pools_vec: Vec<_> = unique_pools.iter().cloned().collect();

        for chunk in unique_pools_vec.chunks(cfg.chunk_size_large) {
            let mut chunk_results = self
                .db_client
                .query(pool_query)
                .bind(chunk)
                .fetch_all()
                .await
                .map_err(|e| anyhow!("[LIQUIDITY_FAIL]: PoolQuery chunk failed. Error: {}", e))?;
            pools_info.append(&mut chunk_results);
        }

        let pool_to_token_map: HashMap<String, (String, Option<u8>, Option<u8>)> = pools_info
            .into_iter()
            .map(|p| {
                (
                    p.pool_address,
                    (p.base_address, p.base_decimals, p.quote_decimals),
                )
            })
            .collect();

        let links: Vec<_> = liquidity_adds
            .iter()
            .filter_map(|l| {
                pool_to_token_map.get(&l.pool_address).map(
                    |(token_address, base_decimals, quote_decimals)| {
                        let base_scale = 10f64.powi(base_decimals.unwrap_or(0) as i32);
                        let quote_scale = 10f64.powi(quote_decimals.unwrap_or(0) as i32);
                        ProvidedLiquidityLink {
                            signature: l.signature.clone(),
                            wallet: l.lp_provider.clone(),
                            token: token_address.clone(),
                            pool_address: l.pool_address.clone(),
                            amount_base: l.base_amount as f64 / base_scale,
                            amount_quote: l.quote_amount as f64 / quote_scale,
                            timestamp: l.timestamp as i64,
                        }
                    },
                )
            })
            .collect();

        if !links.is_empty() {
            self.write_provided_liquidity_links(&links).await?;
        }
        Ok(())
    }

    async fn detect_and_write_whale_links(&self, trades: &[TradeRow]) -> Result<()> {
        let start = Instant::now();
        let cfg = &*LINK_GRAPH_CONFIG;
        let unique_mints_in_batch: Vec<String> = trades
            .iter()
            .map(|t| t.base_address.clone())
            .unique()
            .collect();
        if unique_mints_in_batch.is_empty() {
            return Ok(());
        }

        // --- NEW: CONFIDENCE FILTER ---
        // 1. Check which of the mints in the batch have a creation event in our DB.
        #[derive(Row, Deserialize, Debug)]
        struct MintCheck {
            mint_address: String,
        }
        let mint_query = "SELECT DISTINCT mint_address FROM mints WHERE mint_address IN ?";

        let mut fully_tracked_mints = HashSet::new();
        for chunk in unique_mints_in_batch.chunks(cfg.chunk_size_mint_large) {
            let mut cursor = self
                .db_client
                .query(mint_query)
                .bind(chunk)
                .fetch::<MintCheck>()?;
            while let Some(mint_row) = cursor.next().await? {
                fully_tracked_mints.insert(mint_row.mint_address);
            }
        }

        if fully_tracked_mints.is_empty() {
            return Ok(());
        }
        let confident_mints: Vec<String> = fully_tracked_mints.iter().cloned().collect();
        let ath_map = self.fetch_latest_ath_map(&confident_mints).await?;
        if ath_map.is_empty() {
            return Ok(());
        }
        // --- END CONFIDENCE FILTER ---

        #[derive(Row, Deserialize, Debug)]
        struct TokenInfo {
            token_address: String,
            total_supply: u64,
            decimals: u8,
        }

        let token_query = "SELECT token_address, total_supply, decimals FROM tokens FINAL WHERE token_address IN ?";

        // --- RE-INTRODUCED CHUNKING for the token pre-filter ---
        const TOKEN_CHUNK_SIZE: usize = 3000;
        let mut context_map: HashMap<String, (u64, f64, u8)> = HashMap::new();

        for chunk in confident_mints.chunks(cfg.chunk_size_token) {
            let chunk_results: Vec<TokenInfo> = self
                .db_client
                .query(token_query)
                .bind(chunk)
                .fetch_all()
                .await
                .map_err(|e| {
                    anyhow!(
                        "[WHALE_FAIL]: Token pre-filter query chunk failed. Error: {}",
                        e
                    )
                })?;
            for token in chunk_results {
                if let Some(ath) = ath_map.get(&token.token_address) {
                    if *ath >= cfg.ath_price_threshold_usd {
                        context_map.insert(
                            token.token_address,
                            (token.total_supply, *ath, token.decimals),
                        );
                    }
                }
            }
        }
        // --- END CHUNKING ---

        if context_map.is_empty() {
            return Ok(());
        }

        let tokens_to_query: Vec<String> = context_map.keys().cloned().collect();

        #[derive(Row, Deserialize, Debug)]
        struct WhaleInfo {
            wallet_address: String,
            mint_address: String,
            current_balance: f64,
        }

        let whales_query = "
            SELECT wallet_address, mint_address, current_balance 
            FROM wallet_holdings
            WHERE mint_address IN ? AND current_balance > 0
            QUALIFY ROW_NUMBER() OVER (PARTITION BY mint_address ORDER BY current_balance DESC) <= ?
        ";

        // --- RE-INTRODUCED CHUNKING for the main whale query ---
        let mut top_holders: Vec<WhaleInfo> = Vec::new();
        for chunk in tokens_to_query.chunks(cfg.chunk_size_token) {
            let chunk_results = self
                .db_client
                .query(whales_query)
                .bind(chunk)
                .bind(cfg.whale_rank_threshold)
                .fetch_all()
                .await
                .map_err(|e| {
                    anyhow!(
                        "[WHALE_FAIL]: Batched holder query chunk failed. Error: {}",
                        e
                    )
                })?;
            top_holders.extend(chunk_results);
        }
        // --- END CHUNKING ---

        let mut links = Vec::new();
        for holder in top_holders {
            if let Some((raw_total_supply, ath_usd, decimals)) =
                context_map.get(&holder.mint_address)
            {
                if *raw_total_supply == 0 {
                    continue;
                }

                // --- THE FIX ---
                // Adjust the total supply to be human-readable before dividing.
                let human_total_supply = *raw_total_supply as f64 / 10f64.powi(*decimals as i32);
                if human_total_supply == 0.0 {
                    continue;
                }
                // --- END FIX ---

                let holding_pct = (holder.current_balance / human_total_supply) as f32;

                links.push(WhaleOfLink {
                    timestamp: Utc::now().timestamp(),
                    wallet: holder.wallet_address.clone(),
                    token: holder.mint_address.clone(),
                    holding_pct_at_creation: holding_pct,
                    ath_usd_at_creation: *ath_usd,
                });
            }
        }

        if !links.is_empty() {
            self.write_whale_of_links(&links).await?;
        }

        println!(
            "[LinkGraph] [Profile] detect_and_write_whale_links: {} links in {:?}",
            links.len(),
            start.elapsed()
        );
        Ok(())
    }

    async fn create_wallet_nodes(&self, wallets: &HashSet<String>) -> Result<()> {
        if wallets.is_empty() {
            return Ok(());
        }
        let cfg = &*LINK_GRAPH_CONFIG;

        // Convert the HashSet to a Vec to be able to create chunks
        let wallet_vec: Vec<_> = wallets.iter().cloned().collect();

        // Process the wallets in smaller, manageable chunks
        for chunk in wallet_vec.chunks(cfg.chunk_size_large) {
            let params: Vec<_> = chunk
                .iter()
                .map(|addr| HashMap::from([("address", BoltType::from(addr.clone()))]))
                .collect();

            let q = query("UNWIND $wallets as wallet MERGE (w:Wallet {address: wallet.address})")
                .param("wallets", params);

            let _guard = NEO4J_WRITE_LOCK.lock().await;
            self.neo4j_client.run(q).await?;
        }
        Ok(())
    }

    async fn create_token_nodes(&self, tokens: &HashSet<String>) -> Result<()> {
        if tokens.is_empty() {
            return Ok(());
        }
        let cfg = &*LINK_GRAPH_CONFIG;

        // Convert the HashSet to a Vec to be able to create chunks
        let token_vec: Vec<_> = tokens.iter().cloned().collect();

        // Process the tokens in smaller, manageable chunks
        for chunk in token_vec.chunks(cfg.chunk_size_large) {
            let params: Vec<_> = chunk
                .iter()
                .map(|addr| HashMap::from([("address", BoltType::from(addr.clone()))]))
                .collect();

            let q = query("UNWIND $tokens as token MERGE (t:Token {address: token.address}) ON CREATE SET t.created_ts = token.created_ts")
                .param("tokens", params);

            let _guard = NEO4J_WRITE_LOCK.lock().await;
            self.neo4j_client.run(q).await?;
        }
        Ok(())
    }

    async fn write_bundle_trade_links(&self, links: &[BundleTradeLink]) -> Result<()> {
        if links.is_empty() {
            return Ok(());
        }
        let params: Vec<_> = links
            .iter()
            .map(|l| {
                HashMap::from([
                    ("wa", BoltType::from(l.wallet_a.clone())),
                    ("wb", BoltType::from(l.wallet_b.clone())),
                    ("mint", BoltType::from(l.mint.clone())),
                    ("slot", BoltType::from(l.slot)),
                    ("timestamp", BoltType::from(l.timestamp)),
                    ("signatures", BoltType::from(l.signatures.clone())),
                ])
            })
            .collect();
        // Corrected relationship name to BUNDLE_TRADE for consistency
        let q = query("
            UNWIND $x as t 
            MERGE (a:Wallet {address: t.wa})
            MERGE (b:Wallet {address: t.wb})
            MERGE (a)-[r:BUNDLE_TRADE {mint: t.mint, slot: t.slot}]->(b)
            ON CREATE SET r.timestamp = t.timestamp, r.signatures = t.signatures
            ON MATCH SET r.timestamp = CASE WHEN t.timestamp < r.timestamp THEN t.timestamp ELSE r.timestamp END
        ");
        let _guard = NEO4J_WRITE_LOCK.lock().await;
        self.neo4j_client
            .run(q.param("x", params))
            .await
            .map_err(|e| e.into())
    }

    async fn write_transfer_links(&self, links: &[TransferLink]) -> Result<()> {
        if links.is_empty() {
            return Ok(());
        }

        // --- THE FIX ---
        // Use `unique_by` to get the *entire first link object* for each unique path.
        // This preserves the signature and timestamp from the first event we see.
        let unique_links = links
            .iter()
            .unique_by(|l| (&l.source, &l.destination, &l.mint))
            .collect::<Vec<_>>();

        // Now build the parameters with the full data from the unique links.
        let params: Vec<_> = unique_links
            .iter()
            .map(|l| {
                HashMap::from([
                    ("source", BoltType::from(l.source.clone())),
                    ("destination", BoltType::from(l.destination.clone())),
                    ("mint", BoltType::from(l.mint.clone())),
                    ("signature", BoltType::from(l.signature.clone())), // Include the signature
                    ("timestamp", BoltType::from(l.timestamp)), // Include the on-chain timestamp
                    ("amount", BoltType::from(l.amount)),
                ])
            })
            .collect();

        // --- UPDATED CYPHER QUERY ---
        // The query now sets the signature and on-chain timestamp on the link when it's first created.
        let q = query("
            UNWIND $x as t 
            MERGE (s:Wallet {address: t.source})
            MERGE (d:Wallet {address: t.destination})
            MERGE (s)-[r:TRANSFERRED_TO {mint: t.mint}]->(d)
            ON CREATE SET 
                r.signature = t.signature, 
                r.timestamp = t.timestamp,
                r.amount = t.amount
            ON MATCH SET r.timestamp = CASE WHEN t.timestamp < r.timestamp THEN t.timestamp ELSE r.timestamp END
        ");

        let _guard = NEO4J_WRITE_LOCK.lock().await;
        let result = self.neo4j_client.run(q.param("x", params)).await;
        result.map_err(|e| e.into())
    }

    async fn write_coordinated_activity_links(
        &self,
        links: &[CoordinatedActivityLink],
    ) -> Result<()> {
        if links.is_empty() {
            return Ok(());
        }

        let params: Vec<_> = links
            .iter()
            .map(|l| {
                HashMap::from([
                    ("leader", BoltType::from(l.leader.clone())),
                    ("follower", BoltType::from(l.follower.clone())),
                    ("mint", BoltType::from(l.mint.clone())),
                    ("timestamp", BoltType::from(l.timestamp)),
                    // Use the new, correct field names
                    ("l_sig_1", BoltType::from(l.leader_first_sig.clone())),
                    ("l_sig_2", BoltType::from(l.leader_second_sig.clone())),
                    ("f_sig_1", BoltType::from(l.follower_first_sig.clone())),
                    ("f_sig_2", BoltType::from(l.follower_second_sig.clone())),
                    ("gap_1", BoltType::from(l.time_gap_on_first_sec)),
                    ("gap_2", BoltType::from(l.time_gap_on_second_sec)),
                ])
            })
            .collect();

        // This query now creates a single, comprehensive link per pair/mint
        let q = query("
            UNWIND $x as t
            MERGE (l:Wallet {address: t.leader})
            MERGE (f:Wallet {address: t.follower})
            MERGE (f)-[r:COORDINATED_ACTIVITY {mint: t.mint}]->(l)
            ON CREATE SET
                r.timestamp = t.timestamp,
                r.leader_first_sig = t.l_sig_1,
                r.leader_second_sig = t.l_sig_2,
                r.follower_first_sig = t.f_sig_1,
                r.follower_second_sig = t.f_sig_2,
                r.time_gap_on_first_sec = t.gap_1,
                r.time_gap_on_second_sec = t.gap_2
            ON MATCH SET r.timestamp = CASE WHEN t.timestamp < r.timestamp THEN t.timestamp ELSE r.timestamp END
        ");

        let _guard = NEO4J_WRITE_LOCK.lock().await;
        self.neo4j_client
            .run(q.param("x", params))
            .await
            .map_err(|e| e.into())
    }

    async fn write_copied_trade_links(&self, links: &[CopiedTradeLink]) -> Result<()> {
        if links.is_empty() {
            return Ok(());
        }
        // This uses the latest struct definition provided in the prompt.
        let params: Vec<_> = links
            .iter()
            .map(|l| {
                HashMap::from([
                    ("follower", BoltType::from(l.follower.clone())),
                    ("leader", BoltType::from(l.leader.clone())),
                    ("mint", BoltType::from(l.mint.clone())),
                    ("buy_gap", BoltType::from(l.time_gap_on_buy_sec)),
                    ("sell_gap", BoltType::from(l.time_gap_on_sell_sec)),
                    ("leader_pnl", BoltType::from(l.leader_pnl)),
                    ("follower_pnl", BoltType::from(l.follower_pnl)),
                    ("l_buy_sig", BoltType::from(l.leader_buy_sig.clone())),
                    ("l_sell_sig", BoltType::from(l.leader_sell_sig.clone())),
                    ("f_buy_sig", BoltType::from(l.follower_buy_sig.clone())),
                    ("f_sell_sig", BoltType::from(l.follower_sell_sig.clone())),
                    ("l_buy_total", BoltType::from(l.leader_buy_total)),
                    ("l_sell_total", BoltType::from(l.leader_sell_total)),
                    ("f_buy_total", BoltType::from(l.follower_buy_total)),
                    ("f_sell_total", BoltType::from(l.follower_sell_total)),
                    ("f_buy_slip", BoltType::from(l.follower_buy_slippage)),
                    ("f_sell_slip", BoltType::from(l.follower_sell_slippage)),
                    ("timestamp", BoltType::from(l.timestamp)),
                ])
            })
            .collect();
        let q = query("
            UNWIND $x as t 
            MERGE (f:Wallet {address: t.follower})
            MERGE (l:Wallet {address: t.leader})
            MERGE (f)-[r:COPIED_TRADE {mint: t.mint}]->(l)
            ON CREATE SET
                r.timestamp = t.timestamp,
                r.follower = t.follower,
                r.leader = t.leader,
                r.mint = t.mint,
                r.buy_gap = t.buy_gap,
                r.sell_gap = t.sell_gap,
                r.leader_pnl = t.leader_pnl,
                r.follower_pnl = t.follower_pnl,
                r.l_buy_sig = t.l_buy_sig,
                r.l_sell_sig = t.l_sell_sig,
                r.f_buy_sig = t.f_buy_sig,
                r.f_sell_sig = t.f_sell_sig,
                r.l_buy_total = t.l_buy_total,
                r.l_sell_total = t.l_sell_total,
                r.f_buy_total = t.f_buy_total,
                r.f_sell_total = t.f_sell_total,
                r.f_buy_slip = t.f_buy_slip,
                r.f_sell_slip = t.f_sell_slip
            ON MATCH SET r.timestamp = CASE WHEN t.timestamp < r.timestamp THEN t.timestamp ELSE r.timestamp END
        ");
        let _guard = NEO4J_WRITE_LOCK.lock().await;
        self.neo4j_client
            .run(q.param("x", params))
            .await
            .map_err(|e| e.into())
    }

    async fn write_minted_links(&self, links: &[MintedLink], mints: &[MintRow]) -> Result<()> {
        if links.is_empty() {
            return Ok(());
        }
        let mint_map: HashMap<_, _> = mints.iter().map(|m| (m.signature.clone(), m)).collect();

        let params: Vec<_> = links
            .iter()
            .filter_map(|l| {
                mint_map.get(&l.signature).map(|m| {
                    HashMap::from([
                        ("creator", BoltType::from(m.creator_address.clone())),
                        ("token", BoltType::from(m.mint_address.clone())),
                        ("signature", BoltType::from(l.signature.clone())),
                        ("timestamp", BoltType::from(l.timestamp)),
                        ("buy_amount", BoltType::from(l.buy_amount)),
                    ])
                })
            })
            .collect();

        if params.is_empty() {
            return Ok(());
        }
        // --- MODIFIED: MERGE on the signature for idempotency ---
        let q = query("
            UNWIND $x as t 
            MERGE (c:Wallet {address: t.creator})
            MERGE (k:Token {address: t.token})
            MERGE (c)-[r:MINTED {signature: t.signature}]->(k)
            ON CREATE SET r.timestamp = t.timestamp, r.buy_amount = t.buy_amount
            ON MATCH SET r.timestamp = CASE WHEN t.timestamp < r.timestamp THEN t.timestamp ELSE r.timestamp END
        ");
        let _guard = NEO4J_WRITE_LOCK.lock().await;
        self.neo4j_client
            .run(q.param("x", params))
            .await
            .map_err(|e| e.into())
    }

    async fn write_sniped_links(
        &self,
        links: &[SnipedLink],
        snipers: &HashMap<String, (String, String)>,
    ) -> Result<()> {
        if links.is_empty() {
            return Ok(());
        }

        let params: Vec<_> = links
            .iter()
            .filter_map(|l| {
                snipers.get(&l.signature).map(|(wallet, token)| {
                    HashMap::from([
                        ("wallet", BoltType::from(wallet.clone())),
                        ("token", BoltType::from(token.clone())),
                        ("signature", BoltType::from(l.signature.clone())),
                        ("rank", BoltType::from(l.rank)),
                        ("sniped_amount", BoltType::from(l.sniped_amount)),
                        ("timestamp", BoltType::from(l.timestamp)),
                    ])
                })
            })
            .collect();

        if params.is_empty() {
            return Ok(());
        }

        // --- MODIFIED: MERGE on signature ---
        let q = query("
            UNWIND $x as t
            MERGE (w:Wallet {address: t.wallet})
            MERGE (k:Token {address: t.token})
            MERGE (w)-[r:SNIPED {signature: t.signature}]->(k)
            ON CREATE SET r.rank = t.rank, r.sniped_amount = t.sniped_amount, r.timestamp = t.timestamp
            ON MATCH SET r.timestamp = CASE WHEN t.timestamp < r.timestamp THEN t.timestamp ELSE r.timestamp END
        ");
        let _guard = NEO4J_WRITE_LOCK.lock().await;
        self.neo4j_client
            .run(q.param("x", params))
            .await
            .map_err(|e| e.into())
    }

    async fn write_locked_supply_links(
        &self,
        links: &[LockedSupplyLink],
        locks: &[SupplyLockRow],
    ) -> Result<()> {
        if links.is_empty() {
            return Ok(());
        }
        let lock_map: HashMap<_, _> = locks.iter().map(|l| (l.signature.clone(), l)).collect();

        let params: Vec<_> = links
            .iter()
            .filter_map(|l| {
                lock_map.get(&l.signature).map(|lock_row| {
                    HashMap::from([
                        ("sender", BoltType::from(lock_row.sender.clone())),
                        ("recipient", BoltType::from(lock_row.recipient.clone())),
                        ("mint", BoltType::from(lock_row.mint_address.clone())),
                        ("signature", BoltType::from(l.signature.clone())),
                        ("amount", BoltType::from(l.amount)),
                        ("unlock_ts", BoltType::from(l.unlock_timestamp as i64)),
                        ("timestamp", BoltType::from(l.timestamp)),
                    ])
                })
            })
            .collect();

        if params.is_empty() {
            return Ok(());
        }

        // --- THE CRITICAL FIX ---
        let q = query("
            UNWIND $x as t 
            MERGE (s:Wallet {address: t.sender})
            MERGE (k:Token {address: t.mint}) 
            MERGE (s)-[r:LOCKED_SUPPLY {signature: t.signature}]->(k)
            ON CREATE SET r.amount = t.amount, r.unlock_timestamp = t.unlock_ts, r.recipient = t.recipient, r.timestamp = t.timestamp
            ON MATCH SET r.timestamp = CASE WHEN t.timestamp < r.timestamp THEN t.timestamp ELSE r.timestamp END
        ");
        let _guard = NEO4J_WRITE_LOCK.lock().await;
        self.neo4j_client
            .run(q.param("x", params))
            .await
            .map_err(|e| e.into())
    }

    async fn write_burned_links(&self, links: &[BurnedLink], burns: &[BurnRow]) -> Result<()> {
        if links.is_empty() {
            return Ok(());
        }
        let burn_map: HashMap<_, _> = burns.iter().map(|b| (b.signature.clone(), b)).collect();

        let params: Vec<_> = links
            .iter()
            .filter_map(|l| {
                burn_map.get(&l.signature).map(|burn_row| {
                    HashMap::from([
                        ("wallet", BoltType::from(burn_row.source.clone())),
                        ("token", BoltType::from(burn_row.mint_address.clone())),
                        ("signature", BoltType::from(l.signature.clone())),
                        ("amount", BoltType::from(l.amount)),
                        ("timestamp", BoltType::from(l.timestamp)),
                    ])
                })
            })
            .collect();

        if params.is_empty() {
            return Ok(());
        }
        // --- MODIFIED: MERGE on signature ---
        let q = query("
            UNWIND $x as t 
            MATCH (w:Wallet {address: t.wallet}), (k:Token {address: t.token}) 
            MERGE (w)-[r:BURNED {signature: t.signature}]->(k)
            ON CREATE SET r.amount = t.amount, r.timestamp = t.timestamp
            ON MATCH SET r.timestamp = CASE WHEN t.timestamp < r.timestamp THEN t.timestamp ELSE r.timestamp END
        ");
        let _guard = NEO4J_WRITE_LOCK.lock().await;
        self.neo4j_client
            .run(q.param("x", params))
            .await
            .map_err(|e| e.into())
    }

    async fn write_provided_liquidity_links(&self, links: &[ProvidedLiquidityLink]) -> Result<()> {
        if links.is_empty() {
            return Ok(());
        }
        let params: Vec<_> = links
            .iter()
            .map(|l| {
                HashMap::from([
                    ("wallet", BoltType::from(l.wallet.clone())),
                    ("token", BoltType::from(l.token.clone())),
                    ("signature", BoltType::from(l.signature.clone())),
                    ("pool_address", BoltType::from(l.pool_address.clone())),
                    ("amount_base", BoltType::from(l.amount_base)),
                    ("amount_quote", BoltType::from(l.amount_quote)),
                    ("timestamp", BoltType::from(l.timestamp)),
                ])
            })
            .collect();

        // --- MODIFIED: MERGE on signature ---
        let q = query("
            UNWIND $x as t
            MERGE (w:Wallet {address: t.wallet})
            MERGE (k:Token {address: t.token})
            MERGE (w)-[r:PROVIDED_LIQUIDITY {signature: t.signature}]->(k)
            ON CREATE SET r.pool_address = t.pool_address, r.amount_base = t.amount_base, r.amount_quote = t.amount_quote, r.timestamp = t.timestamp
            ON MATCH SET r.timestamp = CASE WHEN t.timestamp < r.timestamp THEN t.timestamp ELSE r.timestamp END
        ");
        let _guard = NEO4J_WRITE_LOCK.lock().await;
        self.neo4j_client
            .run(q.param("x", params))
            .await
            .map_err(|e| e.into())
    }

    async fn write_top_trader_of_links(&self, links: &[TopTraderOfLink]) -> Result<()> {
        if links.is_empty() {
            return Ok(());
        }
        let params: Vec<_> = links
            .iter()
            .map(|l| {
                HashMap::from([
                    ("wallet", BoltType::from(l.wallet.clone())),
                    ("token", BoltType::from(l.token.clone())),
                    // Add new params
                    ("pnl_at_creation", BoltType::from(l.pnl_at_creation)),
                    ("ath_at_creation", BoltType::from(l.ath_usd_at_creation)),
                    ("timestamp", BoltType::from(l.timestamp)),
                ])
            })
            .collect();

        // --- MODIFIED: The definitive Cypher query ---
        let q = query("
            UNWIND $x as t
            MERGE (w:Wallet {address: t.wallet})
            MERGE (k:Token {address: t.token})
            MERGE (w)-[r:TOP_TRADER_OF]->(k)
            ON CREATE SET
                r.pnl_at_creation = t.pnl_at_creation,
                r.ath_usd_at_creation = t.ath_at_creation,
                r.timestamp = t.timestamp
            ON MATCH SET r.timestamp = CASE WHEN t.timestamp < r.timestamp THEN t.timestamp ELSE r.timestamp END
        ");
        let _guard = NEO4J_WRITE_LOCK.lock().await;
        self.neo4j_client
            .run(q.param("x", params))
            .await
            .map_err(|e| e.into())
    }

    async fn write_whale_of_links(&self, links: &[WhaleOfLink]) -> Result<()> {
        if links.is_empty() {
            return Ok(());
        }
        let params: Vec<_> = links
            .iter()
            .map(|l| {
                HashMap::from([
                    ("wallet", BoltType::from(l.wallet.clone())),
                    ("token", BoltType::from(l.token.clone())),
                    // Add new params
                    ("pct_at_creation", BoltType::from(l.holding_pct_at_creation)),
                    ("ath_at_creation", BoltType::from(l.ath_usd_at_creation)),
                    ("timestamp", BoltType::from(l.timestamp)),
                ])
            })
            .collect();

        // --- MODIFIED: The definitive Cypher query ---
        let q = query("
            UNWIND $x as t
            MERGE (w:Wallet {address: t.wallet})
            MERGE (k:Token {address: t.token})
            MERGE (w)-[r:WHALE_OF]->(k)
            ON CREATE SET
                r.holding_pct_at_creation = t.pct_at_creation,
                r.ath_usd_at_creation = t.ath_at_creation,
                r.timestamp = t.timestamp
            ON MATCH SET r.timestamp = CASE WHEN t.timestamp < r.timestamp THEN t.timestamp ELSE r.timestamp END
        ");
        let _guard = NEO4J_WRITE_LOCK.lock().await;
        self.neo4j_client
            .run(q.param("x", params))
            .await
            .map_err(|e| e.into())
    }

    async fn fetch_latest_ath_map(
        &self,
        token_addresses: &[String],
    ) -> Result<HashMap<String, f64>> {
        let mut ath_map = HashMap::new();
        if token_addresses.is_empty() {
            return Ok(ath_map);
        }
        let cfg = &*LINK_GRAPH_CONFIG;

        #[derive(Row, Deserialize, Debug)]
        struct AthInfo {
            token_address: String,
            ath_price_usd: f64,
        }

        let query = "
            SELECT token_address, argMax(ath_price_usd, updated_at) AS ath_price_usd
            FROM token_metrics
            WHERE token_address IN ?
            GROUP BY token_address
        ";

        for chunk in token_addresses.chunks(cfg.chunk_size_large) {
            let mut chunk_rows: Vec<AthInfo> = self
                .db_client
                .query(query)
                .bind(chunk)
                .fetch_all()
                .await
                .map_err(|e| anyhow!("[LinkGraph] ATH fetch failed: {}", e))?;
            for row in chunk_rows.drain(..) {
                ath_map.insert(row.token_address, row.ath_price_usd);
            }
        }

        Ok(ath_map)
    }

    async fn fetch_pnl(&self, wallet_address: &str, mint_address: &str) -> Result<f64> {
        let q_str = format!(
            "SELECT realized_profit_pnl FROM wallet_holdings WHERE wallet_address = '{}' AND mint_address = '{}'",
            wallet_address, mint_address
        );
        // Fetch the pre-calculated f32 value
        let pnl_f32 = self.db_client.query(&q_str).fetch_one::<f32>().await?;
        // Cast to f64 for the return type
        Ok(pnl_f32 as f64)
    }
}

async fn collect_events_from_stream(
    redis_conn: &MultiplexedConnection,
    stream_key: &str,
    group_name: &str,
    consumer_name: &str,
) -> Result<Vec<(String, EventPayload)>> {
    let cfg = &*LINK_GRAPH_CONFIG;
    let mut conn = redis_conn.clone();
    // Fetch up to 5000 messages at a time.
    // Block for a very short period to avoid busy-looping if the stream is empty, but remain responsive.
    let opts = StreamReadOptions::default()
        .group(group_name, consumer_name)
        .count(cfg.redis_read_count)
        .block(cfg.redis_block_ms as usize); // Block for max N ms

    let reply: StreamReadReply = conn.xread_options(&[stream_key], &[">"], &opts).await?;

    let mut events = Vec::new();
    for stream_entry in reply.keys {
        for message in stream_entry.ids {
            if let Some(payload_value) = message.map.get("payload") {
                if let Ok(payload_bytes) = Vec::<u8>::from_redis_value(payload_value) {
                    if let Ok(payload) = bincode::deserialize::<EventPayload>(&payload_bytes) {
                        events.push((message.id.clone(), payload));
                    }
                }
            }
        }
    }
    Ok(events)
}
