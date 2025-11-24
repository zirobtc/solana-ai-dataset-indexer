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
use serde::Deserialize;
use solana_sdk::native_token::LAMPORTS_PER_SOL;
use std::collections::{HashMap, HashSet, VecDeque};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex, mpsc};
use tokio::time::sleep;
use tokio::time::{Instant, MissedTickBehavior, interval};
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

#[derive(Debug)]
struct LinkGraphConfig {
    time_window_seconds: u32,
    copied_trade_window_seconds: i64,
    sniper_rank_threshold: u64,
    whale_rank_threshold: u64,
    min_top_trader_pnl: f32,
    min_trade_total_usd: f64,
    ath_price_threshold_usd: f64,
    window_max_wait_ms: u64,
    late_slack_ms: u64,
    chunk_size_large: usize,
    chunk_size_historical: usize,
    chunk_size_mint_small: usize,
    chunk_size_mint_large: usize,
    chunk_size_token: usize,
    trade_cache_max_entries: usize,
    trade_cache_ttl_secs: u32,
    trade_cache_max_recent: usize,
    writer_channel_capacity: usize,
}

static LINK_GRAPH_CONFIG: Lazy<LinkGraphConfig> = Lazy::new(|| LinkGraphConfig {
    time_window_seconds: env_parse("LINK_GRAPH_TIME_WINDOW_SECONDS", 120_u32),
    copied_trade_window_seconds: env_parse("LINK_GRAPH_COPIED_TRADE_WINDOW_SECONDS", 60_i64),
    sniper_rank_threshold: env_parse("LINK_GRAPH_SNIPER_RANK_THRESHOLD", 45_u64),
    whale_rank_threshold: env_parse("LINK_GRAPH_WHALE_RANK_THRESHOLD", 5_u64),
    min_top_trader_pnl: env_parse("LINK_GRAPH_MIN_TOP_TRADER_PNL", 1.0_f32),
    min_trade_total_usd: env_parse("LINK_GRAPH_MIN_TRADE_TOTAL_USD", 20.0_f64),
    ath_price_threshold_usd: env_parse("LINK_GRAPH_ATH_PRICE_THRESHOLD_USD", 0.0002000_f64),
    window_max_wait_ms: env_parse("LINK_GRAPH_WINDOW_MAX_WAIT_MS", 250_u64),
    late_slack_ms: env_parse("LINK_GRAPH_LATE_SLACK_MS", 2000_u64),
    chunk_size_large: env_parse("LINK_GRAPH_CHUNK_SIZE_LARGE", 3000_usize),
    chunk_size_historical: env_parse("LINK_GRAPH_CHUNK_SIZE_HISTORICAL", 1000_usize),
    chunk_size_mint_small: env_parse("LINK_GRAPH_CHUNK_SIZE_MINT_SMALL", 1500_usize),
    chunk_size_mint_large: env_parse("LINK_GRAPH_CHUNK_SIZE_MINT_LARGE", 3000_usize),
    chunk_size_token: env_parse("LINK_GRAPH_CHUNK_SIZE_TOKEN", 3000_usize),
    trade_cache_max_entries: env_parse("LINK_GRAPH_TRADE_CACHE_MAX_ENTRIES", 1_000_000_usize),
    trade_cache_ttl_secs: env_parse("LINK_GRAPH_TRADE_CACHE_TTL_SECS", 600_u32),
    trade_cache_max_recent: env_parse("LINK_GRAPH_TRADE_CACHE_MAX_RECENT", 16_usize),
    writer_channel_capacity: env_parse("LINK_GRAPH_WRITER_CHANNEL_CAPACITY", 5000_usize),
});

fn env_parse<T: FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<T>().ok())
        .unwrap_or(default)
}

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

enum FollowerLink {
    Copied(CopiedTradeLink),
    Coordinated(CoordinatedActivityLink),
}

pub struct LinkGraph {
    db_client: Client,
    neo4j_client: Arc<Graph>,
    rx: mpsc::Receiver<EventPayload>,
    link_graph_depth: Arc<AtomicUsize>,
    write_lock: Mutex<()>,
    trade_cache: Arc<Mutex<HashMap<(String, String), CachedPairState>>>,
    write_sender: mpsc::Sender<WriteJob>,
    writer_depth: Arc<AtomicUsize>,
}

// Global Neo4j write lock to serialize batches across workers and avoid deadlocks.
static NEO4J_WRITE_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

#[derive(Row, Deserialize, Debug)]
struct Ping {
    alive: u8,
}
#[derive(Row, Deserialize, Debug)]
struct CountResult {
    count: u64,
}

#[derive(Clone, Debug)]
struct CachedTrade {
    maker: String,
    base_address: String,
    timestamp: u32,
    signature: String,
    trade_type: u8,
    total_usd: f64,
    slippage: f32,
}

#[derive(Debug)]
struct CachedPairState {
    first_buy: Option<CachedTrade>,
    first_sell: Option<CachedTrade>,
    recent: VecDeque<CachedTrade>,
    last_seen: u32,
}

#[derive(Debug)]
pub struct WriteJob {
    query: String,
    params: Vec<HashMap<String, BoltType>>,
}

impl LinkGraph {
    pub async fn new(
        db_client: Client,
        neo4j_client: Arc<Graph>,
        rx: mpsc::Receiver<EventPayload>,
        link_graph_depth: Arc<AtomicUsize>,
        write_sender: mpsc::Sender<WriteJob>,
        writer_depth: Arc<AtomicUsize>,
    ) -> Result<Self> {
        let _: Ping = db_client.query("SELECT 1 as alive").fetch_one().await?;
        neo4j_client.run(query("MATCH (n) RETURN count(n)")).await?;
        println!("[WalletGraph] ✔️ Connected to ClickHouse, Neo4j. Listening on channel.");
        Ok(Self {
            db_client,
            neo4j_client,
            rx,
            link_graph_depth,
            write_lock: Mutex::new(()),
            trade_cache: Arc::new(Mutex::new(HashMap::new())),
            write_sender,
            writer_depth,
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        let cfg = &*LINK_GRAPH_CONFIG;
        let mut message_buffer: Vec<EventPayload> = Vec::new();
        let mut current_window_start: Option<u32> = None;
        let mut window_opened_at: Option<Instant> = None;
        let mut flush_check = interval(Duration::from_millis(cfg.window_max_wait_ms.max(50)));
        flush_check.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let late_slack_secs: u32 = (cfg.late_slack_ms / 1000) as u32;

        loop {
            tokio::select! {
                maybe_payload = self.rx.recv() => {
                    match maybe_payload {
                        Some(payload) => {
                            // one item left the channel
                            self.link_graph_depth.fetch_sub(1, Ordering::Relaxed);
                            if current_window_start.is_none() {
                                current_window_start = Some(payload.timestamp);
                                window_opened_at = Some(Instant::now());
                            }

                            let window_end = current_window_start.unwrap() + cfg.time_window_seconds;
                            if payload.timestamp <= window_end + late_slack_secs {
                                message_buffer.push(payload);
                            } else {
                                if !message_buffer.is_empty() {
                                    message_buffer.sort_by_key(|p| p.timestamp);
                                    let batch = std::mem::take(&mut message_buffer);
                                    if let Err(e) = self.process_batch_with_retry(batch).await {
                                        eprintln!("[LinkGraph] 🔴 Fatal processing window: {}", e);
                                        std::process::exit(1);
                                    }
                                }
                                current_window_start = Some(payload.timestamp);
                                window_opened_at = Some(Instant::now());
                                message_buffer.push(payload);
                            }
                        }
                        None => {
                            eprintln!("[LinkGraph] 🔴 Input channel closed. Exiting.");
                            if !message_buffer.is_empty() {
                                message_buffer.sort_by_key(|p| p.timestamp);
                                let batch = std::mem::take(&mut message_buffer);
                                if let Err(e) = self.process_batch_with_retry(batch).await {
                                    eprintln!("[LinkGraph] 🔴 Fatal processing final window: {}", e);
                                }
                            }
                            // Fatal: the producer is gone. Exit so it's obvious.
                            std::process::exit(1);
                        }
                    }
                }
                _ = flush_check.tick() => {
                    if !message_buffer.is_empty() {
                        if let Some(opened) = window_opened_at {
                            if opened.elapsed() >= Duration::from_millis(cfg.window_max_wait_ms) {
                                message_buffer.sort_by_key(|p| p.timestamp);
                                let batch = std::mem::take(&mut message_buffer);
                                if let Err(e) = self.process_batch_with_retry(batch).await {
                                    eprintln!("[LinkGraph] 🔴 Fatal processing timed window: {}", e);
                                    std::process::exit(1);
                                }
                                current_window_start = None;
                                window_opened_at = None;
                            }
                        }
                    }
                }
            }
        }

        Ok(())
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

        // Process the entire batch as a single logical unit with a per-worker write lock.
        let _guard = self.write_lock.lock().await;
        self.process_time_window(&payloads).await?;

        println!(
            "[LinkGraph] Finished processing batch of {} events.",
            payloads.len()
        );
        Ok(())
    }

    async fn process_batch_with_retry(&self, payloads: Vec<EventPayload>) -> Result<()> {
        // Serialize across all workers to avoid Neo4j deadlocks.
        let _global_lock = NEO4J_WRITE_LOCK.lock().await;
        let mut attempts = 0;
        let max_retries = 3;
        loop {
            match self.process_batch(payloads.clone()).await {
                Ok(_) => return Ok(()),
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
                        return Err(e);
                    }
                }
            }
        }
    }

    // --- Main Logic for Pattern Detection ---
    fn cached_trade_from_trade(trade: &TradeRow) -> CachedTrade {
        CachedTrade {
            maker: trade.maker.clone(),
            base_address: trade.base_address.clone(),
            timestamp: trade.timestamp,
            signature: trade.signature.clone(),
            trade_type: trade.trade_type,
            total_usd: trade.total_usd,
            slippage: trade.slippage,
        }
    }

    async fn update_trade_cache(&self, trades: &[&TradeRow]) -> Result<()> {
        if trades.is_empty() {
            return Ok(());
        }
        let cfg = &*LINK_GRAPH_CONFIG;
        let now_ts = trades.iter().map(|t| t.timestamp).max().unwrap_or(0);
        let cutoff = now_ts.saturating_sub(cfg.trade_cache_ttl_secs);

        let mut cache = self.trade_cache.lock().await;
        cache.retain(|_, state| state.last_seen >= cutoff);

        for trade in trades {
            let key = (trade.maker.clone(), trade.base_address.clone());
            let entry = cache.entry(key).or_insert_with(|| CachedPairState {
                first_buy: None,
                first_sell: None,
                recent: VecDeque::new(),
                last_seen: 0,
            });

            entry.last_seen = entry.last_seen.max(trade.timestamp);

            let ct = Self::cached_trade_from_trade(trade);
            if trade.trade_type == 0 {
                if entry
                    .first_buy
                    .as_ref()
                    .map_or(true, |b| ct.timestamp < b.timestamp)
                {
                    entry.first_buy = Some(ct.clone());
                }
            } else if trade.trade_type == 1 {
                if entry
                    .first_sell
                    .as_ref()
                    .map_or(true, |s| ct.timestamp < s.timestamp)
                {
                    entry.first_sell = Some(ct.clone());
                }
            }

            entry.recent.push_back(ct);
            while entry.recent.len() > cfg.trade_cache_max_recent {
                entry.recent.pop_front();
            }
            while let Some(front) = entry.recent.front() {
                if front.timestamp + cfg.trade_cache_ttl_secs < now_ts {
                    entry.recent.pop_front();
                } else {
                    break;
                }
            }
        }

        if cache.len() > cfg.trade_cache_max_entries {
            let mut entries: Vec<_> = cache
                .iter()
                .map(|(k, v)| (k.clone(), v.last_seen))
                .collect();
            entries.sort_by_key(|(_, ts)| *ts);
            let to_drop = entries.len().saturating_sub(cfg.trade_cache_max_entries);
            for (key, _) in entries.into_iter().take(to_drop) {
                cache.remove(&key);
            }
        }
        Ok(())
    }

    async fn build_histories_from_cache(
        &self,
        pairs: &[(String, String)],
    ) -> Result<HashMap<(String, String), Vec<FullHistTrade>>> {
        let mut map = HashMap::new();
        let cache = self.trade_cache.lock().await;
        for pair in pairs {
            if let Some(state) = cache.get(pair) {
                let mut collected = Vec::new();
                if let Some(b) = &state.first_buy {
                    collected.push(Self::cached_to_full(b));
                }
                if let Some(s) = &state.first_sell {
                    collected.push(Self::cached_to_full(s));
                }
                for t in state.recent.iter() {
                    collected.push(Self::cached_to_full(t));
                }

                if !collected.is_empty() {
                    collected.sort_by_key(|t| t.timestamp);
                    collected.dedup_by(|a, b| a.signature == b.signature);
                    map.insert(pair.clone(), collected);
                }
            }
        }
        Ok(map)
    }

    fn cached_to_full(ct: &CachedTrade) -> FullHistTrade {
        FullHistTrade {
            maker: ct.maker.clone(),
            base_address: ct.base_address.clone(),
            timestamp: ct.timestamp,
            signature: ct.signature.clone(),
            trade_type: ct.trade_type,
            total_usd: ct.total_usd,
            slippage: ct.slippage,
        }
    }

    pub async fn writer_task(
        mut rx: mpsc::Receiver<WriteJob>,
        neo4j_client: Arc<Graph>,
        writer_depth: Arc<AtomicUsize>,
    ) {
        while let Some(job) = rx.recv().await {
            writer_depth.fetch_sub(1, Ordering::Relaxed);
            let q = query(&job.query).param("x", job.params.clone());
            if let Err(e) = neo4j_client.run(q).await {
                eprintln!("[LinkGraph] 🔴 Writer failed to run query: {}", e);
            }
        }
        eprintln!("[LinkGraph] 🔴 Writer channel closed.");
    }

    async fn enqueue_write(
        &self,
        cypher: &str,
        params: Vec<HashMap<String, BoltType>>,
    ) -> Result<()> {
        let job = WriteJob {
            query: cypher.to_string(),
            params,
        };
        self.write_sender
            .send(job)
            .await
            .map_err(|e| anyhow!("[LinkGraph] Failed to enqueue write: {}", e))?;
        self.writer_depth.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

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
        println!(
            "[LinkGraph] [Profile] process_mints: {} mints in {:?}",
            mints.len(),
            start.elapsed()
        );
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
        println!(
            "[LinkGraph] [Profile] process_supply_locks: {} locks in {:?}",
            locks.len(),
            start.elapsed()
        );
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
        println!(
            "[LinkGraph] [Profile] process_burns: {} burns in {:?}",
            burns.len(),
            start.elapsed()
        );
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
        println!(
            "[LinkGraph] [Profile] process_transfers: {} transfers in {:?}",
            transfers.len(),
            start.elapsed()
        );
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
        // Update and read from the bounded in-memory cache; fallback to CH only on misses.
        self.update_trade_cache(&significant_trades).await?;
        let mut historical_trades_map = self.build_histories_from_cache(&unique_pairs).await?;

        let missing_pairs: Vec<(String, String)> = unique_pairs
            .iter()
            .filter(|k| !historical_trades_map.contains_key(*k))
            .cloned()
            .collect();
        if !missing_pairs.is_empty() {
            let historical_query = "
                SELECT maker, base_address, toUnixTimestamp(timestamp) as timestamp, signature, trade_type, total_usd, slippage
                FROM trades
                WHERE (maker, base_address) IN ?
            ";
            for chunk in missing_pairs.chunks(cfg.chunk_size_historical) {
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
                .map(|addr| HashMap::from([("address".to_string(), BoltType::from(addr.clone()))]))
                .collect();

            let cypher = "
                UNWIND $wallets as wallet
                MERGE (w:Wallet {address: wallet.address})
            ";

            self.enqueue_write(cypher, params).await?;
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
                .map(|addr| HashMap::from([("address".to_string(), BoltType::from(addr.clone()))]))
                .collect();

            let cypher = "
                UNWIND $tokens as token
                MERGE (t:Token {address: token.address})
                ON CREATE SET t.created_ts = token.created_ts
            ";

            self.enqueue_write(cypher, params).await?;
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
                    ("wa".to_string(), BoltType::from(l.wallet_a.clone())),
                    ("wb".to_string(), BoltType::from(l.wallet_b.clone())),
                    ("mint".to_string(), BoltType::from(l.mint.clone())),
                    ("slot".to_string(), BoltType::from(l.slot)),
                    ("timestamp".to_string(), BoltType::from(l.timestamp)),
                    (
                        "signatures".to_string(),
                        BoltType::from(l.signatures.clone()),
                    ),
                ])
            })
            .collect();
        // Corrected relationship name to BUNDLE_TRADE for consistency
        let cypher = "
            UNWIND $x as t 
            MERGE (a:Wallet {address: t.wa})
            MERGE (b:Wallet {address: t.wb})
            MERGE (a)-[r:BUNDLE_TRADE {mint: t.mint, slot: t.slot}]->(b)
            ON CREATE SET r.timestamp = t.timestamp, r.signatures = t.signatures
            ON MATCH SET r.timestamp = CASE WHEN t.timestamp < r.timestamp THEN t.timestamp ELSE r.timestamp END
        ";
        self.enqueue_write(cypher, params).await
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
                    ("source".to_string(), BoltType::from(l.source.clone())),
                    (
                        "destination".to_string(),
                        BoltType::from(l.destination.clone()),
                    ),
                    ("mint".to_string(), BoltType::from(l.mint.clone())),
                    ("signature".to_string(), BoltType::from(l.signature.clone())), // Include the signature
                    ("timestamp".to_string(), BoltType::from(l.timestamp)), // Include the on-chain timestamp
                    ("amount".to_string(), BoltType::from(l.amount)),
                ])
            })
            .collect();

        // --- UPDATED CYPHER QUERY ---
        // The query now sets the signature and on-chain timestamp on the link when it's first created.
        let cypher = "
            UNWIND $x as t 
            MERGE (s:Wallet {address: t.source})
            MERGE (d:Wallet {address: t.destination})
            MERGE (s)-[r:TRANSFERRED_TO {mint: t.mint}]->(d)
            ON CREATE SET 
                r.signature = t.signature, 
                r.timestamp = t.timestamp,
                r.amount = t.amount
            ON MATCH SET r.timestamp = CASE WHEN t.timestamp < r.timestamp THEN t.timestamp ELSE r.timestamp END
        ";

        self.enqueue_write(cypher, params).await
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
                    ("leader".to_string(), BoltType::from(l.leader.clone())),
                    ("follower".to_string(), BoltType::from(l.follower.clone())),
                    ("mint".to_string(), BoltType::from(l.mint.clone())),
                    ("timestamp".to_string(), BoltType::from(l.timestamp)),
                    // Use the new, correct field names
                    (
                        "l_sig_1".to_string(),
                        BoltType::from(l.leader_first_sig.clone()),
                    ),
                    (
                        "l_sig_2".to_string(),
                        BoltType::from(l.leader_second_sig.clone()),
                    ),
                    (
                        "f_sig_1".to_string(),
                        BoltType::from(l.follower_first_sig.clone()),
                    ),
                    (
                        "f_sig_2".to_string(),
                        BoltType::from(l.follower_second_sig.clone()),
                    ),
                    ("gap_1".to_string(), BoltType::from(l.time_gap_on_first_sec)),
                    (
                        "gap_2".to_string(),
                        BoltType::from(l.time_gap_on_second_sec),
                    ),
                ])
            })
            .collect();

        // This query now creates a single, comprehensive link per pair/mint
        let cypher = "
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
        ";

        self.enqueue_write(cypher, params).await
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
                    ("follower".to_string(), BoltType::from(l.follower.clone())),
                    ("leader".to_string(), BoltType::from(l.leader.clone())),
                    ("mint".to_string(), BoltType::from(l.mint.clone())),
                    ("buy_gap".to_string(), BoltType::from(l.time_gap_on_buy_sec)),
                    (
                        "sell_gap".to_string(),
                        BoltType::from(l.time_gap_on_sell_sec),
                    ),
                    ("leader_pnl".to_string(), BoltType::from(l.leader_pnl)),
                    ("follower_pnl".to_string(), BoltType::from(l.follower_pnl)),
                    (
                        "l_buy_sig".to_string(),
                        BoltType::from(l.leader_buy_sig.clone()),
                    ),
                    (
                        "l_sell_sig".to_string(),
                        BoltType::from(l.leader_sell_sig.clone()),
                    ),
                    (
                        "f_buy_sig".to_string(),
                        BoltType::from(l.follower_buy_sig.clone()),
                    ),
                    (
                        "f_sell_sig".to_string(),
                        BoltType::from(l.follower_sell_sig.clone()),
                    ),
                    (
                        "l_buy_total".to_string(),
                        BoltType::from(l.leader_buy_total),
                    ),
                    (
                        "l_sell_total".to_string(),
                        BoltType::from(l.leader_sell_total),
                    ),
                    (
                        "f_buy_total".to_string(),
                        BoltType::from(l.follower_buy_total),
                    ),
                    (
                        "f_sell_total".to_string(),
                        BoltType::from(l.follower_sell_total),
                    ),
                    (
                        "f_buy_slip".to_string(),
                        BoltType::from(l.follower_buy_slippage),
                    ),
                    (
                        "f_sell_slip".to_string(),
                        BoltType::from(l.follower_sell_slippage),
                    ),
                    ("timestamp".to_string(), BoltType::from(l.timestamp)),
                ])
            })
            .collect();
        let cypher = "
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
        ";
        self.enqueue_write(cypher, params).await
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
                        (
                            "creator".to_string(),
                            BoltType::from(m.creator_address.clone()),
                        ),
                        ("token".to_string(), BoltType::from(m.mint_address.clone())),
                        ("signature".to_string(), BoltType::from(l.signature.clone())),
                        ("timestamp".to_string(), BoltType::from(l.timestamp)),
                        ("buy_amount".to_string(), BoltType::from(l.buy_amount)),
                    ])
                })
            })
            .collect();

        if params.is_empty() {
            return Ok(());
        }
        // --- MODIFIED: MERGE on the signature for idempotency ---
        let cypher = "
            UNWIND $x as t 
            MERGE (c:Wallet {address: t.creator})
            MERGE (k:Token {address: t.token})
            MERGE (c)-[r:MINTED {signature: t.signature}]->(k)
            ON CREATE SET r.timestamp = t.timestamp, r.buy_amount = t.buy_amount
            ON MATCH SET r.timestamp = CASE WHEN t.timestamp < r.timestamp THEN t.timestamp ELSE r.timestamp END
        ";
        self.enqueue_write(cypher, params).await
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
                        ("wallet".to_string(), BoltType::from(wallet.clone())),
                        ("token".to_string(), BoltType::from(token.clone())),
                        ("signature".to_string(), BoltType::from(l.signature.clone())),
                        ("rank".to_string(), BoltType::from(l.rank)),
                        ("sniped_amount".to_string(), BoltType::from(l.sniped_amount)),
                        ("timestamp".to_string(), BoltType::from(l.timestamp)),
                    ])
                })
            })
            .collect();

        if params.is_empty() {
            return Ok(());
        }

        // --- MODIFIED: MERGE on signature ---
        let cypher = "
            UNWIND $x as t
            MERGE (w:Wallet {address: t.wallet})
            MERGE (k:Token {address: t.token})
            MERGE (w)-[r:SNIPED {signature: t.signature}]->(k)
            ON CREATE SET r.rank = t.rank, r.sniped_amount = t.sniped_amount, r.timestamp = t.timestamp
            ON MATCH SET r.timestamp = CASE WHEN t.timestamp < r.timestamp THEN t.timestamp ELSE r.timestamp END
        ";
        self.enqueue_write(cypher, params).await
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
                        (
                            "sender".to_string(),
                            BoltType::from(lock_row.sender.clone()),
                        ),
                        (
                            "recipient".to_string(),
                            BoltType::from(lock_row.recipient.clone()),
                        ),
                        (
                            "mint".to_string(),
                            BoltType::from(lock_row.mint_address.clone()),
                        ),
                        ("signature".to_string(), BoltType::from(l.signature.clone())),
                        ("amount".to_string(), BoltType::from(l.amount)),
                        (
                            "unlock_ts".to_string(),
                            BoltType::from(l.unlock_timestamp as i64),
                        ),
                        ("timestamp".to_string(), BoltType::from(l.timestamp)),
                    ])
                })
            })
            .collect();

        if params.is_empty() {
            return Ok(());
        }

        // --- THE CRITICAL FIX ---
        let cypher = "
            UNWIND $x as t 
            MERGE (s:Wallet {address: t.sender})
            MERGE (k:Token {address: t.mint}) 
            MERGE (s)-[r:LOCKED_SUPPLY {signature: t.signature}]->(k)
            ON CREATE SET r.amount = t.amount, r.unlock_timestamp = t.unlock_ts, r.recipient = t.recipient, r.timestamp = t.timestamp
            ON MATCH SET r.timestamp = CASE WHEN t.timestamp < r.timestamp THEN t.timestamp ELSE r.timestamp END
        ";
        self.enqueue_write(cypher, params).await
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
                        (
                            "wallet".to_string(),
                            BoltType::from(burn_row.source.clone()),
                        ),
                        (
                            "token".to_string(),
                            BoltType::from(burn_row.mint_address.clone()),
                        ),
                        ("signature".to_string(), BoltType::from(l.signature.clone())),
                        ("amount".to_string(), BoltType::from(l.amount)),
                        ("timestamp".to_string(), BoltType::from(l.timestamp)),
                    ])
                })
            })
            .collect();

        if params.is_empty() {
            return Ok(());
        }
        // --- MODIFIED: MERGE on signature ---
        let cypher = "
            UNWIND $x as t 
            MATCH (w:Wallet {address: t.wallet}), (k:Token {address: t.token}) 
            MERGE (w)-[r:BURNED {signature: t.signature}]->(k)
            ON CREATE SET r.amount = t.amount, r.timestamp = t.timestamp
            ON MATCH SET r.timestamp = CASE WHEN t.timestamp < r.timestamp THEN t.timestamp ELSE r.timestamp END
        ";
        self.enqueue_write(cypher, params).await
    }

    async fn write_provided_liquidity_links(&self, links: &[ProvidedLiquidityLink]) -> Result<()> {
        if links.is_empty() {
            return Ok(());
        }
        let params: Vec<_> = links
            .iter()
            .map(|l| {
                HashMap::from([
                    ("wallet".to_string(), BoltType::from(l.wallet.clone())),
                    ("token".to_string(), BoltType::from(l.token.clone())),
                    ("signature".to_string(), BoltType::from(l.signature.clone())),
                    (
                        "pool_address".to_string(),
                        BoltType::from(l.pool_address.clone()),
                    ),
                    ("amount_base".to_string(), BoltType::from(l.amount_base)),
                    ("amount_quote".to_string(), BoltType::from(l.amount_quote)),
                    ("timestamp".to_string(), BoltType::from(l.timestamp)),
                ])
            })
            .collect();

        // --- MODIFIED: MERGE on signature ---
        let cypher = "
            UNWIND $x as t
            MERGE (w:Wallet {address: t.wallet})
            MERGE (k:Token {address: t.token})
            MERGE (w)-[r:PROVIDED_LIQUIDITY {signature: t.signature}]->(k)
            ON CREATE SET r.pool_address = t.pool_address, r.amount_base = t.amount_base, r.amount_quote = t.amount_quote, r.timestamp = t.timestamp
            ON MATCH SET r.timestamp = CASE WHEN t.timestamp < r.timestamp THEN t.timestamp ELSE r.timestamp END
        ";
        self.enqueue_write(cypher, params).await
    }

    async fn write_top_trader_of_links(&self, links: &[TopTraderOfLink]) -> Result<()> {
        if links.is_empty() {
            return Ok(());
        }
        let params: Vec<_> = links
            .iter()
            .map(|l| {
                HashMap::from([
                    ("wallet".to_string(), BoltType::from(l.wallet.clone())),
                    ("token".to_string(), BoltType::from(l.token.clone())),
                    // Add new params
                    (
                        "pnl_at_creation".to_string(),
                        BoltType::from(l.pnl_at_creation),
                    ),
                    (
                        "ath_at_creation".to_string(),
                        BoltType::from(l.ath_usd_at_creation),
                    ),
                    ("timestamp".to_string(), BoltType::from(l.timestamp)),
                ])
            })
            .collect();

        // --- MODIFIED: The definitive Cypher query ---
        let cypher = "
            UNWIND $x as t
            MERGE (w:Wallet {address: t.wallet})
            MERGE (k:Token {address: t.token})
            MERGE (w)-[r:TOP_TRADER_OF]->(k)
            ON CREATE SET
                r.pnl_at_creation = t.pnl_at_creation,
                r.ath_usd_at_creation = t.ath_at_creation,
                r.timestamp = t.timestamp
            ON MATCH SET r.timestamp = CASE WHEN t.timestamp < r.timestamp THEN t.timestamp ELSE r.timestamp END
        ";
        self.enqueue_write(cypher, params).await
    }

    async fn write_whale_of_links(&self, links: &[WhaleOfLink]) -> Result<()> {
        if links.is_empty() {
            return Ok(());
        }
        let params: Vec<_> = links
            .iter()
            .map(|l| {
                HashMap::from([
                    ("wallet".to_string(), BoltType::from(l.wallet.clone())),
                    ("token".to_string(), BoltType::from(l.token.clone())),
                    // Add new params
                    (
                        "pct_at_creation".to_string(),
                        BoltType::from(l.holding_pct_at_creation),
                    ),
                    (
                        "ath_at_creation".to_string(),
                        BoltType::from(l.ath_usd_at_creation),
                    ),
                    ("timestamp".to_string(), BoltType::from(l.timestamp)),
                ])
            })
            .collect();

        // --- MODIFIED: The definitive Cypher query ---
        let cypher = "
            UNWIND $x as t
            MERGE (w:Wallet {address: t.wallet})
            MERGE (k:Token {address: t.token})
            MERGE (w)-[r:WHALE_OF]->(k)
            ON CREATE SET
                r.holding_pct_at_creation = t.pct_at_creation,
                r.ath_usd_at_creation = t.ath_at_creation,
                r.timestamp = t.timestamp
            ON MATCH SET r.timestamp = CASE WHEN t.timestamp < r.timestamp THEN t.timestamp ELSE r.timestamp END
        ";
        self.enqueue_write(cypher, params).await
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
