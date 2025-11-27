use crate::database::insert_rows;
use crate::services::price_service::PriceService;
use crate::types::{
    EventPayload, EventType, MigrationRow, MintRow, TokenMetricsRow, TokenStaticRow, TradeRow,
};
use anyhow::{Context, Result, anyhow};
use borsh::BorshDeserialize;
use clickhouse::Client;
use futures_util::future;
use mpl_token_metadata::accounts::Metadata;
use once_cell::sync::Lazy;
use redis::aio::MultiplexedConnection;
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, Client as RedisClient, FromRedisValue};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_program::program_pack::Pack;
use solana_sdk::pubkey::Pubkey;
use spl_token::state::Mint;
use std::collections::{HashMap, HashSet};
use std::env;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

type TokenCache = HashMap<String, TokenEntry>;

fn env_parse<T: FromStr>(key: &str, default: T) -> T {
    env::var(key)
        .ok()
        .and_then(|v| v.parse::<T>().ok())
        .unwrap_or(default)
}

static TOKEN_STATS_CHUNK_SIZE: Lazy<usize> =
    Lazy::new(|| env_parse("TOKEN_STATS_CHUNK_SIZE", 1000usize));

#[derive(Debug, Clone)]
struct TokenEntry {
    token: TokenStaticRow,
    metrics: TokenMetricsRow,
}

impl TokenEntry {
    fn new(token: TokenStaticRow, metrics: Option<TokenMetricsRow>) -> Self {
        let metrics = metrics
            .unwrap_or_else(|| TokenMetricsRow::new(token.token_address.clone(), token.updated_at));
        Self { token, metrics }
    }
}

#[derive(Clone, Debug)]
struct TokenContext {
    timestamp: u32,
    protocol: Option<u8>,
    pool_address: Option<String>,
    decimals: Option<u8>,
}

impl TokenContext {
    fn new(
        timestamp: u32,
        protocol: Option<u8>,
        pool_address: Option<String>,
        decimals: Option<u8>,
    ) -> Self {
        Self {
            timestamp,
            protocol,
            pool_address,
            decimals,
        }
    }
}

fn record_token_context(
    contexts: &mut HashMap<String, TokenContext>,
    token_address: &str,
    timestamp: u32,
    protocol: Option<u8>,
    pool_address: Option<String>,
    decimals: Option<u8>,
) {
    if token_address.is_empty() {
        return;
    }

    let mut pool_for_insert = pool_address.clone();
    let entry = contexts
        .entry(token_address.to_string())
        .or_insert_with(|| {
            TokenContext::new(timestamp, protocol, pool_for_insert.take(), decimals)
        });

    if timestamp < entry.timestamp {
        entry.timestamp = timestamp;
    }

    if entry.protocol.is_none() {
        entry.protocol = protocol;
    }

    let should_update_pool = entry
        .pool_address
        .as_ref()
        .map(|p| p.is_empty())
        .unwrap_or(true);
    if should_update_pool {
        if let Some(pool) = pool_address {
            if !pool.is_empty() {
                entry.pool_address = Some(pool);
            }
        }
    }

    if let Some(dec) = decimals {
        entry.decimals = Some(dec);
    }
}

fn pool_addresses_from_context(context: &TokenContext) -> Vec<String> {
    context
        .pool_address
        .as_ref()
        .filter(|addr| !addr.is_empty())
        .map(|addr| vec![addr.clone()])
        .unwrap_or_default()
}

fn event_success(event: &EventType) -> bool {
    match event {
        EventType::Trade(row) => row.success,
        EventType::Mint(row) => row.success,
        EventType::Migration(row) => row.success,
        EventType::FeeCollection(row) => row.success,
        EventType::Liquidity(row) => row.success,
        EventType::PoolCreation(row) => row.success,
        EventType::Transfer(row) => row.success,
        EventType::SupplyLock(row) => row.success,
        EventType::SupplyLockAction(row) => row.success,
        EventType::Burn(row) => row.success,
    }
}

pub struct TokenAggregator {
    db_client: Client,
    redis_conn: MultiplexedConnection,
    rpc_client: Arc<RpcClient>,
    price_service: PriceService,
    backfill_mode: bool,
}

impl TokenAggregator {
    pub async fn new(
        db_client: Client,
        redis_client: RedisClient,
        rpc_client: Arc<RpcClient>,
        price_service: PriceService,
    ) -> Result<Self> {
        let redis_conn = redis_client.get_multiplexed_async_connection().await?;
        println!("[TokenAggregator] ✔️ Connected to ClickHouse, Redis, and Solana RPC.");

        let backfill_mode =
            env::var("BACKFILL_MODE").unwrap_or_else(|_| "false".to_string()) == "true";
        Ok(Self {
            db_client,
            redis_conn,
            rpc_client,
            price_service,
            backfill_mode,
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        let stream_key = "event_queue";
        let group_name = "token_aggregators";
        let consumer_name = format!("consumer-tokens-{}", uuid::Uuid::new_v4());

        let mut publisher_conn = self.redis_conn.clone();
        let next_queue = "wallet_agg_queue";

        let result: redis::RedisResult<()> = self
            .redis_conn
            .xgroup_create_mkstream(stream_key, group_name, "0")
            .await;
        if let Err(e) = result {
            if !e.to_string().contains("BUSYGROUP") {
                return Err(anyhow!(
                    "[TokenAggregator] Failed to create consumer group: {}",
                    e
                ));
            }
            println!(
                "[TokenAggregator] Consumer group '{}' already exists. Resuming.",
                group_name
            );
        } else {
            println!(
                "[TokenAggregator] Created new consumer group '{}'.",
                group_name
            );
        }

        loop {
            let messages = match self
                .collect_events(stream_key, group_name, &consumer_name)
                .await
            {
                Ok(msgs) => msgs,
                Err(e) => {
                    eprintln!(
                        "[TokenAggregator] 🔴 Error reading from Redis: {}. Retrying...",
                        e
                    );
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };
            if messages.is_empty() {
                continue;
            }

            println!(
                "[TokenAggregator] ⚙️ Starting processing for a new batch of {} events...",
                messages.len()
            );
            let message_ids: Vec<String> = messages.iter().map(|(id, _)| id.clone()).collect();
            let payloads: Vec<EventPayload> =
                messages.into_iter().map(|(_, payload)| payload).collect();

            match self.process_batch(payloads.clone()).await {
                // Clone payloads to use them after processing
                Ok(_) => {
                    if !message_ids.is_empty() {
                        // Forward each payload to the next queue in the pipeline
                        for payload in payloads {
                            let payload_data = bincode::serialize(&payload)?;
                            let _: () = publisher_conn
                                .xadd(next_queue, "*", &[("payload", payload_data)])
                                .await?;
                        }
                        println!(
                            "[TokenAggregator] ✅ Finished batch, forwarded {} events to {}.",
                            message_ids.len(),
                            next_queue
                        );

                        // Acknowledge the message from the source queue ('event_queue')
                        let _: () = self
                            .redis_conn
                            .xack(stream_key, group_name, &message_ids)
                            .await?;
                        let _: i64 = self
                            .redis_conn
                            .xdel::<_, _, i64>(stream_key, &message_ids)
                            .await?;
                    }
                }
                Err(e) => {
                    eprintln!(
                        "[TokenAggregator] ❌ Failed to process batch, will not forward or ACK. Error: {}",
                        e
                    );
                }
            }
        }
    }

    async fn process_batch(&self, payloads: Vec<EventPayload>) -> Result<()> {
        let mut token_contexts: HashMap<String, TokenContext> = HashMap::new();
        for payload in &payloads {
            if !event_success(&payload.event) {
                continue;
            }
            let decimals_map = &payload.token_decimals;
            match &payload.event {
                EventType::Trade(t) => {
                    let pool = (!t.pool_address.is_empty()).then(|| t.pool_address.clone());
                    record_token_context(
                        &mut token_contexts,
                        &t.base_address,
                        t.timestamp,
                        Some(t.protocol),
                        pool.clone(),
                        decimals_map.get(&t.base_address).cloned(),
                    );
                    record_token_context(
                        &mut token_contexts,
                        &t.quote_address,
                        t.timestamp,
                        Some(t.protocol),
                        pool,
                        decimals_map.get(&t.quote_address).cloned(),
                    );
                }
                EventType::Mint(m) => {
                    record_token_context(
                        &mut token_contexts,
                        &m.mint_address,
                        m.timestamp,
                        Some(m.protocol),
                        (!m.pool_address.is_empty()).then(|| m.pool_address.clone()),
                        Some(m.token_decimals),
                    );
                }
                EventType::Migration(m) => {
                    record_token_context(
                        &mut token_contexts,
                        &m.mint_address,
                        m.timestamp,
                        Some(m.protocol),
                        (!m.pool_address.is_empty()).then(|| m.pool_address.clone()),
                        decimals_map.get(&m.mint_address).cloned(),
                    );
                }
                EventType::FeeCollection(f) => {
                    let vault = (!f.vault_address.is_empty()).then(|| f.vault_address.clone());
                    record_token_context(
                        &mut token_contexts,
                        &f.token_0_mint_address,
                        f.timestamp,
                        Some(f.protocol),
                        vault.clone(),
                        decimals_map.get(&f.token_0_mint_address).cloned(),
                    );
                    if let Some(token_1) = &f.token_1_mint_address {
                        record_token_context(
                            &mut token_contexts,
                            token_1,
                            f.timestamp,
                            Some(f.protocol),
                            vault.clone(),
                            decimals_map.get(token_1).cloned(),
                        );
                    }
                }
                EventType::PoolCreation(p) => {
                    record_token_context(
                        &mut token_contexts,
                        &p.base_address,
                        p.timestamp,
                        Some(p.protocol),
                        (!p.pool_address.is_empty()).then(|| p.pool_address.clone()),
                        p.base_decimals
                            .or_else(|| decimals_map.get(&p.base_address).cloned()),
                    );
                    record_token_context(
                        &mut token_contexts,
                        &p.quote_address,
                        p.timestamp,
                        Some(p.protocol),
                        (!p.pool_address.is_empty()).then(|| p.pool_address.clone()),
                        p.quote_decimals
                            .or_else(|| decimals_map.get(&p.quote_address).cloned()),
                    );
                }
                EventType::Transfer(t) => {
                    record_token_context(
                        &mut token_contexts,
                        &t.mint_address,
                        t.timestamp,
                        None,
                        None,
                        decimals_map.get(&t.mint_address).cloned(),
                    );
                }
                EventType::SupplyLock(lock) => {
                    record_token_context(
                        &mut token_contexts,
                        &lock.mint_address,
                        lock.timestamp,
                        Some(lock.protocol),
                        None,
                        decimals_map.get(&lock.mint_address).cloned(),
                    );
                }
                EventType::SupplyLockAction(action) => {
                    record_token_context(
                        &mut token_contexts,
                        &action.mint_address,
                        action.timestamp,
                        Some(action.protocol),
                        None,
                        decimals_map.get(&action.mint_address).cloned(),
                    );
                }
                EventType::Burn(burn) => {
                    record_token_context(
                        &mut token_contexts,
                        &burn.mint_address,
                        burn.timestamp,
                        None,
                        None,
                        decimals_map.get(&burn.mint_address).cloned(),
                    );
                }
                EventType::Liquidity(_) => {}
                _ => {}
            }
        }

        if token_contexts.is_empty() {
            println!("[TokenAggregator] -> Batch contains no relevant token events. Skipping.");
            return Ok(());
        }
        println!(
            "[TokenAggregator] -> Batch contains {} unique tokens.",
            token_contexts.len()
        );

        let mut tokens = self
            .fetch_tokens_from_db(&token_contexts.keys().cloned().collect::<Vec<_>>())
            .await?;

        let missing_tokens: Vec<String> = token_contexts
            .keys()
            .filter(|address| !tokens.contains_key(*address))
            .cloned()
            .collect();

        if !missing_tokens.is_empty() {
            println!(
                "[TokenAggregator] -> Found {} new tokens to fetch metadata for.",
                missing_tokens.len()
            );

            if !self.backfill_mode {
                let fetch_futures = missing_tokens
                    .iter()
                    .map(|key| async move { (key.clone(), self.fetch_token_metadata(key).await) });
                let fetched_results = future::join_all(fetch_futures).await;

                for (key, rpc_result) in fetched_results {
                    let context = match token_contexts.get(&key) {
                        Some(ctx) => ctx.clone(),
                        None => continue,
                    };
                    let protocol = context.protocol.unwrap_or(0);
                    let token_row = match rpc_result {
                        Ok((metadata, mint_data)) => {
                            println!(
                                "[TokenAggregator] -> ✅ Successfully fetched metadata for new token {}.",
                                key
                            );

                            let creator = metadata
                                .creators
                                .as_ref()
                                .and_then(|creators| creators.first())
                                .map(|c| c.address.to_string())
                                .unwrap_or_default();

                            TokenStaticRow::new(
                                key.clone(),
                                context.timestamp,
                                metadata.name.trim_end_matches('\0').to_string(),
                                metadata.symbol.trim_end_matches('\0').to_string(),
                                metadata.uri.trim_end_matches('\0').to_string(),
                                mint_data.decimals,
                                creator,
                                pool_addresses_from_context(&context),
                                protocol,
                                mint_data.supply,
                                metadata.is_mutable,
                                Some(metadata.update_authority.to_string()),
                                Option::from(mint_data.mint_authority)
                                    .map(|pk: Pubkey| pk.to_string()),
                                Option::from(mint_data.freeze_authority)
                                    .map(|pk: Pubkey| pk.to_string()),
                            )
                        }
                        Err(e) => {
                            eprintln!(
                                "[TokenAggregator] -> ❌ RPC failed for {}: {}. Creating placeholder.",
                                key, e
                            );
                            TokenStaticRow::new(
                                key.clone(),
                                context.timestamp,
                                String::new(),
                                String::new(),
                                String::new(),
                                context.decimals.unwrap_or(0),
                                String::new(),
                                pool_addresses_from_context(&context),
                                protocol,
                                0,
                                true,
                                None,
                                None,
                                None,
                            )
                        }
                    };
                    tokens.insert(key.clone(), TokenEntry::new(token_row, None));
                }
            } else {
                println!(
                    "[TokenAggregator] -> Creating {} placeholder tokens in backfill mode.",
                    missing_tokens.len()
                );
                for key in missing_tokens {
                    if let Some(context) = token_contexts.get(&key) {
                        let placeholder_row = TokenStaticRow::new(
                            key.clone(),
                            context.timestamp,
                            String::new(),
                            String::new(),
                            String::new(),
                            context.decimals.unwrap_or(0),
                            String::new(),
                            pool_addresses_from_context(context),
                            context.protocol.unwrap_or(0),
                            0,
                            false,
                            None,
                            None,
                            None,
                        );
                        tokens.insert(key.clone(), TokenEntry::new(placeholder_row, None));
                    }
                }
            }
        }

        let trader_pairs_in_batch: Vec<(String, String)> = payloads
            .iter()
            .filter_map(|p| {
                if let EventType::Trade(t) = &p.event {
                    Some((t.base_address.clone(), t.maker.clone()))
                } else {
                    None
                }
            })
            .collect();

        let mut existing_traders = HashSet::new();
        if !trader_pairs_in_batch.is_empty() {
            for chunk in trader_pairs_in_batch.chunks(*TOKEN_STATS_CHUNK_SIZE) {
                let mut cursor = self.db_client
                    .query("SELECT DISTINCT (mint_address, wallet_address) FROM wallet_holdings WHERE (mint_address, wallet_address) IN ?")
                    .bind(chunk)
                    .fetch::<(String, String)>()?;

                while let Some(pair) = cursor.next().await? {
                    existing_traders.insert(pair);
                }
            }
        }

        let mut counted_in_this_batch: HashSet<(String, String)> = HashSet::new();

        for payload in payloads.iter() {
            if !event_success(&payload.event) {
                continue;
            }
            match &payload.event {
                EventType::Mint(mint) => self.process_mint(mint, &mut tokens),
                EventType::Trade(trade) => {
                    self.process_trade(
                        trade,
                        &mut tokens,
                        &existing_traders,
                        &mut counted_in_this_batch,
                    );
                }
                EventType::Migration(migration) => self.process_migration(migration, &mut tokens),
                _ => {}
            }
        }

        self.finalize_and_persist(tokens).await
    }

    fn process_trade(
        &self,
        trade: &TradeRow,
        tokens: &mut TokenCache,
        existing_traders: &HashSet<(String, String)>,
        counted_in_this_batch: &mut HashSet<(String, String)>,
    ) {
        if let Some(entry) = tokens.get_mut(&trade.base_address) {
            entry.token.updated_at = trade.timestamp;
            entry.metrics.updated_at = trade.timestamp;

            // --- START: CORRECT UNIQUE HOLDER LOGIC ---

            let current_pair = (trade.base_address.clone(), trade.maker.clone());

            // We only increment the counter if:
            // 1. The trader is NOT in the set of traders we know about from the database.
            // 2. We have NOT already counted this trader for this token in this batch.
            if !existing_traders.contains(&current_pair) {
                // The .insert() returns true only the first time we see this pair in this batch.
                if counted_in_this_batch.insert(current_pair) {
                    entry.metrics.unique_holders += 1;
                }
            }

            let trade_total_in_usd = trade.total_usd;

            entry.metrics.total_volume_usd += trade_total_in_usd;
            entry.metrics.ath_price_usd = entry.metrics.ath_price_usd.max(trade.price_usd);

            if trade.trade_type == 0 {
                // Buy
                entry.metrics.total_buys += 1;
            } else {
                // Sell
                entry.metrics.total_sells += 1;
            }
        }
    }

    async fn fetch_tokens_from_db(&self, keys: &[String]) -> Result<TokenCache> {
        if keys.is_empty() {
            return Ok(HashMap::new());
        }
        let query_str = "
            SELECT
                *
            FROM tokens_latest
            WHERE token_address IN ?
        ";

        let mut statics = HashMap::new();
        for chunk in keys.chunks(*TOKEN_STATS_CHUNK_SIZE) {
            let mut cursor = self
                .db_client
                .query(query_str)
                .bind(chunk)
                .fetch::<TokenStaticRow>()?;

            while let Ok(Some(token)) = cursor.next().await {
                statics.insert(token.token_address.clone(), token);
            }
        }

        let metrics_map = self.fetch_token_metrics(keys).await?;
        let mut tokens = HashMap::new();

        for (address, token) in statics {
            let metrics = metrics_map.get(&address).cloned();
            tokens.insert(address.clone(), TokenEntry::new(token, metrics));
        }

        Ok(tokens)
    }

    async fn fetch_token_metrics(
        &self,
        keys: &[String],
    ) -> Result<HashMap<String, TokenMetricsRow>> {
        if keys.is_empty() {
            return Ok(HashMap::new());
        }

        let query_str = "
            SELECT
                *
            FROM token_metrics_latest
            WHERE token_address IN ?
            ORDER BY token_address, updated_at DESC
            LIMIT 1 BY token_address
        ";

        let mut metrics = HashMap::new();

        for chunk in keys.chunks(*TOKEN_STATS_CHUNK_SIZE) {
            let mut cursor = self
                .db_client
                .query(query_str)
                .bind(chunk)
                .fetch::<TokenMetricsRow>()?;

            while let Ok(Some(row)) = cursor.next().await {
                metrics.insert(row.token_address.clone(), row);
            }
        }

        Ok(metrics)
    }

    async fn fetch_token_metadata(&self, mint_address_str: &str) -> Result<(Metadata, Mint)> {
        let mint_pubkey = Pubkey::from_str(mint_address_str)?;
        let metadata_pubkey = Metadata::find_pda(&mint_pubkey).0;

        let (mint_account_res, metadata_account_res) = future::join(
            self.rpc_client.get_account(&mint_pubkey),
            self.rpc_client.get_account(&metadata_pubkey),
        )
        .await;

        let mint_account = mint_account_res?;
        let metadata_account = metadata_account_res?;

        let mint_data = Mint::unpack(&mint_account.data)?;
        let metadata = Metadata::deserialize(&mut &metadata_account.data[..])?;

        Ok((metadata, mint_data))
    }

    fn process_mint(&self, mint: &MintRow, tokens: &mut TokenCache) {
        let is_new = !tokens.contains_key(&mint.mint_address);
        let entry = tokens
            .entry(mint.mint_address.clone())
            .or_insert_with(|| TokenEntry::new(TokenStaticRow::new_from_mint(mint), None));
        let token = &mut entry.token;

        if is_new {
            println!(
                "[TokenAggregator] -> Created new token record for {} from MINT event.",
                mint.mint_address
            );
        } else {
            println!(
                "[TokenAggregator] -> Enriched existing token record for {} with MINT event data.",
                mint.mint_address
            );
            token.updated_at = mint.timestamp;
            token.created_at = token.created_at.min(mint.timestamp);
            token.decimals = mint.token_decimals;
            token.launchpad = mint.protocol;
            token.protocol = mint.protocol;
            token.total_supply = mint.total_supply;
            token.is_mutable = mint.is_mutable;
            token.update_authority = mint.update_authority.clone();
            token.mint_authority = mint.mint_authority.clone();
            token.freeze_authority = mint.freeze_authority.clone();
            if token.name.is_empty() {
                token.name = mint.token_name.clone().unwrap_or_default();
            }
            if token.symbol.is_empty() {
                token.symbol = mint.token_symbol.clone().unwrap_or_default();
            }
            if token.token_uri.is_empty() {
                token.token_uri = mint.token_uri.clone().unwrap_or_default();
            }
            if token.creator_address.is_empty() {
                token.creator_address = mint.creator_address.clone();
            }
            if !mint.pool_address.is_empty() && !token.pool_addresses.contains(&mint.pool_address) {
                token.pool_addresses.push(mint.pool_address.clone());
            }
        }
    }

    fn process_migration(&self, migration: &MigrationRow, tokens: &mut TokenCache) {
        if let Some(entry) = tokens.get_mut(&migration.mint_address) {
            let token = &mut entry.token;
            println!(
                "[TokenAggregator] -> Updating protocol for token {} due to migration.",
                migration.mint_address
            );
            token.updated_at = migration.timestamp;
            token.protocol = migration.protocol;
            if !token.pool_addresses.contains(&migration.pool_address) {
                token.pool_addresses.push(migration.pool_address.clone());
            }
        }
    }

    async fn finalize_and_persist(&self, tokens: TokenCache) -> Result<()> {
        if tokens.is_empty() {
            return Ok(());
        }

        let mut updated_tokens = Vec::new();
        let mut metric_rows = Vec::new();

        for entry in tokens.into_values() {
            if Self::metrics_has_activity(&entry.metrics) {
                metric_rows.push(entry.metrics);
            }
            updated_tokens.push(entry.token);
        }

        insert_rows(
            &self.db_client,
            "tokens",
            updated_tokens.clone(),
            "Token Aggregator",
            "tokens",
        )
        .await
        .with_context(|| "Failed to persist token data to ClickHouse")?;

        insert_rows(
            &self.db_client,
            "tokens_latest",
            updated_tokens,
            "Token Aggregator",
            "tokens_latest",
        )
        .await
        .with_context(|| "Failed to persist token snapshot data to ClickHouse")?;

        insert_rows(
            &self.db_client,
            "token_metrics",
            metric_rows.clone(),
            "Token Aggregator",
            "token_metrics",
        )
        .await
        .with_context(|| "Failed to persist token metric history to ClickHouse")?;

        insert_rows(
            &self.db_client,
            "token_metrics_latest",
            metric_rows,
            "Token Aggregator",
            "token_metrics_latest",
        )
        .await
        .with_context(|| "Failed to persist token metric snapshots to ClickHouse")?;

        Ok(())
    }

    fn metrics_has_activity(metrics: &TokenMetricsRow) -> bool {
        metrics.total_volume_usd > 0.0
            || metrics.total_buys > 0
            || metrics.total_sells > 0
            || metrics.unique_holders > 0
            || metrics.ath_price_usd > 0.0
    }

    async fn collect_events(
        &mut self,
        stream_key: &str,
        group_name: &str,
        consumer_name: &str,
    ) -> Result<Vec<(String, EventPayload)>> {
        let opts = StreamReadOptions::default()
            .group(group_name, consumer_name)
            .count(1000)
            .block(2000);
        let reply: StreamReadReply = self
            .redis_conn
            .xread_options(&[stream_key], &[">"], &opts)
            .await?;
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
}
