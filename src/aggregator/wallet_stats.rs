use crate::database::insert_rows;
use crate::handlers::constants::{NATIVE_MINT, USD1_MINT, USDC_MINT, USDT_MINT};
use crate::services::price_service::PriceService;
use crate::types::{
    EventPayload, EventType, MintRow, TradeRow, TransferRow, WalletHoldingRow,
    WalletProfileMetricsRow, WalletProfileRow,
};
use anyhow::{Result, anyhow};
use chrono::{DateTime, Datelike, TimeZone, Utc};
use clickhouse::Client;
use futures::stream::StreamExt;
use once_cell::sync::Lazy;
use redis::FromRedisValue;
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, Client as RedisClient, aio::MultiplexedConnection};
use solana_sdk::native_token::LAMPORTS_PER_SOL;
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::str::FromStr;
use std::time::Duration;

type ProfileCache = HashMap<String, WalletProfileEntry>;
type HoldingsCache = HashMap<(String, String), WalletHoldingRow>;

static FUNDING_THRESHOLD_SOL: Lazy<f64> =
    Lazy::new(|| env_parse("WALLET_FUNDING_THRESHOLD_SOL", 0.003_f64));
static WALLET_STATS_REDIS_READ_COUNT: Lazy<usize> =
    Lazy::new(|| env_parse("WALLET_STATS_REDIS_READ_COUNT", 500_usize));
static WALLET_STATS_REDIS_BLOCK_MS: Lazy<u64> =
    Lazy::new(|| env_parse("WALLET_STATS_REDIS_BLOCK_MS", 2000_u64));

fn env_parse<T: FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<T>().ok())
        .unwrap_or(default)
}

#[derive(Debug, PartialEq)]
enum TradeType {
    Buy,
    Sell,
}

// Implement TryFrom to avoid panics on invalid data.
impl TryFrom<u8> for TradeType {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(TradeType::Buy),
            1 => Ok(TradeType::Sell),
            _ => Err(anyhow!("Invalid trade type: {}", value)),
        }
    }
}

pub struct WalletAggregator {
    db_client: Client,
    redis_conn: MultiplexedConnection,
    price_service: PriceService,
}

impl WalletAggregator {
    pub async fn new(
        db_client: Client,
        redis_client: RedisClient,
        price_service: PriceService,
    ) -> Result<Self> {
        let redis_conn = redis_client.get_multiplexed_async_connection().await?;
        Ok(Self {
            db_client,
            redis_conn,
            price_service,
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        // --- Setup for the consumer ---
        let stream_key = "wallet_agg_queue";
        let group_name = "wallet_aggregators";
        let consumer_name = format!("consumer-{}", uuid::Uuid::new_v4());

        // Ensure the consumer group exists. This is idempotent.
        let result: redis::RedisResult<()> = self
            .redis_conn
            .xgroup_create_mkstream(stream_key, group_name, "0")
            .await;

        if let Err(e) = result {
            // It's okay if the group already exists. Any other error is a problem.
            if !e.to_string().contains("BUSYGROUP") {
                // This is a real error, so we exit.
                return Err(anyhow!(
                    "Failed to create or connect to consumer group: {}",
                    e
                ));
            }
            // If it's a BUSYGROUP error, we just print a confirmation and continue.
            println!(
                "[walletAggregator] Consumer group '{}' already exists. Resuming.",
                group_name
            );
        } else {
            println!(
                "[walletAggregator] Created new consumer group '{}' on stream '{}'.",
                group_name, stream_key
            );
        }

        // --- Setup for the publisher ---
        let mut publisher_conn = self.redis_conn.clone();
        let link_graph_queue = "link_graph_queue";

        loop {
            // Fetch a batch using the new stream-based function
            let messages = match collect_events_from_stream(
                &self.redis_conn,
                stream_key,
                group_name,
                &consumer_name,
            )
            .await
            {
                Ok(msgs) => msgs,
                Err(e) => {
                    eprintln!(
                        "[walletAggregator] Error reading from Redis Stream: {}. Retrying...",
                        e
                    );
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };

            if messages.is_empty() {
                continue;
            }

            // Extract just the payloads for processing, but keep the IDs for acknowledging
            let message_ids: Vec<String> = messages.iter().map(|(id, _)| id.clone()).collect();
            let payloads: Vec<EventPayload> =
                messages.into_iter().map(|(_, payload)| payload).collect();

            // Process the batch as before
            match self.process_batch(payloads.clone()).await {
                Ok(_) => {
                    println!(
                        "[walletAggregator] ✅ Batch processed successfully. Forwarding to LinkGraph."
                    );
                    // Forward to the next queue
                    for payload in payloads {
                        let payload_data = bincode::serialize(&payload)?;
                        let _: () = publisher_conn
                            .xadd(link_graph_queue, "*", &[("payload", payload_data)])
                            .await?;
                    }
                    // IMPORTANT: Acknowledge the messages from the source queue
                    if !message_ids.is_empty() {
                        let result: redis::RedisResult<()> = self
                            .redis_conn
                            .xack(stream_key, group_name, &message_ids)
                            .await;
                        if let Err(e) = result {
                            eprintln!(
                                "[walletAggregator] 🔴 FAILED to acknowledge messages: {}",
                                e
                            );
                        } else if let Err(e) = self
                            .redis_conn
                            .xdel::<_, _, i64>(stream_key, &message_ids)
                            .await
                        {
                            eprintln!("[walletAggregator] 🔴 FAILED to delete messages: {}", e);
                        }
                    }
                }
                Err(e) => {
                    println!(
                        "[walletAggregator] ❌ Failed to process batch, will not forward or ACK. Error: {}",
                        e
                    );
                }
            }
        }
    }

    async fn fetch_wallet_tags(
        &self,
        wallet_addresses: Vec<String>,
    ) -> Result<HashMap<String, Vec<String>>> {
        if wallet_addresses.is_empty() {
            return Ok(HashMap::new());
        }

        #[derive(clickhouse::Row, serde::Deserialize)]
        struct WalletTagInfo {
            wallet_address: String,
            tag: String,
        }

        let mut cursor = self
            .db_client
            .query("SELECT wallet_address, tag FROM known_wallets WHERE wallet_address IN ?")
            .bind(wallet_addresses)
            .fetch::<WalletTagInfo>()?;

        let mut tags_map: HashMap<String, Vec<String>> = HashMap::new();
        while let Some(row) = cursor.next().await? {
            tags_map
                .entry(row.wallet_address)
                .or_default()
                .push(row.tag);
        }

        Ok(tags_map)
    }

    async fn process_batch(&self, payloads: Vec<EventPayload>) -> Result<()> {
        let wallets_in_batch: Vec<String> = payloads
            .iter()
            .flat_map(|payload| match &payload.event {
                EventType::Trade(trade) => vec![trade.maker.clone()],
                EventType::Transfer(transfer) => {
                    vec![transfer.source.clone(), transfer.destination.clone()]
                }
                EventType::Mint(mint) => vec![mint.creator_address.clone()],
                _ => Vec::new(),
            })
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        let holding_keys_in_batch: Vec<(String, String)> = payloads
            .iter()
            .flat_map(|payload| match &payload.event {
                EventType::Trade(trade) => vec![(trade.maker.clone(), trade.base_address.clone())],
                EventType::Transfer(transfer) => vec![
                    (transfer.source.clone(), transfer.mint_address.clone()),
                    (transfer.destination.clone(), transfer.mint_address.clone()),
                ],
                _ => Vec::new(),
            })
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        if wallets_in_batch.is_empty() {
            return Ok(());
        }

        let mut profiles = self.fetch_profiles(&wallets_in_batch).await?;
        let mut holdings = self.fetch_holdings(&holding_keys_in_batch).await?;

        let transfer_wallets: HashSet<String> = payloads
            .iter()
            .filter_map(|payload| match &payload.event {
                EventType::Transfer(transfer) => {
                    Some(vec![transfer.source.clone(), transfer.destination.clone()])
                }
                _ => None,
            })
            .flatten()
            .collect();

        let new_wallets_to_tag: Vec<String> = transfer_wallets
            .iter()
            .filter(|wallet| !profiles.contains_key(*wallet))
            .cloned()
            .collect();

        let new_tags = self.fetch_wallet_tags(new_wallets_to_tag).await?;

        for payload in payloads.iter() {
            match &payload.event {
                EventType::Trade(trade) => {
                    if let Err(err) = self
                        .process_trade(trade, payload, &mut profiles, &mut holdings)
                        .await
                    {
                        println!(
                            "[walletStats] WARN failed to process trade {}, skipping. Error: {}",
                            trade.signature, err
                        );
                    }
                }
                EventType::Transfer(transfer) => self.process_transfer(
                    transfer,
                    payload,
                    &mut profiles,
                    &mut holdings,
                    &new_tags,
                ),
                EventType::Mint(mint) => self.process_mint(mint, &mut profiles),
                _ => {}
            }
        }

        self.finalize_and_persist(profiles, holdings).await?;
        Ok(())
    }

    fn process_mint(&self, mint: &MintRow, profiles: &mut ProfileCache) {
        let entry = profiles
            .entry(mint.creator_address.clone())
            .or_insert_with(|| {
                WalletProfileEntry::new(mint.creator_address.clone(), mint.timestamp)
            });
        let profile = &mut entry.profile;

        profile.updated_at = mint.timestamp;
        profile.last_seen_ts = profile.last_seen_ts.max(mint.timestamp);

        if !profile.deployed_tokens.contains(&mint.mint_address) {
            profile.deployed_tokens.push(mint.mint_address.clone());
        }
    }

    async fn process_trade(
        &self,
        trade: &TradeRow,
        payload: &EventPayload,
        profiles: &mut ProfileCache,
        holdings: &mut HoldingsCache,
    ) -> Result<()> {
        let entry = profiles
            .entry(trade.maker.clone())
            .or_insert_with(|| WalletProfileEntry::new(trade.maker.clone(), trade.timestamp));

        let native_price = self.price_service.get_price(trade.timestamp).await;

        let (holding_period_update, realized_profit_sol, realized_profit_usd) =
            if trade.base_address != NATIVE_MINT {
                let holding = holdings
                    .entry((trade.maker.clone(), trade.base_address.clone()))
                    .or_insert_with(|| {
                        WalletHoldingRow::new(
                            trade.maker.clone(),
                            trade.base_address.clone(),
                            trade.timestamp,
                        )
                    });
                self.update_holding_from_trade(holding, trade, payload, native_price)
                    .await?
            } else {
                (0.0, 0.0, 0.0)
            };

        self.update_profile_from_trade(
            entry,
            trade,
            payload,
            native_price,
            holding_period_update,
            realized_profit_sol,
            realized_profit_usd,
        );
        Ok(())
    }

    async fn update_holding_from_trade(
        &self,
        holding: &mut WalletHoldingRow,
        trade: &TradeRow,
        payload: &EventPayload,
        native_price: f64,
    ) -> Result<(f64, f64, f64)> {
        let trade_type = TradeType::try_from(trade.trade_type)?;
        let base_decimals = payload
            .token_decimals
            .get(&trade.base_address)
            .cloned()
            .unwrap_or(9);
        let quote_decimals = payload
            .token_decimals
            .get(&trade.quote_address)
            .cloned()
            .unwrap_or(9);
        let base_amount_decimal = trade.base_amount as f64 / 10f64.powi(base_decimals as i32);
        let quote_amount_decimal = trade.quote_amount as f64 / 10f64.powi(quote_decimals as i32);

        let trade_total_in_sol = self.convert_to_sol(
            quote_amount_decimal,
            &trade.quote_address,
            trade.total_usd,
            native_price,
        );
        let mut realized_profit_sol = 0.0;
        let mut realized_profit_usd = 0.0;
        let mut holding_period_update = 0.0;

        match trade_type {
            TradeType::Buy => {
                // If re-entering a position from flat, reset holding start.
                if holding.current_balance <= 0.0 {
                    holding.start_holding_at = trade.timestamp;
                }
                holding.history_bought_amount += base_amount_decimal;
                holding.history_bought_cost_sol += trade_total_in_sol;
                holding.current_balance += base_amount_decimal;
            }
            TradeType::Sell => {
                let pre_balance = holding.current_balance;
                let sold_amount = base_amount_decimal.min(pre_balance);
                holding.history_sold_amount += base_amount_decimal;
                holding.history_sold_income_sol += trade_total_in_sol;
                holding.current_balance = (holding.current_balance - base_amount_decimal).max(0.0);

                let avg_cost_per_token = if holding.history_bought_amount > 0.0 {
                    holding.history_bought_cost_sol
                        / holding.history_bought_amount.max(f64::EPSILON)
                } else {
                    0.0
                };
                let cost_basis_sol = avg_cost_per_token * sold_amount;
                realized_profit_sol = trade_total_in_sol - cost_basis_sol;
                realized_profit_usd = realized_profit_sol * native_price;

                holding.realized_profit_sol += realized_profit_sol;
                holding.realized_profit_usd += realized_profit_usd;
                // Reduce basis to reflect sold amount.
                holding.history_bought_amount =
                    (holding.history_bought_amount - sold_amount).max(0.0);
                holding.history_bought_cost_sol =
                    (holding.history_bought_cost_sol - cost_basis_sol).max(0.0);
                if holding.history_bought_cost_sol > 0.0 {
                    holding.realized_profit_pnl =
                        (holding.realized_profit_sol / holding.history_bought_cost_sol) as f32;
                }

                // Holding period update: only when selling from an existing position.
                if sold_amount > 0.0
                    && holding.start_holding_at > 0
                    && trade.timestamp > holding.start_holding_at
                {
                    holding_period_update = (trade.timestamp - holding.start_holding_at) as f64;
                }
                // If position is closed, reset holding start.
                if holding.current_balance <= f64::EPSILON {
                    holding.start_holding_at = trade.timestamp;
                }
            }
        }

        holding.updated_at = trade.timestamp;
        Ok((
            holding_period_update,
            realized_profit_sol,
            realized_profit_usd,
        ))
    }

    fn update_profile_from_trade(
        &self,
        entry: &mut WalletProfileEntry,
        trade: &TradeRow,
        payload: &EventPayload,
        native_price: f64,
        holding_period_update: f64,
        realized_profit_sol: f64,
        realized_profit_usd: f64,
    ) {
        let profile = &mut entry.profile;
        let metrics = &mut entry.metrics;
        let event_time = Utc
            .timestamp_opt(trade.timestamp as i64, 0)
            .single()
            .unwrap_or_else(|| Utc::now());
        self.reset_periodic_stats_if_needed(metrics, event_time);
        let trade_type = TradeType::try_from(trade.trade_type).unwrap();

        let quote_decimals = payload
            .token_decimals
            .get(&trade.quote_address)
            .cloned()
            .unwrap_or(9);
        let quote_amount_decimal = trade.quote_amount as f64 / 10f64.powi(quote_decimals as i32);
        let trade_total_in_sol = self.convert_to_sol(
            quote_amount_decimal,
            &trade.quote_address,
            trade.total_usd,
            native_price,
        );
        let trade_total_in_usd = trade.total_usd;

        // --- Periodic Stats Calculation (1D, 7D, 30D) ---
        let mut stats_1d = (
            metrics.stats_1d_buy_count,
            metrics.stats_1d_sell_count,
            metrics.stats_1d_total_bought_cost_sol,
            metrics.stats_1d_total_bought_cost_usd,
            metrics.stats_1d_total_sold_income_sol,
            metrics.stats_1d_total_sold_income_usd,
            metrics.stats_1d_realized_profit_sol,
            metrics.stats_1d_realized_profit_usd,
            metrics.stats_1d_winrate * metrics.stats_1d_sell_count as f32, // win count
            metrics.stats_1d_avg_holding_period,
        );
        let mut stats_7d = (
            metrics.stats_7d_buy_count,
            metrics.stats_7d_sell_count,
            metrics.stats_7d_total_bought_cost_sol,
            metrics.stats_7d_total_bought_cost_usd,
            metrics.stats_7d_total_sold_income_sol,
            metrics.stats_7d_total_sold_income_usd,
            metrics.stats_7d_realized_profit_sol,
            metrics.stats_7d_realized_profit_usd,
            metrics.stats_7d_winrate * metrics.stats_7d_sell_count as f32, // win count
            metrics.stats_7d_avg_holding_period,
        );
        let mut stats_30d = (
            metrics.stats_30d_buy_count,
            metrics.stats_30d_sell_count,
            metrics.stats_30d_total_bought_cost_sol,
            metrics.stats_30d_total_bought_cost_usd,
            metrics.stats_30d_total_sold_income_sol,
            metrics.stats_30d_total_sold_income_usd,
            metrics.stats_30d_realized_profit_sol,
            metrics.stats_30d_realized_profit_usd,
            metrics.stats_30d_winrate * metrics.stats_30d_sell_count as f32, // win count
            metrics.stats_30d_avg_holding_period,
        );

        // --- Total Stats Calculation ---
        let previous_total_wins = metrics.total_winrate * metrics.total_sells_count as f32;
        let mut new_total_buys = metrics.total_buys_count;
        let mut new_total_sells = metrics.total_sells_count;
        let mut current_total_wins = previous_total_wins;

        if trade_type == TradeType::Buy {
            new_total_buys += 1;
            stats_1d.0 += 1;
            stats_1d.2 += trade_total_in_sol;
            stats_1d.3 += trade_total_in_usd;
            stats_7d.0 += 1;
            stats_7d.2 += trade_total_in_sol;
            stats_7d.3 += trade_total_in_usd;
            stats_30d.0 += 1;
            stats_30d.2 += trade_total_in_sol;
            stats_30d.3 += trade_total_in_usd;
        } else {
            // Sell
            new_total_sells += 1;
            if realized_profit_sol >= 0.0 {
                current_total_wins += 1.0;
                stats_1d.8 += 1.0;
                stats_7d.8 += 1.0;
                stats_30d.8 += 1.0;
            }
            stats_1d.1 += 1;
            stats_1d.4 += trade_total_in_sol;
            stats_1d.5 += trade_total_in_usd;
            stats_1d.6 += realized_profit_sol;
            stats_1d.7 += realized_profit_usd;
            stats_7d.1 += 1;
            stats_7d.4 += trade_total_in_sol;
            stats_7d.5 += trade_total_in_usd;
            stats_7d.6 += realized_profit_sol;
            stats_7d.7 += realized_profit_usd;
            stats_30d.1 += 1;
            stats_30d.4 += trade_total_in_sol;
            stats_30d.5 += trade_total_in_usd;
            stats_30d.6 += realized_profit_sol;
            stats_30d.7 += realized_profit_usd;
        }

        // Track token count per window (per trade event).
        metrics.stats_1d_tokens_traded += 1;
        metrics.stats_7d_tokens_traded += 1;
        metrics.stats_30d_tokens_traded += 1;

        let new_total_winrate = if new_total_sells > 0 {
            current_total_wins / new_total_sells as f32
        } else {
            0.0
        };

        // --- Finalize Periodic Stats ---
        if holding_period_update > 0.0 {
            if stats_1d.1 > 0 {
                let prev = (stats_1d.1 - 1) as f64;
                stats_1d.9 =
                    ((stats_1d.9 as f64 * prev + holding_period_update) / stats_1d.1 as f64) as f32;
            }
            if stats_7d.1 > 0 {
                let prev = (stats_7d.1 - 1) as f64;
                stats_7d.9 =
                    ((stats_7d.9 as f64 * prev + holding_period_update) / stats_7d.1 as f64) as f32;
            }
            if stats_30d.1 > 0 {
                let prev = (stats_30d.1 - 1) as f64;
                stats_30d.9 = ((stats_30d.9 as f64 * prev + holding_period_update)
                    / stats_30d.1 as f64) as f32;
            }
        }
        let final_winrate_1d = if stats_1d.1 > 0 {
            stats_1d.8 / stats_1d.1 as f32
        } else {
            0.0
        };
        let stats_1d_realized_profit_pnl = if stats_1d.2 > 0.0 {
            (stats_1d.6 / stats_1d.2) as f32
        } else {
            0.0
        };
        let final_winrate_7d = if stats_7d.1 > 0 {
            stats_7d.8 / stats_7d.1 as f32
        } else {
            0.0
        };
        let stats_7d_realized_profit_pnl = if stats_7d.2 > 0.0 {
            (stats_7d.6 / stats_7d.2) as f32
        } else {
            0.0
        };
        let final_winrate_30d = if stats_30d.1 > 0 {
            stats_30d.8 / stats_30d.1 as f32
        } else {
            0.0
        };
        let stats_30d_realized_profit_pnl = if stats_30d.2 > 0.0 {
            (stats_30d.6 / stats_30d.2) as f32
        } else {
            0.0
        };

        profile.updated_at = trade.timestamp;
        profile.last_seen_ts = profile.last_seen_ts.max(trade.timestamp);
        metrics.updated_at = trade.timestamp;

        if let Some(lamports) = payload
            .balances
            .get(NATIVE_MINT)
            .and_then(|b| b.get(&trade.maker))
        {
            metrics.balance = *lamports as f64 / LAMPORTS_PER_SOL as f64;
        }

        metrics.total_buys_count = new_total_buys;
        metrics.total_sells_count = new_total_sells;
        metrics.total_winrate = new_total_winrate;

        metrics.stats_1d_buy_count = stats_1d.0;
        metrics.stats_1d_sell_count = stats_1d.1;
        metrics.stats_1d_total_bought_cost_sol = stats_1d.2;
        metrics.stats_1d_total_bought_cost_usd = stats_1d.3;
        metrics.stats_1d_total_sold_income_sol = stats_1d.4;
        metrics.stats_1d_total_sold_income_usd = stats_1d.5;
        metrics.stats_1d_realized_profit_sol = stats_1d.6;
        metrics.stats_1d_realized_profit_usd = stats_1d.7;
        metrics.stats_1d_avg_holding_period = stats_1d.9;
        metrics.stats_1d_realized_profit_pnl = stats_1d_realized_profit_pnl;
        metrics.stats_1d_winrate = final_winrate_1d;
        metrics.stats_1d_total_fee += trade.priority_fee;

        metrics.stats_7d_buy_count = stats_7d.0;
        metrics.stats_7d_sell_count = stats_7d.1;
        metrics.stats_7d_total_bought_cost_sol = stats_7d.2;
        metrics.stats_7d_total_bought_cost_usd = stats_7d.3;
        metrics.stats_7d_total_sold_income_sol = stats_7d.4;
        metrics.stats_7d_total_sold_income_usd = stats_7d.5;
        metrics.stats_7d_realized_profit_sol = stats_7d.6;
        metrics.stats_7d_realized_profit_usd = stats_7d.7;
        metrics.stats_7d_avg_holding_period = stats_7d.9;
        metrics.stats_7d_realized_profit_pnl = stats_7d_realized_profit_pnl;
        metrics.stats_7d_winrate = final_winrate_7d;
        metrics.stats_7d_total_fee += trade.priority_fee;

        metrics.stats_30d_buy_count = stats_30d.0;
        metrics.stats_30d_sell_count = stats_30d.1;
        metrics.stats_30d_total_bought_cost_sol = stats_30d.2;
        metrics.stats_30d_total_bought_cost_usd = stats_30d.3;
        metrics.stats_30d_total_sold_income_sol = stats_30d.4;
        metrics.stats_30d_total_sold_income_usd = stats_30d.5;
        metrics.stats_30d_realized_profit_sol = stats_30d.6;
        metrics.stats_30d_realized_profit_usd = stats_30d.7;
        metrics.stats_30d_avg_holding_period = stats_30d.9;
        metrics.stats_30d_realized_profit_pnl = stats_30d_realized_profit_pnl;
        metrics.stats_30d_winrate = final_winrate_30d;
        metrics.stats_30d_total_fee += trade.priority_fee;
    }

    fn reset_periodic_stats_if_needed(
        &self,
        metrics: &mut WalletProfileMetricsRow,
        event_time: DateTime<Utc>,
    ) {
        let last_update = Utc
            .timestamp_opt(metrics.updated_at as i64, 0)
            .single()
            .unwrap_or(event_time);

        if event_time.ordinal() != last_update.ordinal() || event_time.year() != last_update.year()
        {
            metrics.stats_1d_buy_count = 0;
            metrics.stats_1d_sell_count = 0;
            metrics.stats_1d_total_bought_cost_sol = 0.0;
            metrics.stats_1d_total_bought_cost_usd = 0.0;
            metrics.stats_1d_total_sold_income_sol = 0.0;
            metrics.stats_1d_total_sold_income_usd = 0.0;
            metrics.stats_1d_realized_profit_sol = 0.0;
            metrics.stats_1d_realized_profit_usd = 0.0;
            metrics.stats_1d_realized_profit_pnl = 0.0;
            metrics.stats_1d_winrate = 0.0;
            metrics.stats_1d_tokens_traded = 0;
            metrics.stats_1d_avg_holding_period = 0.0;
            metrics.stats_1d_transfer_in_count = 0;
            metrics.stats_1d_transfer_out_count = 0;
            metrics.stats_1d_total_fee = 0.0;
        }

        if event_time.iso_week().week() != last_update.iso_week().week()
            || event_time.year() != last_update.year()
        {
            metrics.stats_7d_buy_count = 0;
            metrics.stats_7d_sell_count = 0;
            metrics.stats_7d_total_bought_cost_sol = 0.0;
            metrics.stats_7d_total_bought_cost_usd = 0.0;
            metrics.stats_7d_total_sold_income_sol = 0.0;
            metrics.stats_7d_total_sold_income_usd = 0.0;
            metrics.stats_7d_realized_profit_sol = 0.0;
            metrics.stats_7d_realized_profit_usd = 0.0;
            metrics.stats_7d_realized_profit_pnl = 0.0;
            metrics.stats_7d_winrate = 0.0;
            metrics.stats_7d_tokens_traded = 0;
            metrics.stats_7d_avg_holding_period = 0.0;
            metrics.stats_7d_transfer_in_count = 0;
            metrics.stats_7d_transfer_out_count = 0;
            metrics.stats_7d_total_fee = 0.0;
        }

        if event_time.month() != last_update.month() || event_time.year() != last_update.year() {
            metrics.stats_30d_buy_count = 0;
            metrics.stats_30d_sell_count = 0;
            metrics.stats_30d_total_bought_cost_sol = 0.0;
            metrics.stats_30d_total_bought_cost_usd = 0.0;
            metrics.stats_30d_total_sold_income_sol = 0.0;
            metrics.stats_30d_total_sold_income_usd = 0.0;
            metrics.stats_30d_realized_profit_sol = 0.0;
            metrics.stats_30d_realized_profit_usd = 0.0;
            metrics.stats_30d_realized_profit_pnl = 0.0;
            metrics.stats_30d_winrate = 0.0;
            metrics.stats_30d_tokens_traded = 0;
            metrics.stats_30d_avg_holding_period = 0.0;
            metrics.stats_30d_transfer_in_count = 0;
            metrics.stats_30d_transfer_out_count = 0;
            metrics.stats_30d_total_fee = 0.0;
        }
    }

    fn process_transfer(
        &self,
        transfer: &TransferRow,
        payload: &EventPayload,
        profiles: &mut ProfileCache,
        holdings: &mut HoldingsCache,
        new_tags: &HashMap<String, Vec<String>>, // ADD this parameter
    ) {
        let event_time = Utc
            .timestamp_opt(transfer.timestamp as i64, 0)
            .single()
            .unwrap_or_else(|| Utc::now());
        let event_ts = transfer.timestamp;

        let process_wallet =
            |wallet_address: &String, is_source: bool, profiles: &mut ProfileCache| {
                let entry = profiles.entry(wallet_address.clone()).or_insert_with(|| {
                    let mut new_entry =
                        WalletProfileEntry::new(wallet_address.clone(), transfer.timestamp);
                    if let Some(tags) = new_tags.get(wallet_address) {
                        new_entry.profile.tags = tags.clone();
                    }
                    new_entry
                });

                if entry.profile.tags.is_empty() {
                    if let Some(tags) = new_tags.get(wallet_address) {
                        entry.profile.tags = tags.clone();
                    }
                }

                let profile = &mut entry.profile;
                let metrics = &mut entry.metrics;
                self.reset_periodic_stats_if_needed(metrics, event_time);
                profile.updated_at = event_ts;
                profile.last_seen_ts = profile.last_seen_ts.max(transfer.timestamp);
                metrics.updated_at = event_ts;

                if let Some(lamports) = payload
                    .balances
                    .get(NATIVE_MINT)
                    .and_then(|b| b.get(wallet_address))
                {
                    metrics.balance = *lamports as f64 / LAMPORTS_PER_SOL as f64;
                }

                let is_spl = transfer.mint_address != NATIVE_MINT;

                if !is_source && !is_spl {
                    let post_balance_lamports = payload
                        .balances
                        .get(NATIVE_MINT)
                        .and_then(|b| b.get(wallet_address))
                        .cloned()
                        .unwrap_or(0);

                    let post_balance_sol = post_balance_lamports as f64 / LAMPORTS_PER_SOL as f64;
                    let pre_balance_sol = post_balance_sol - transfer.amount_decimal;
                    if pre_balance_sol <= *FUNDING_THRESHOLD_SOL {
                        profile.funded_from = transfer.source.clone();
                        profile.funded_timestamp = transfer.timestamp;
                        profile.funded_signature = transfer.signature.clone();
                        profile.funded_amount = transfer.amount_decimal;
                    }
                }

                if is_source {
                    if is_spl {
                        metrics.spl_transfers_out_count += 1;
                    } else {
                        metrics.transfers_out_count += 1;
                    }
                    metrics.stats_7d_transfer_out_count += 1;
                    metrics.stats_30d_transfer_out_count += 1;
                    metrics.stats_1d_transfer_out_count += 1;
                } else {
                    if is_spl {
                        metrics.spl_transfers_in_count += 1;
                    } else {
                        metrics.transfers_in_count += 1;
                    }
                    metrics.stats_7d_transfer_in_count += 1;
                    metrics.stats_30d_transfer_in_count += 1;
                    metrics.stats_1d_transfer_in_count += 1;
                }
            };

        let process_holding =
            |wallet_address: &String, is_source: bool, holdings: &mut HoldingsCache| {
                if transfer.mint_address == NATIVE_MINT {
                    return;
                }
                let holding_key = (wallet_address.clone(), transfer.mint_address.clone());
                let holding = holdings.entry(holding_key).or_insert_with(|| {
                    WalletHoldingRow::new(
                        wallet_address.clone(),
                        transfer.mint_address.clone(),
                        transfer.timestamp,
                    )
                });

                holding.updated_at = event_ts;

                if is_source {
                    holding.current_balance = transfer.source_balance;
                    holding.history_transfer_out += 1;
                } else {
                    holding.current_balance = transfer.destination_balance;
                    holding.history_transfer_in += 1;
                }
            };

        process_wallet(&transfer.source, true, profiles);
        process_holding(&transfer.source, true, holdings);
        if transfer.source != transfer.destination {
            process_wallet(&transfer.destination, false, profiles);
            process_holding(&transfer.destination, false, holdings);
        }
    }
    fn convert_to_sol(
        &self,
        amount: f64,
        quote_address: &str,
        total_usd: f64,
        native_price: f64,
    ) -> f64 {
        if quote_address == NATIVE_MINT {
            amount
        } else if (quote_address == USDC_MINT
            || quote_address == USDT_MINT
            || quote_address == USD1_MINT)
            && native_price > 0.0
        {
            total_usd / native_price
        } else {
            0.0
        }
    }

    async fn finalize_and_persist(
        &self,
        profiles: ProfileCache,
        holdings: HoldingsCache,
    ) -> Result<()> {
        let mut updated_profiles = Vec::new();
        let mut updated_metrics = Vec::new();
        for entry in profiles.into_values() {
            updated_profiles.push(entry.profile);
            updated_metrics.push(entry.metrics);
        }
        if !updated_profiles.is_empty() {
            println!("[db] 💾 Flushing {} profiles...", updated_profiles.len());
            insert_rows(
                &self.db_client,
                "wallet_profiles",
                updated_profiles,
                "Wallet Aggregator",
                "profiles",
            )
            .await?;
        }
        if !updated_metrics.is_empty() {
            println!(
                "[db] 💾 Flushing {} profile metrics...",
                updated_metrics.len()
            );
            insert_rows(
                &self.db_client,
                "wallet_profile_metrics",
                updated_metrics,
                "Wallet Aggregator",
                "profile_metrics",
            )
            .await?;
        }
        let updated_holdings: Vec<WalletHoldingRow> = holdings.into_values().collect();
        if !updated_holdings.is_empty() {
            println!("[db] 💾 Flushing {} holdings...", updated_holdings.len());
            insert_rows(
                &self.db_client,
                "wallet_holdings",
                updated_holdings,
                "Wallet Aggregator",
                "holdings",
            )
            .await?;
        }
        Ok(())
    }

    async fn fetch_profiles(&self, wallets: &[String]) -> Result<ProfileCache> {
        if wallets.is_empty() {
            return Ok(HashMap::new());
        }

        let mut profile_cursor = self
            .db_client
            .query(
                "
                SELECT * FROM wallet_profiles
                WHERE wallet_address IN ?
                ORDER BY updated_at DESC
            ",
            )
            .bind(wallets)
            .fetch::<WalletProfileRow>()?;
        let mut profiles: HashMap<String, WalletProfileRow> = HashMap::new();
        while let Some(profile) = profile_cursor.next().await? {
            profiles.insert(profile.wallet_address.clone(), profile);
        }

        let mut metrics_cursor = self
            .db_client
            .query(
                "
                SELECT * FROM wallet_profile_metrics
                WHERE wallet_address IN ?
                ORDER BY updated_at DESC
            ",
            )
            .bind(wallets)
            .fetch::<WalletProfileMetricsRow>()?;
        let mut metrics_map: HashMap<String, WalletProfileMetricsRow> = HashMap::new();
        while let Some(metrics) = metrics_cursor.next().await? {
            metrics_map.insert(metrics.wallet_address.clone(), metrics);
        }

        let mut entries = HashMap::new();
        for (wallet, profile) in profiles {
            let metrics = metrics_map.remove(&wallet);
            entries.insert(
                wallet.clone(),
                WalletProfileEntry::from_parts(profile, metrics),
            );
        }
        Ok(entries)
    }

    async fn fetch_holdings(&self, keys: &[(String, String)]) -> Result<HoldingsCache> {
        if keys.is_empty() {
            return Ok(HashMap::new());
        }
        let mut cursor = self
            .db_client
            .query(
                "
                SELECT * FROM wallet_holdings
                WHERE (wallet_address, mint_address) IN ?
                ORDER BY updated_at DESC
            ",
            )
            .bind(keys)
            .fetch::<WalletHoldingRow>()?;
        let mut holdings = HashMap::new();
        while let Some(holding) = cursor.next().await? {
            holdings.insert(
                (holding.wallet_address.clone(), holding.mint_address.clone()),
                holding,
            );
        }
        Ok(holdings)
    }
}

async fn collect_events_from_stream(
    redis_conn: &MultiplexedConnection,
    stream_key: &str,
    group_name: &str,
    consumer_name: &str,
) -> Result<Vec<(String, EventPayload)>> {
    let mut conn = redis_conn.clone();
    let opts = StreamReadOptions::default()
        .group(group_name, consumer_name)
        .count(*WALLET_STATS_REDIS_READ_COUNT) // Read up to N messages at a time
        .block((*WALLET_STATS_REDIS_BLOCK_MS) as usize); // Block for up to N ms waiting for messages

    let reply: StreamReadReply = conn.xread_options(&[stream_key], &[">"], &opts).await?;

    let mut events = Vec::new();
    for stream_entry in reply.keys {
        for message in stream_entry.ids {
            // --- THIS IS THE CORRECTED CODE ---
            if let Some(payload_value) = message.map.get("payload") {
                if let Ok(payload_bytes) = Vec::<u8>::from_redis_value(payload_value) {
                    if let Ok(payload) = bincode::deserialize::<EventPayload>(&payload_bytes) {
                        events.push((message.id.clone(), payload));
                    }
                }
            }
            // --- END CORRECTION ---
        }
    }
    Ok(events)
}
struct WalletProfileEntry {
    profile: WalletProfileRow,
    metrics: WalletProfileMetricsRow,
}

impl WalletProfileEntry {
    fn new(wallet_address: String, timestamp: u32) -> Self {
        Self {
            profile: WalletProfileRow::new(wallet_address.clone(), timestamp),
            metrics: WalletProfileMetricsRow::new(wallet_address, timestamp),
        }
    }

    fn from_parts(profile: WalletProfileRow, metrics: Option<WalletProfileMetricsRow>) -> Self {
        let wallet_address = profile.wallet_address.clone();
        Self {
            metrics: metrics.unwrap_or_else(|| {
                WalletProfileMetricsRow::new(wallet_address.clone(), profile.updated_at)
            }),
            profile,
        }
    }
}
