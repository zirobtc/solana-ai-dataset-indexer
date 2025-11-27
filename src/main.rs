#![allow(warnings)]

use anyhow::{Result, anyhow};
use backoff::{ExponentialBackoff, future::retry};
use clickhouse::Client;
use dotenvy::dotenv;
use futures_util::StreamExt;
use redis::{
    Client as RedisClient,
    streams::{StreamReadOptions, StreamReadReply},
};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    hash::Hash,
    instruction::CompiledInstruction,
    message::{Message, VersionedMessage},
    pubkey::Pubkey,
    signature::Signature,
    transaction::VersionedTransaction,
};
use yellowstone_grpc_proto::{prelude::SubscribeUpdateTransaction, solana};

use neo4rs::Graph;
use once_cell::sync::Lazy;
use redis::FromRedisValue;
use redis::{AsyncCommands, streams::StreamMaxlen};
use std::{
    collections::HashMap,
    env,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    vec,
};
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio::{
    sync::mpsc,
    task::JoinHandle,
    time::{MissedTickBehavior, interval, sleep, timeout},
};
use tonic::transport::ClientTlsConfig;
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::prelude::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterTransactions, TransactionStatusMeta,
    subscribe_update::UpdateOneof,
};

// --- MODULES ---
mod aggregator;
mod database;
mod handlers;
mod services;
mod simulator;
mod spl_system_decoder;
mod types;
mod utils;
use crate::services::price_service::{PriceService, price_updater_task};
use crate::{
    aggregator::{
        link_graph::{LinkGraph, WriteJob},
        token_stats::TokenAggregator,
    },
    services::solana_subscribers::{car_file_subscriber, geyser_subscriber, websocket_subscriber},
    types::{
        BurnRow, EventPayload, EventType, FeeCollectionRow, LiquidityRow, MigrationRow, MintRow,
        PoolCreationRow, SupplyLockActionRow, SupplyLockRow, TradeRow, TransferRow,
        UnifiedTransaction,
    },
};

fn channel_capacity_from_env(var: &str, default: usize) -> usize {
    env::var(var)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|cap| *cap > 0)
        .unwrap_or(default)
}

// --- Use the new handlers and the trait ---
use handlers::{
    TransactionHandler,
    TransactionInfo,
    pump_fun_amm_handler::PumpFunAmmHandler,
    pump_fun_launchpad_handler::PumpFunLaunchpadHandler,
    spl_token_handler::SplTokenHandler,
    streamflow_handler::StreamflowHandler,
    // raydium_cpmm_handler::RaydiumCpmmHandler, raydium_launchpad_handler::RaydiumLaunchpadHandler,
    system_handler::SystemHandler,
};

use crate::database::insert_rows;

async fn transaction_processor(
    rx_receiver: Arc<Mutex<mpsc::Receiver<UnifiedTransaction<'static>>>>,
    protocol_handlers: Vec<Box<dyn TransactionHandler>>,
    solana_handlers: Vec<Box<dyn TransactionHandler>>,
    price_service: PriceService,
    event_payload_sender: mpsc::Sender<EventPayload>,
    worker_id: usize,
) -> Result<()> {
    println!(
        " Transaction processor worker {} started with {} protocol handlers.",
        worker_id,
        protocol_handlers.len()
    );

    // --- Metrics Tracking ---
    let mut last_log_time = Instant::now();
    let mut total_processed_count: u64 = 0;
    let mut last_log_tx_count: u64 = 0;

    let mut idle_interval = interval(Duration::from_secs(15));
    idle_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut activity_since_idle_log = false;

    loop {
        tokio::select! {
            maybe_tx = async {
                let mut guard = rx_receiver.lock().await;
                guard.recv().await
            } => {
                let tx = match maybe_tx {
                    Some(tx) => tx,
                    None => break,
                };
                activity_since_idle_log = true;

                // Refresh price cache periodically
                let mut historical_price = price_service.get_price(tx.block_time).await;

                if historical_price == 0.0 {
                    historical_price = 195.0
                }

                // println!("\n[Processor] Processing TX: {} at price ${:.4}", tx.signature, historical_price);

                if let Some(e) = &tx.error {
                    // println!("[Processor] ⚠️ Processing Failed TX {:?}", e);
                }

                for handler in &protocol_handlers {
                    if handler.is_of_interest(&tx) {
                        match handler.process_transaction(&tx, historical_price).await {
                            Ok(events) => {
                                for event in events {
                                    if let Err(e) = event_payload_sender.send(event).await {
                                        eprintln!("[Processor] MPSC channel closed: {}", e);
                                        return Ok(());
                                    }
                                }
                            },
                            Err(e) => { println!("[Processor] Handler '{}' failed: {}", handler.name(), e) }
                        }
                    }
                }

                for handler in &solana_handlers {
                    if handler.is_of_interest(&tx) {
                        match handler.process_transaction(&tx, historical_price).await {
                            Ok(events) => {
                                for event in events {
                                    if let Err(e) = event_payload_sender.send(event).await {
                                        eprintln!("[Processor] MPSC channel closed: {}", e);
                                        return Ok(());
                                    }
                                }
                            },
                            Err(e) => { println!("[Processor] Handler '{}' failed: {}", handler.name(), e) }
                        }
                    }
                }

                total_processed_count += 1;

                let now = Instant::now();
                if now.duration_since(last_log_time) >= Duration::from_secs(10) {
                    let tx_delta = total_processed_count.saturating_sub(last_log_tx_count);
                    let seconds_delta = now.duration_since(last_log_time).as_secs_f64();
                    if seconds_delta > 0.0 {
                        let processing_rate = tx_delta as f64 / seconds_delta;
                        println!(
                            "[Processor] Throughput: {:.0} tx/s (processed {} in last {:.2}s). Total processed: {}.",
                            processing_rate, tx_delta, seconds_delta, total_processed_count
                        );
                    }
                    last_log_time = now;
                    last_log_tx_count = total_processed_count;
                }
            }
            _ = idle_interval.tick() => {
                if activity_since_idle_log {
                    activity_since_idle_log = false;
                } else {
                    println!("[Processor] Idle: no transactions received in the last 15s. Awaiting upstream data...");
                }
            }
        }
    }

    Ok(())
}

async fn database_writer_service(
    db_client: Client,
    redis_client: RedisClient,
    read_count: usize,
) -> Result<()> {
    println!("[DB Writer] Service started.");
    let mut conn = redis_client.get_multiplexed_async_connection().await?;
    let stream_key = "event_queue";
    let group_name = "database_writers";
    let consumer_name = format!("consumer-db-{}", uuid::Uuid::new_v4());
    static DB_WRITER_DLQ_STREAM: Lazy<String> = Lazy::new(|| {
        std::env::var("DB_WRITER_DLQ_STREAM").unwrap_or_else(|_| "db_writer_dlq".to_string())
    });

    // This is the single, correct block for creating the consumer group
    let result: redis::RedisResult<()> = conn
        .xgroup_create_mkstream(stream_key, group_name, "0")
        .await;

    if let Err(e) = result {
        if !e.to_string().contains("BUSYGROUP") {
            return Err(anyhow!("Failed to create DB writer consumer group: {}", e));
        }
        println!(
            "[DB Writer] Consumer group '{}' already exists. Resuming.",
            group_name
        );
    } else {
        println!(
            "[DB Writer] Created new consumer group '{}' on stream '{}'.",
            group_name, stream_key
        );
    }

    let opts = StreamReadOptions::default()
        .group(group_name, &consumer_name)
        .count(read_count)
        .block(5000);

    loop {
        let reply: StreamReadReply = match conn.xread_options(&[stream_key], &[">"], &opts).await {
            Ok(reply) => reply,
            Err(e) => {
                eprintln!(
                    "[DB Writer] 🔴 Error reading from Redis Stream: {}. Retrying...",
                    e
                );
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        if reply.keys.is_empty() {
            continue; // No new messages, loop again.
        }

        let mut message_ids_to_ack = Vec::new();
        let mut trades: Vec<TradeRow> = Vec::new();
        let mut transfers: Vec<TransferRow> = Vec::new();
        let mut mints: Vec<MintRow> = Vec::new();
        let mut burns: Vec<BurnRow> = Vec::new();
        let mut pools: Vec<PoolCreationRow> = Vec::new();
        let mut liquidity: Vec<LiquidityRow> = Vec::new();

        let mut migrations: Vec<MigrationRow> = Vec::new();
        let mut fee_collections: Vec<FeeCollectionRow> = Vec::new();
        let mut supply_locks: Vec<SupplyLockRow> = Vec::new();
        let mut supply_lock_actions: Vec<SupplyLockActionRow> = Vec::new();
        let mut payloads_for_retry: Vec<Vec<u8>> = Vec::new();

        // Deserialization logic remains the same...
        for stream_entry in &reply.keys {
            for message in &stream_entry.ids {
                message_ids_to_ack.push(message.id.clone());
                if let Some(payload_value) = message.map.get("payload") {
                    if let Ok(payload_bytes) = Vec::<u8>::from_redis_value(payload_value) {
                        payloads_for_retry.push(payload_bytes.clone());
                        if let Ok(payload) = bincode::deserialize::<EventPayload>(&payload_bytes) {
                            match payload.event {
                                EventType::Trade(row) => trades.push(row),
                                EventType::Transfer(row) => transfers.push(row),
                                EventType::Mint(row) => mints.push(row),
                                EventType::Burn(row) => burns.push(row),
                                EventType::PoolCreation(row) => pools.push(row),
                                EventType::Liquidity(row) => liquidity.push(row),
                                EventType::Migration(row) => migrations.push(row),
                                EventType::FeeCollection(row) => fee_collections.push(row),
                                EventType::SupplyLock(row) => supply_locks.push(row),
                                EventType::SupplyLockAction(row) => supply_lock_actions.push(row),
                            }
                        }
                    }
                }
            }
        }

        // --- RESILIENT INSERTION LOGIC ---
        let mut all_inserts_ok = true;
        if let Err(e) = insert_rows(&db_client, "trades", trades, "DB Writer", "trade").await {
            all_inserts_ok = false;
            eprintln!("[DB Writer] 🔴 FAILED to insert trades: {}", e);
        }
        if let Err(e) =
            insert_rows(&db_client, "transfers", transfers, "DB Writer", "transfer").await
        {
            all_inserts_ok = false;
            eprintln!("[DB Writer] 🔴 FAILED to insert transfers: {}", e);
        }
        if let Err(e) = insert_rows(&db_client, "mints", mints, "DB Writer", "mint").await {
            all_inserts_ok = false;
            eprintln!("[DB Writer] 🔴 FAILED to insert mints: {}", e);
        }
        if let Err(e) = insert_rows(&db_client, "burns", burns, "DB Writer", "burn").await {
            all_inserts_ok = false;
            eprintln!("[DB Writer] 🔴 FAILED to insert burns: {}", e);
        }
        if let Err(e) = insert_rows(
            &db_client,
            "pool_creations",
            pools,
            "DB Writer",
            "pool_creation",
        )
        .await
        {
            all_inserts_ok = false;
            eprintln!("[DB Writer] 🔴 FAILED to insert pool_creations: {}", e);
        }
        if let Err(e) =
            insert_rows(&db_client, "liquidity", liquidity, "DB Writer", "liquidity").await
        {
            all_inserts_ok = false;
            eprintln!("[DB Writer] 🔴 FAILED to insert liquidity: {}", e);
        }
        if let Err(e) = insert_rows(
            &db_client,
            "migrations",
            migrations,
            "DB Writer",
            "migration",
        )
        .await
        {
            all_inserts_ok = false;
            eprintln!("[DB Writer] 🔴 FAILED to insert migrations: {}", e);
        }
        if let Err(e) = insert_rows(
            &db_client,
            "fee_collections",
            fee_collections,
            "DB Writer",
            "fee_collection",
        )
        .await
        {
            all_inserts_ok = false;
            eprintln!("[DB Writer] 🔴 FAILED to insert fee collections: {}", e);
        }
        if let Err(e) = insert_rows(
            &db_client,
            "supply_locks",
            supply_locks,
            "DB Writer",
            "supply_lock",
        )
        .await
        {
            all_inserts_ok = false;
            eprintln!("[DB Writer] 🔴 FAILED to insert supply locks: {}", e);
        }
        if let Err(e) = insert_rows(
            &db_client,
            "supply_lock_actions",
            supply_lock_actions,
            "DB Writer",
            "supply_lock_action",
        )
        .await
        {
            all_inserts_ok = false;
            eprintln!("[DB Writer] 🔴 FAILED to insert supply lock actions: {}", e);
        }

        if !all_inserts_ok {
            let mut dlq_failed = false;
            for payload in payloads_for_retry {
                let payload_entry = [("payload", payload)];
                if let Err(e) = conn
                    .xadd::<_, _, _, _, ()>(&*DB_WRITER_DLQ_STREAM, "*", &payload_entry)
                    .await
                {
                    dlq_failed = true;
                    eprintln!(
                        "[DB Writer] 🔴 FAILED to write payload to DLQ after insert error: {}",
                        e
                    );
                    break;
                }
            }
            if dlq_failed {
                eprintln!(
                    "[DB Writer] ⚠️ Leaving messages pending because DLQ write failed. Manual intervention required."
                );
                continue;
            }
        }

        // Only acknowledge once data is safely persisted or requeued.
        if !message_ids_to_ack.is_empty() {
            let result: redis::RedisResult<()> =
                conn.xack(stream_key, group_name, &message_ids_to_ack).await;
            if let Err(e) = result {
                eprintln!(
                    "[DB Writer] 🔴 FAILED to acknowledge messages in Redis: {}",
                    e
                );
            } else if let Err(e) = conn
                .xdel::<_, _, i64>(stream_key, &message_ids_to_ack)
                .await
            {
                eprintln!("[DB Writer] 🔴 FAILED to delete messages in Redis: {}", e);
            }
        }
    }
}

async fn redis_publisher_service(
    mut mpsc_receiver: mpsc::Receiver<EventPayload>,
    redis_client: RedisClient,
    max_stream_len: usize,
    backfill_backlog_limit: usize,
) -> Result<()> {
    println!("[Redis Publisher] Service started.");
    let stream_key = "event_queue";
    let stream_maxlen = if max_stream_len > 0 {
        Some(StreamMaxlen::Approx(max_stream_len))
    } else {
        None
    };

    let mut conn = redis_client.get_multiplexed_async_connection().await?;

    while let Some(payload) = mpsc_receiver.recv().await {
        let payload_data = match bincode::serialize(&payload) {
            Ok(data) => data,
            Err(e) => {
                eprintln!("[Redis Publisher] Failed to serialize payload: {}", e);
                continue;
            }
        };

        let payload_entry = [("payload", payload_data)];

        // Block/retry until the payload is written; do not drop events on timeout.
        loop {
            // In backfill mode, we don't use Redis truncation; enforce a soft backlog cap via blocking.
            if backfill_backlog_limit > 0 && stream_maxlen.is_none() {
                match conn.xlen::<_, usize>(stream_key).await {
                    Ok(len) if len as usize >= backfill_backlog_limit => {
                        eprintln!(
                            "[Redis Publisher] Backlog at {} (limit {}). Throttling producer...",
                            len, backfill_backlog_limit
                        );
                        sleep(Duration::from_millis(200)).await;
                        continue;
                    }
                    Ok(_) => {}
                    Err(e) => {
                        eprintln!(
                            "[Redis Publisher] Failed to read backlog length: {}. Will retry.",
                            e
                        );
                        sleep(Duration::from_millis(200)).await;
                        continue;
                    }
                }
            }

            let write_result = if let Some(maxlen) = &stream_maxlen {
                conn.xadd_maxlen::<_, _, _, _, ()>(stream_key, maxlen.clone(), "*", &payload_entry)
                    .await
            } else {
                conn.xadd::<_, _, _, _, ()>(stream_key, "*", &payload_entry)
                    .await
            };

            match write_result {
                Ok(()) => break,
                Err(e) => {
                    eprintln!(
                        "[Redis Publisher] XADD failed (will retry, payload retained): {}",
                        e
                    );
                    // Attempt to reconnect before retrying the same payload.
                    loop {
                        match redis_client.get_multiplexed_async_connection().await {
                            Ok(new_conn) => {
                                conn = new_conn;
                                break;
                            }
                            Err(err) => {
                                eprintln!(
                                    "[Redis Publisher] Reconnect attempt failed: {}. Retrying...",
                                    err
                                );
                                sleep(Duration::from_secs(1)).await;
                            }
                        }
                    }
                }
            }
        }
    }
    // Channel closed: this is fatal because upstream is gone; exit to avoid silent data loss.
    eprintln!("[Redis Publisher] 🔴 Input channel closed. Exiting.");
    std::process::exit(1);
    Ok(())
}

async fn redis_queue_monitor(
    redis_client: RedisClient,
    link_graph_depths: Vec<Arc<AtomicUsize>>,
    writer_depth: Arc<AtomicUsize>,
    tx_receiver: Arc<Mutex<mpsc::Receiver<UnifiedTransaction<'static>>>>,
) -> Result<()> {
    let mut conn = redis_client.get_multiplexed_async_connection().await?;
    let mut ticker = interval(Duration::from_secs(10));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let queues = vec!["event_queue", "wallet_agg_queue"];

    loop {
        ticker.tick().await;
        let mut sizes = Vec::new();
        for key in &queues {
            match conn.xlen::<_, usize>(key).await {
                Ok(len) => sizes.push(format!("{}={}", key, len)),
                Err(e) => sizes.push(format!("{}=err({})", key, e)),
            }
        }
        let link_graph_len: usize = link_graph_depths
            .iter()
            .map(|d| d.load(Ordering::Relaxed))
            .sum();
        let writer_len: usize = writer_depth.load(Ordering::Relaxed);
        let tx_len = match tx_receiver.try_lock() {
            Ok(rx) => rx.len(),
            Err(_) => {
                // Avoid blocking transaction processors; report as busy.
                usize::MAX
            }
        };
        sizes.push(format!("link_graph_channel={}", link_graph_len));
        sizes.push(format!("link_graph_writer_channel={}", writer_len));
        if tx_len == usize::MAX {
            sizes.push("tx_channel=busy".to_string());
        } else {
            sizes.push(format!("tx_channel={}", tx_len));
        }
        println!("[RedisMonitor] Queue sizes: {}", sizes.join(", "));
    }
}

async fn initialize_price_service() -> Result<(PriceService, JoinHandle<Result<()>>)> {
    println!("[Main] Initializing Price Service...");

    // PriceService::new() will block here until the API call is complete.
    let price_service = PriceService::new()
        .await
        .expect("FATAL: Could not initialize Price Service.");

    println!("[Main] ✅ Price Service is ready. Spawning updater task.");

    // Spawn the background updater task.
    let price_thread = tokio::spawn(price_updater_task(price_service.clone()));

    Ok((price_service, price_thread))
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().expect("Failed to read .env file");

    // --- Configuration ---
    let debug_mode = env::var("DEBUG").unwrap_or_else(|_| "false".to_string()) == "true";
    let backfill_mode = env::var("BACKFILL_MODE").unwrap_or_else(|_| "false".to_string()) == "true";
    let simulator_mode =
        env::var("SIMULATOR_MODE").unwrap_or_else(|_| "false".to_string()) == "true";
    let geyser_endpoint = env::var("GEYSER_ENDPOINT").expect("GEYSER_ENDPOINT must be set");
    let geyser_api_key = env::var("GEYSER_API_KEY").expect("GEYSER_API_KEY must be set");
    let rpc_url = env::var("SOLANA_RPC_URL").expect("Solana rpc url not found");
    let ws_url = env::var("WS_RPC_ENDPOINT").expect("WS_RPC_ENDPOINT must be set");
    let db_url = env::var("CLICKHOUSE_URL").expect("CLICKHOUSE_URL must be set");
    let neo4j_uri = env::var("NEO4J_URI").expect("NEO4J_URI must be set");

    // --- Client Initialization ---
    let client_with_url = Client::default()
        .with_url(&db_url)
        .with_option("max_bytes_before_external_sort", "1000000000")     // 3 GB
        .with_option("max_bytes_before_external_group_by", "1000000000") // 3 GB
        .with_option("max_threads", "4") // Good limitation for concurrency
        .with_option("max_memory_usage", "0"); // 0 = Unlimited (let the spill settings handle safety)
    let db_client = client_with_url.with_user("default").with_password("");

    db_client.query("SELECT 1").fetch_one::<u8>().await?;
    println!(" Connection to ClickHouse successful.");

    let redis_url = env::var("REDIS_URL").expect("REDIS_URL must be set");
    let redis_client = RedisClient::open(redis_url)?;
    println!(" Redis client initialized.");

    let rpc_client = Arc::new(RpcClient::new(rpc_url));
    println!(" Solana RPC client initialized.");

    let neo4j_client = Arc::new(Graph::new(&neo4j_uri, "neo4j", "neo4j123").await?);
    println!(" Connection to Neo4j successful.");

    // --- Channel for our NEW UnifiedTransaction type ---
    let tx_channel_capacity = channel_capacity_from_env("TX_CHANNEL_CAPACITY", 4096);
    let event_channel_capacity = channel_capacity_from_env("EVENT_CHANNEL_CAPACITY", 4096);
    // In backfill we disable Redis truncation to avoid losing older events.
    let redis_stream_max_len = if backfill_mode {
        0
    } else {
        channel_capacity_from_env("REDIS_STREAM_MAX_LEN", 50_000)
    };
    // When backfilling, enforce a bounded backlog by throttling publisher instead of truncating.
    let redis_backfill_backlog_limit = if backfill_mode {
        channel_capacity_from_env("REDIS_BACKFILL_BACKLOG_LIMIT", 5_000_000)
    } else {
        0
    };
    println!(
        "[Main] 🧮 Channel capacities -> tx: {}, events: {}. Redis stream max len: {}",
        tx_channel_capacity, event_channel_capacity, redis_stream_max_len
    );
    let (tx_sender, tx_receiver) = mpsc::channel(tx_channel_capacity);
    let (event_sender, event_receiver) = mpsc::channel::<EventPayload>(event_channel_capacity);
    let link_graph_channel_capacity =
        channel_capacity_from_env("LINK_GRAPH_CHANNEL_CAPACITY", 4096);
    let link_graph_workers = channel_capacity_from_env("LINK_GRAPH_WORKERS", 1);
    let writer_channel_capacity =
        channel_capacity_from_env("LINK_GRAPH_WRITER_CHANNEL_CAPACITY", 20_000);
    let (link_graph_write_sender, link_graph_write_receiver) =
        mpsc::channel::<WriteJob>(writer_channel_capacity);
    let link_graph_writer_depth = Arc::new(AtomicUsize::new(0));
    let mut link_graph_senders = Vec::with_capacity(link_graph_workers);
    let mut link_graph_receivers = Vec::with_capacity(link_graph_workers);
    let mut link_graph_depths = Vec::with_capacity(link_graph_workers);
    for _ in 0..link_graph_workers {
        let (tx, rx) = mpsc::channel::<EventPayload>(link_graph_channel_capacity);
        link_graph_senders.push(tx);
        link_graph_receivers.push(rx);
        link_graph_depths.push(Arc::new(AtomicUsize::new(0)));
    }
    let shared_tx_receiver = Arc::new(Mutex::new(tx_receiver));

    // --- Background Tasks ---

    let mut tasks = vec![];
    let (price_service, price_thread) = initialize_price_service().await?;
    tasks.push(price_thread);

    let monitor_redis_client = redis_client.clone();
    let monitor_depths = link_graph_depths.clone();
    let monitor_writer_depth = link_graph_writer_depth.clone();
    let monitor_tx_receiver = shared_tx_receiver.clone();
    let monitor_task = tokio::spawn(redis_queue_monitor(
        monitor_redis_client,
        monitor_depths,
        monitor_writer_depth,
        monitor_tx_receiver,
    ));
    tasks.push(monitor_task);

    let publisher_redis_client = redis_client.clone();
    let publisher_task = tokio::spawn(redis_publisher_service(
        event_receiver,
        publisher_redis_client,
        redis_stream_max_len,
        redis_backfill_backlog_limit,
    ));
    tasks.push(publisher_task);
    println!("[Main] 🚀 Redis publisher service spawned.");

    let db_writer_workers = channel_capacity_from_env("DB_WRITER_WORKERS", 1);
    let db_writer_read_count = channel_capacity_from_env("DB_WRITER_READ_COUNT", 5000);
    for worker_id in 0..db_writer_workers {
        let writer_db_client = db_client.clone();
        let writer_redis_client = redis_client.clone();
        let read_count = db_writer_read_count;
        let handle = tokio::spawn(async move {
            println!(
                "[Main] 🚀 Database (Raw Events) writer service worker {} spawned with batch {}.",
                worker_id, read_count
            );
            if let Err(e) =
                database_writer_service(writer_db_client, writer_redis_client, read_count).await
            {
                eprintln!(
                    "[Main] ❌ Database writer worker {} failed: {}",
                    worker_id, e
                );
            }
            Result::<()>::Ok(())
        });
        tasks.push(handle);
    }

    let wallet_agg_workers = channel_capacity_from_env("WALLET_AGG_WORKERS", 1);
    for worker_id in 0..wallet_agg_workers {
        let aggregator_db_client = db_client.clone();
        let aggregator_redis_client = redis_client.clone();
        let wallet_aggregator_price_service = price_service.clone();
        let link_graph_senders_clone = link_graph_senders.clone();
        let link_graph_depths_clone = link_graph_depths.clone();
        let lg_worker_count = link_graph_workers;
        let handle = tokio::spawn(async move {
            match aggregator::wallet_stats::WalletAggregator::new(
                aggregator_db_client,
                aggregator_redis_client,
                wallet_aggregator_price_service,
                link_graph_senders_clone,
                link_graph_depths_clone,
                lg_worker_count,
            )
            .await
            {
                Ok(mut aggregator) => {
                    println!(
                        "[Main] 🚀 Wallet stats aggregator worker {} spawned.",
                        worker_id
                    );
                    if let Err(e) = aggregator.run().await {
                        eprintln!(
                            "[Main] ❌ WalletAggregator worker {} failed: {}",
                            worker_id, e
                        );
                    }
                }
                Err(e) => {
                    eprintln!(
                        "[Main] ❌ Failed to create WalletAggregator worker {}: {}",
                        worker_id, e
                    );
                }
            }
            Result::<()>::Ok(())
        });
        tasks.push(handle);
    }

    let token_agg_workers = channel_capacity_from_env("TOKEN_AGG_WORKERS", 1);
    for worker_id in 0..token_agg_workers {
        let token_agg_db_client = db_client.clone();
        let token_agg_redis_client = redis_client.clone();
        let token_agg_rpc_client = Arc::clone(&rpc_client);
        let token_agg_price_service = price_service.clone();
        let handle = tokio::spawn(async move {
            match TokenAggregator::new(
                token_agg_db_client,
                token_agg_redis_client,
                token_agg_rpc_client,
                token_agg_price_service,
            )
            .await
            {
                Ok(mut token_aggregator) => {
                    println!(
                        "[Main] 🚀 Token stats aggregator worker {} spawned.",
                        worker_id
                    );
                    if let Err(e) = token_aggregator.run().await {
                        eprintln!(
                            "[Main] ❌ TokenAggregator worker {} failed: {}",
                            worker_id, e
                        );
                    }
                }
                Err(e) => {
                    eprintln!(
                        "[Main] ❌ Failed to create TokenAggregator worker {}: {}",
                        worker_id, e
                    );
                }
            }
            Result::<()>::Ok(())
        });
        tasks.push(handle);
    }

    let programs_to_monitor = vec![
        "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P".to_string(), // Pump.fun Launchpad
        "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA".to_string(), // Pump.fun AMM
        "strmRqUCoQUgGUan5YhzUZa6KqdzwX5L6FpUxfmKg5m".to_string(), // Streamflow
        "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_string(), // SPL Token Program
        "11111111111111111111111111111111".to_string(),
    ];

    // Spawn transaction processor workers
    let tx_processor_workers = channel_capacity_from_env("TX_PROCESSOR_WORKERS", 2);
    // Spawn single global link-graph writer
    {
        let writer_client = Arc::clone(&neo4j_client);
        let writer_depth = link_graph_writer_depth.clone();
        let writer_rx = link_graph_write_receiver;
        let writer_task = tokio::spawn(async move {
            LinkGraph::writer_task(writer_rx, writer_client, writer_depth).await;
            Result::<()>::Ok(())
        });
        tasks.push(writer_task);
    }
    for worker_id in 0..tx_processor_workers {
        // Recreate handlers per worker
        let protocol_handlers: Vec<Box<dyn TransactionHandler>> = vec![
            Box::new(PumpFunAmmHandler::new()?),
            Box::new(PumpFunLaunchpadHandler::new()?),
            // Box::new(RaydiumLaunchpadHandler::new()?),
            // Box::new(RaydiumCpmmHandler::new()?),
            Box::new(StreamflowHandler::new()?),
        ];

        let solana_handlers: Vec<Box<dyn TransactionHandler>> = vec![
            Box::new(SplTokenHandler::new()?),
            Box::new(SystemHandler::new()?),
        ];

        let tx_receiver_clone = Arc::clone(&shared_tx_receiver);
        let price_clone = price_service.clone();
        let event_sender_clone = event_sender.clone();

        let handle = tokio::spawn(transaction_processor(
            tx_receiver_clone,
            protocol_handlers,
            solana_handlers,
            price_clone,
            event_sender_clone,
            worker_id,
        ));
        tasks.push(handle);
    }

    if simulator_mode {
        // --- SIMULATOR MODE ---
        println!("[Main] 🧪 Mode: SIMULATOR. Spawning simulator task.");
        let sim_handle = tokio::spawn(simulator::simulator_task(
            redis_client.clone(),
            "scenario.json",
        ));
        tasks.push(sim_handle);
    } else if backfill_mode {
        println!("[Main] 🚀 Mode: BACKFILL. Starting CAR file subscriber.");
        let car_file_path =
            env::var("CAR_FILE_PATH").expect("CAR_FILE_PATH must be set in .env for backfill mode");

        // --- THE ONLY CHANGE IS HERE ---
        // Pass the programs_to_monitor vector to the subscriber
        let subscriber_task = tokio::spawn(car_file_subscriber(
            car_file_path,
            programs_to_monitor, // Pass the list of programs
            tx_sender,
        ));
        tasks.push(subscriber_task);
    } else if debug_mode {
        println!("[Main] Mode: DEBUG. Starting WebSocket subscriber.");
        let subscriber_task = tokio::spawn(websocket_subscriber(
            ws_url,
            Arc::clone(&rpc_client),
            programs_to_monitor,
            tx_sender,
        ));
        tasks.push(subscriber_task);
    } else {
        println!("[Main] Mode: PRODUCTION. Starting Geyser subscriber.");
        let subscriber_task = tokio::spawn(geyser_subscriber(
            geyser_endpoint,
            geyser_api_key,
            programs_to_monitor,
            tx_sender,
        ));

        tasks.push(subscriber_task);
    };

    // --- SPAWN THE WALLET GRAPH AGGREGATOR THREADS ---
    for (worker_id, graph_rx) in link_graph_receivers.into_iter().enumerate() {
        let graph_neo4j_client = Arc::clone(&neo4j_client);
        let graph_db_client = db_client.clone();
        let graph_depth = link_graph_depths[worker_id % link_graph_depths.len()].clone();
        let writer_depth = link_graph_writer_depth.clone();
        let write_sender = link_graph_write_sender.clone();
        let handle = tokio::spawn(async move {
            match LinkGraph::new(
                graph_db_client,
                graph_neo4j_client,
                graph_rx,
                graph_depth,
                write_sender,
                writer_depth,
            )
            .await
            {
                Ok(mut graph_aggregator) => {
                    println!(
                        "[Main] 🚀 Wallet graph aggregator worker {} spawned.",
                        worker_id
                    );
                    if let Err(e) = graph_aggregator.run().await {
                        eprintln!(
                            "[Main] ❌ Wallet graph aggregator worker {} failed: {}",
                            worker_id, e
                        );
                    }
                }
                Err(e) => {
                    eprintln!(
                        "[Main] ❌ Failed to create WalletGraph aggregator worker {}: {}",
                        worker_id, e
                    );
                }
            }
            Result::<()>::Ok(())
        });
        tasks.push(handle);
    }

    // Wait for the primary tasks to complete
    for handle in tasks {
        if let Err(e) = handle.await {
            eprintln!("A critical task panicked or returned an error: {}", e);
        }
    }

    Ok(())
}
