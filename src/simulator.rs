//! src/simulator.rs

use anyhow::{Result, anyhow};
use clickhouse::Client;
use futures::stream::{self, StreamExt};
use redis::{AsyncCommands, Client as RedisClient};
use serde::Deserialize;
use serde_json::Value;
use std::{collections::HashMap, fs, str::FromStr, time::Duration};

use crate::{
    database::insert_rows,
    types::{
        BurnRow, EventPayload, EventType, LiquidityRow, MintRow, PoolCreationRow, SupplyLockRow,
        TokenBalanceMap, TradeRow, TransferRow, WalletHoldingRow,
    },
};

#[derive(Deserialize, Debug, Clone)]
struct Scenario {
    time: u64,
    event_type: String,
    data: Value,
    // ADDED: New optional fields for contextual data
    #[serde(default)]
    balances: HashMap<String, HashMap<String, String>>, // Read balances as strings
    #[serde(default)]
    token_decimals: HashMap<String, u8>,
}

/// A single-threaded simulator that reads a scenario file and injects events
/// into the databases at the correct time using a ticker loop.
pub async fn simulator_task(redis_client: RedisClient, scenario_file: &str) -> Result<()> {
    println!("🧪 Starting Simulator from file: {}", scenario_file);
    let mut redis_conn = redis_client.get_multiplexed_async_connection().await?;

    let scenario_data = fs::read_to_string(scenario_file)?;
    let mut scenarios: Vec<Scenario> = serde_json::from_str(&scenario_data)?;
    scenarios.sort_by_key(|s| s.time);
    println!(
        "🧪 Loaded and sorted {} scenarios. Starting injection loop...",
        scenarios.len()
    );

    let mut last_event_time = 0;

    for scenario in scenarios {
        let delay_needed = scenario.time.saturating_sub(last_event_time);
        if delay_needed > 0 {
            tokio::time::sleep(Duration::from_secs(delay_needed)).await;
        }
        println!("[Simulator] -> Injecting event: {}", scenario.event_type);

        let injection_result = inject_single_event(&mut redis_conn, scenario.clone()).await;
        if let Err(e) = injection_result {
            eprintln!(
                "[Simulator] Error injecting event '{}': {}",
                scenario.event_type, e
            );
        }

        last_event_time = scenario.time;
    }

    println!("✅ Simulator finished injecting all scenarios.");
    Ok(())
}

/// Helper function to handle the injection of a single event.
async fn inject_single_event(
    redis_conn: &mut redis::aio::MultiplexedConnection,
    scenario: Scenario,
) -> Result<()> {
    // Convert the string balances from JSON into the required u64 TokenBalanceMap
    let balances_u64: TokenBalanceMap = scenario
        .balances
        .into_iter()
        .map(|(mint, wallet_balances)| {
            let converted_balances = wallet_balances
                .into_iter()
                .map(|(wallet, balance_str)| (wallet, u64::from_str(&balance_str).unwrap_or(0)))
                .collect();
            (mint, converted_balances)
        })
        .collect();

    // The logic here is now simpler: just build the EventPayload.
    let event_payload = match scenario.event_type.as_str() {
        "Trade" => {
            let row: TradeRow = serde_json::from_value(scenario.data)?;
            // REMOVED: No more direct DB inserts from the simulator.
            EventPayload {
                timestamp: row.timestamp,
                event: EventType::Trade(row),
                balances: balances_u64,
                token_decimals: scenario.token_decimals,
            }
        }
        "Transfer" => {
            let row: TransferRow = serde_json::from_value(scenario.data)?;
            // REMOVED: insert_rows(...)
            EventPayload {
                timestamp: row.timestamp,
                event: EventType::Transfer(row),
                balances: balances_u64,
                token_decimals: scenario.token_decimals,
            }
        }
        "Mint" => {
            let row: MintRow = serde_json::from_value(scenario.data)?;
            // REMOVED: insert_rows(...) for both mints and wallet_holdings
            EventPayload {
                timestamp: row.timestamp,
                event: EventType::Mint(row),
                balances: balances_u64,
                token_decimals: scenario.token_decimals,
            }
        }
        "SupplyLock" => {
            let row: SupplyLockRow = serde_json::from_value(scenario.data)?;
            // REMOVED: insert_rows(...)
            EventPayload {
                timestamp: row.timestamp,
                event: EventType::SupplyLock(row),
                balances: balances_u64,
                token_decimals: scenario.token_decimals,
            }
        }
        "Burn" => {
            let row: BurnRow = serde_json::from_value(scenario.data)?;
            // REMOVED: insert_rows(...)
            EventPayload {
                timestamp: row.timestamp,
                event: EventType::Burn(row),
                balances: balances_u64,
                token_decimals: scenario.token_decimals,
            }
        }
        "PoolCreation" => {
            let row: PoolCreationRow = serde_json::from_value(scenario.data)?;
            EventPayload {
                timestamp: row.timestamp,
                event: EventType::PoolCreation(row),
                balances: balances_u64,
                token_decimals: scenario.token_decimals,
            }
        }
        "Liquidity" => {
            let row: LiquidityRow = serde_json::from_value(scenario.data)?;
            EventPayload {
                timestamp: row.timestamp,
                event: EventType::Liquidity(row),
                balances: balances_u64,
                token_decimals: scenario.token_decimals,
            }
        }
        _ => {
            return Err(anyhow!(
                "Unknown event type in scenario: {}",
                scenario.event_type
            ));
        }
    };

    let payload_data = bincode::serialize(&event_payload)?;

    // CHANGED: Use XADD to publish to the Redis Stream, not LPUSH.
    // This now perfectly mimics the real pipeline.
    let _: () = redis_conn
        .xadd("event_queue", "*", &[("payload", &payload_data)])
        .await?;

    Ok(())
}
