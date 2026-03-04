use anyhow::{Context, Result, anyhow};
use chrono::prelude::*;
use reqwest::header::{HeaderMap, HeaderValue, USER_AGENT};
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

// --- Structs to parse CoinGecko API responses ---
#[derive(Deserialize, Debug)]
struct LivePriceResponse {
    solana: UsdPrice,
}
#[derive(Deserialize, Debug)]
struct UsdPrice {
    usd: f64,
}

#[derive(Deserialize, Debug)]
struct HistoricalPriceResponse {
    prices: Vec<[f64; 2]>,
}

// --- The New PriceService ---
#[derive(Clone)]
pub struct PriceService {
    live_price_usd: Arc<RwLock<f64>>,
    historical_prices: Arc<BTreeMap<u32, f64>>,
    backfill_mode: bool,
}

impl PriceService {
    /// Creates a new PriceService instance.
    /// All network-intensive operations are performed here, ONCE at startup.
    pub async fn new() -> Result<Self> {
        let backfill_mode =
            env::var("BACKFILL_MODE").unwrap_or_else(|_| "false".to_string()) == "true";

        let (initial_price, historical_prices) = if backfill_mode {
            println!("[PriceService] Backfill mode enabled. Fetching historical prices...");
            let history = fetch_historical_prices().await?;
            let last_price = history.values().last().cloned().unwrap_or(0.0);
            (last_price, history)
        } else {
            println!("[PriceService] Live mode enabled. Fetching current price...");
            let live_price = fetch_live_native_price().await?;
            (live_price, BTreeMap::new())
        };

        println!("[PriceService] Initialized with price: ${}", initial_price);

        Ok(Self {
            live_price_usd: Arc::new(RwLock::new(initial_price)),
            historical_prices: Arc::new(historical_prices),
            backfill_mode,
        })
    }

    /// Returns the appropriate price for a given UNIX timestamp.
    /// This function is designed for high-frequency calls and contains no I/O.
    pub async fn get_price(&self, timestamp: u32) -> f64 {
        if self.backfill_mode {
            // This is a fast, synchronous, in-memory lookup.
            self.historical_prices
                .range(..=timestamp)
                .next_back()
                .map(|(_, &price)| price)
                .unwrap_or(0.0)
        } else {
            // This is a non-blocking, asynchronous read lock.
            // It's async to prevent stalling the worker thread if the lock is held for writing.
            *self.live_price_usd.read().await
        }
    }
}

/// The background task that ONLY runs in live mode to update the price.
/// It now returns a Result to satisfy the JoinHandle's expected type.
pub async fn price_updater_task(price_service: PriceService) -> Result<()> {
    // This task should not run in backfill mode.
    if price_service.backfill_mode {
        println!("[PriceService] Updater task skipped in backfill mode.");
        return Ok(()); // Immediately return Ok.
    }

    loop {
        tokio::time::sleep(Duration::from_secs(60)).await;
        match fetch_live_native_price().await {
            Ok(price) => {
                let mut price_lock = price_service.live_price_usd.write().await;
                *price_lock = price;
                println!("[PriceService] Updated live native price: ${}", price);
            }
            Err(e) => {
                // We log the error but continue the loop, as this is not a fatal error for the service.
                eprintln!("[PriceService] Failed to fetch live native price: {:?}", e);
            }
        }
    }

    // This code is unreachable but satisfies the function's return type.
    #[allow(unreachable_code)]
    Ok(())
}

// --- Helper Functions (No changes needed below) ---

fn coingecko_base_url() -> String {
    env::var("COINGECKO_BASE_URL").unwrap_or_else(|_| "https://api.coingecko.com/api/v3".to_string())
}

fn coingecko_client() -> Result<reqwest::Client> {
    let mut headers = HeaderMap::new();

    if let Ok(key) = env::var("COINGECKO_PRO_API_KEY") {
        if !key.trim().is_empty() {
            headers.insert(
                "x-cg-pro-api-key",
                HeaderValue::from_str(key.trim()).context("Invalid COINGECKO_PRO_API_KEY")?,
            );
        }
    }

    if let Ok(key) = env::var("COINGECKO_DEMO_API_KEY") {
        if !key.trim().is_empty() {
            headers.insert(
                "x-cg-demo-api-key",
                HeaderValue::from_str(key.trim()).context("Invalid COINGECKO_DEMO_API_KEY")?,
            );
        }
    }

    headers.insert(
        USER_AGENT,
        HeaderValue::from_static("solana-data-api/price_service"),
    );

    Ok(reqwest::Client::builder()
        .default_headers(headers)
        .timeout(Duration::from_secs(120))
        .build()?)
}

fn body_snippet(body: &str) -> String {
    const MAX: usize = 1000;
    if body.len() <= MAX {
        body.to_string()
    } else {
        format!("{}...(truncated)", &body[..MAX])
    }
}

async fn get_text_checked(client: &reqwest::Client, url: &str) -> Result<String> {
    let resp = client.get(url).send().await?;
    let status = resp.status();
    let text = resp.text().await?;
    if !status.is_success() {
        return Err(anyhow!(
            "CoinGecko HTTP {} for {}. Body: {}",
            status,
            url,
            body_snippet(&text)
        ));
    }
    Ok(text)
}

/// Fetches the live price of SOL.
async fn fetch_live_native_price() -> Result<f64> {
    let client = coingecko_client()?;
    let url = format!(
        "{}/simple/price?ids=solana&vs_currencies=usd",
        coingecko_base_url()
    );
    let text = get_text_checked(&client, &url).await?;
    let response: LivePriceResponse = serde_json::from_str(&text)
        .with_context(|| format!("Failed to parse CoinGecko live price response. Body: {}", body_snippet(&text)))?;
    Ok(response.solana.usd)
}

/// Fetches historical SOL prices from CoinGecko for the configured date range.
async fn fetch_historical_prices() -> Result<BTreeMap<u32, f64>> {
    let start_date_str = env::var("BACKFILL_START_DATE").map_err(|_| {
        anyhow!("BACKFILL_START_DATE must be set in .env for backfill mode (YYYY-MM-DD)")
    })?;
    let end_date_str = env::var("BACKFILL_END_DATE").map_err(|_| {
        anyhow!("BACKFILL_END_DATE must be set in .env for backfill mode (YYYY-MM-DD)")
    })?;

    let start_ts = NaiveDate::parse_from_str(&start_date_str, "%Y-%m-%d")?
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .timestamp();
    let end_ts = NaiveDate::parse_from_str(&end_date_str, "%Y-%m-%d")?
        .and_hms_opt(23, 59, 59)
        .unwrap()
        .timestamp();

    let client = coingecko_client()?;
    let url = format!(
        "{}/coins/solana/market_chart/range?vs_currency=usd&from={}&to={}",
        coingecko_base_url(),
        start_ts,
        end_ts
    );

    let text = get_text_checked(&client, &url).await?;
    let value: Value = serde_json::from_str(&text).with_context(|| {
        format!(
            "CoinGecko historical price endpoint returned non-JSON. Body: {}",
            body_snippet(&text)
        )
    })?;

    if value.get("prices").is_none() {
        return Err(anyhow!(
            "CoinGecko historical price response missing 'prices'. This often means auth/rate-limit/format change. Body: {}",
            body_snippet(&text)
        ));
    }

    let response: HistoricalPriceResponse = serde_json::from_value(value).with_context(|| {
        format!(
            "Failed to parse CoinGecko historical price response. Body: {}",
            body_snippet(&text)
        )
    })?;

    let mut price_map = BTreeMap::new();
    for price_point in response.prices {
        let timestamp_sec = (price_point[0] / 1000.0) as u32;
        price_map.insert(timestamp_sec, price_point[1]);
    }

    println!(
        "[PriceService] ✅ Fetched and cached {} historical price points.",
        price_map.len()
    );
    Ok(price_map)
}
