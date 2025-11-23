// File: src/main.rs

#![allow(warnings)]

use tokio::time::{self, Instant};
use anyhow::Result;
use backoff::{future::retry, ExponentialBackoff};
use clickhouse::Client;
use dotenvy::dotenv;
use futures_util::StreamExt;
use solana_sdk::{
    hash::Hash,
    instruction::CompiledInstruction,
    message::{Message, VersionedMessage},
    pubkey::Pubkey,
    signature::Signature,
    transaction::VersionedTransaction,
};
use yellowstone_grpc_proto::prelude::SubscribeUpdateTransaction;

use yellowstone_grpc_proto::prelude::{
    TransactionStatusMeta,
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
    SubscribeRequestFilterTransactions,
};
use std::{
    collections::HashMap,
    env,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::mpsc;
use tonic::transport::ClientTlsConfig;
use yellowstone_grpc_client::GeyserGrpcClient;




async fn debug_subscriber(
    endpoint: String,
    api_key: String,
) -> anyhow::Result<()> {
    println!("👂 Starting Geyser throughput test...");

    retry(ExponentialBackoff::default(), || async {
        let mut client = GeyserGrpcClient::build_from_shared(endpoint.clone())
            .map_err(|e| backoff::Error::permanent(anyhow::Error::from(e)))?
            .x_token(Some(api_key.clone()))
            .map_err(|e| backoff::Error::permanent(anyhow::Error::from(e)))?
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(10))
            .tls_config(ClientTlsConfig::new().with_native_roots())
            .map_err(|e| backoff::Error::permanent(anyhow::Error::from(e)))?
            .max_decoding_message_size(1024 * 1024 * 1024)
            .connect()
            .await
            .map_err(|e| backoff::Error::transient(anyhow::Error::from(e)))?;
        println!("✅ Geyser client connected to {}", endpoint);

        let mut transactions_filter = HashMap::new();
        transactions_filter.insert(
            "all_programs".to_string(),
            SubscribeRequestFilterTransactions {
                vote: Some(false),
                failed: Some(true),
                account_include: vec![],
                ..Default::default()
            },
        );
        let request = SubscribeRequest {
            transactions: transactions_filter,
            commitment: Some(CommitmentLevel::Processed as i32),
            ..Default::default()
        };

        let (_unsubscribe, mut stream) = client
            .subscribe_with_request(Some(request))
            .await
            .map_err(|e| backoff::Error::transient(anyhow::Error::from(e)))?;
        println!("✅ Geyser subscription successful. Starting to count messages...");

        let mut counter = 0u64;
        let mut last_log_time = Instant::now();
        while let Some(message) = stream.next().await {
           
            counter += 1;


            if counter % 1000 == 0 {
                let elapsed_time = last_log_time.elapsed();
                let messages_per_second = 1000.0 / elapsed_time.as_secs_f64();
                println!(
                    "[GEYSER COUNT] Received {} messages so far. Last 1000 messages took {:.2?} at {:.2} messages/s",
                    counter, elapsed_time, messages_per_second
                );
                last_log_time = Instant::now();
            }
        }

        Err(backoff::Error::transient(anyhow::anyhow!(
            "Geyser stream ended unexpectedly after {} messages.", counter
        )))
    })
    .await
}



#[tokio::main]
async fn main() -> Result<()> {
    dotenv().expect("Failed to read .env file");

    let geyser_endpoint = env::var("GEYSER_ENDPOINT").expect("GEYSER_ENDPOINT must be set");
    let geyser_api_key = env::var("GEYSER_API_KEY").expect("GEYSER_API_KEY must be set");

    let debug_subscriber_handle = tokio::spawn(debug_subscriber(
        geyser_endpoint,
        geyser_api_key,
    ));
    let _ = tokio::try_join!( debug_subscriber_handle);
    Ok(())
}