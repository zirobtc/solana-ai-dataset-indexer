use anyhow::{Result, anyhow};
use backoff::{ExponentialBackoff, future::retry};
use base64;
use futures_util::StreamExt;
use oldfaithful_geyser_runner::transaction::Transaction;
use rayon::prelude::*;
use reqwest::StatusCode;
use reqwest::blocking::Client;
use reqwest::header::{ACCEPT_ENCODING, HeaderMap, HeaderValue, RANGE};
use solana_client::{
    nonblocking::{pubsub_client::PubsubClient, rpc_client::RpcClient},
    rpc_config::{RpcTransactionConfig, RpcTransactionLogsConfig, RpcTransactionLogsFilter},
};
use solana_program::example_mocks::solana_sdk::signature;
use solana_sdk::message::MessageHeader;
use solana_sdk::vote::program::ID as VOTE_PROGRAM_ID;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    hash::Hash,
    instruction::CompiledInstruction,
    message::{Message, VersionedMessage},
    pubkey::Pubkey,
    signature::Signature,
    transaction::{TransactionError, VersionedTransaction},
};
use solana_transaction_status::{
    EncodedConfirmedTransactionWithStatusMeta, EncodedTransaction,
    EncodedTransactionWithStatusMeta, TransactionStatusMeta, UiInnerInstructions, UiInstruction,
    UiLoadedAddresses, UiParsedInstruction, UiTransactionEncoding, UiTransactionStatusMeta,
    UiTransactionTokenBalance,
};
use std::collections::HashSet;
use std::io::Cursor;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{
    borrow::Cow,
    collections::HashMap,
    env,
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};

use num_cpus;
use std::io::{self, BufReader, Read};
use tokio::sync::Mutex as TokioMutex; // Use an alias to avoid conflict with std::sync::Mutex
use tokio::sync::mpsc;
use tokio::time::timeout;
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::prelude::{
    CommitmentLevel, InnerInstruction as GrpcInnerInstruction,
    InnerInstructions as GrpcInnerInstructions, SubscribeRequest,
    SubscribeRequestFilterTransactions, SubscribeUpdateTransaction,
    TokenBalance as GrpcTokenBalance, TransactionError as GrpcTransactionError,
    TransactionStatusMeta as GrpcTransactionStatusMeta, UiTokenAmount as GrpcUiTokenAmount,
    subscribe_update::UpdateOneof,
};

use oldfaithful_geyser_runner::{node, utils};
use prost_011::Message as ProstMessage; // Use the aliased prost version
use solana_storage_proto;
use std::convert::{TryFrom, TryInto}; // Needed for metadata conversion

// --- Note: Ensure these use statements are correct for your project layout ---
use crate::types::{BalanceMap, FormattedInstruction, TokenBalanceMap, UnifiedTransaction};
use crate::utils::{build_full_account_keys, format_transaction};

fn channel_capacity_from_env(var: &str, default: usize) -> usize {
    env::var(var)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|cap| *cap > 0)
        .unwrap_or(default)
}

async fn send_with_backpressure<T: Send>(sender: &mpsc::Sender<T>, item: T, label: &str) -> bool {
    match timeout(Duration::from_secs(1), sender.send(item)).await {
        Ok(Ok(())) => true,
        Ok(Err(_)) => {
            eprintln!("{} Receiver dropped. Shutting down sender loop.", label);
            false
        }
        Err(_) => {
            eprintln!(
                "{} Channel full for over 1s. Dropping message to avoid deadlock.",
                label
            );
            false
        }
    }
}

async fn send_with_backpressure_wait<T: Send>(
    sender: &mpsc::Sender<T>,
    mut item: Option<T>,
    label: &str,
) -> bool {
    loop {
        match timeout(Duration::from_secs(1), sender.reserve()).await {
            Ok(Ok(permit)) => {
                permit.send(item.take().expect("item already sent"));
                return true;
            }
            Ok(Err(_)) => {
                eprintln!("{} Receiver dropped. Fatal: exiting.", label);
                std::process::exit(1);
            }
            Err(_) => {
                eprintln!(
                    "{} Channel still full. Waiting for backpressure to clear...",
                    label
                );
            }
        }
    }
}

// --- GEYSER SUBSCRIBER ---
pub async fn geyser_subscriber(
    endpoint: String,
    api_key: String,
    programs_to_monitor: Vec<String>,
    tx_sender: mpsc::Sender<UnifiedTransaction<'static>>,
) -> anyhow::Result<()> {
    println!(
        "👂 Starting Geyser subscriber for {} programs...",
        programs_to_monitor.len()
    );

    retry(ExponentialBackoff::default(), || async {
        let tx_sender = tx_sender.clone();
        let mut client = GeyserGrpcClient::build_from_shared(endpoint.clone())
            .map_err(|e| backoff::Error::permanent(anyhow!(e)))?
            .x_token(Some(api_key.clone()))
            .map_err(|e| backoff::Error::permanent(anyhow!(e)))?
            .connect()
            .await
            .map_err(|e| backoff::Error::transient(anyhow!(e)))?;

        let mut transactions_filter = HashMap::new();
        transactions_filter.insert(
            "all_programs".to_string(),
            SubscribeRequestFilterTransactions {
                vote: Some(false),
                failed: Some(true),
                account_include: programs_to_monitor.clone(),
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
            .map_err(|e| backoff::Error::transient(anyhow!(e)))?;

        println!("✅ Geyser subscription successful.");

        while let Some(message) = stream.next().await {
            match message {
                Ok(msg) => {
                    if let Some(UpdateOneof::Transaction(geyser_tx)) = msg.update_oneof {
                        match convert_grpc_to_unified(geyser_tx) {
                            Ok(unified_tx) => {
                                if !send_with_backpressure(&tx_sender, unified_tx, "[Geyser]").await
                                {
                                    break;
                                }
                            }
                            Err(e) => eprintln!("[Geyser] Failed to convert tx: {:?}", e),
                        }
                    }
                }
                Err(e) => {
                    eprintln!("[Geyser] Stream error: {:?}. Reconnecting...", e);
                    return Err(backoff::Error::transient(anyhow::Error::from(e)));
                }
            }
        }
        Err(backoff::Error::transient(anyhow!("Geyser stream ended.")))
    })
    .await
}

// --- WEBSOCKET SUBSCRIBER ---
pub async fn websocket_subscriber(
    ws_rpc_url: String,
    rpc_client: Arc<RpcClient>,
    programs_to_monitor: Vec<String>,
    tx_sender: mpsc::Sender<UnifiedTransaction<'static>>,
) -> anyhow::Result<()> {
    println!(
        "👂 Starting WebSocket subscriber for {} programs...",
        programs_to_monitor.len()
    );

    let pubsub_client = PubsubClient::new(&ws_rpc_url).await?;
    let (mut logs, _unsubscribe) = pubsub_client
        .logs_subscribe(
            RpcTransactionLogsFilter::Mentions(programs_to_monitor),
            RpcTransactionLogsConfig {
                commitment: Some(CommitmentConfig::confirmed()),
            },
        )
        .await?;
    println!("✅ WebSocket subscription successful.");

    while let Some(log) = logs.next().await {
        let signature: Signature = "3w4E9qhHDyXnr7V6moJB69ubQcDuFiGW8P1BvRnjjhL767TxrGjccbdP9XNKpk9U69BsVTdbbRBynynzE7bhpGDQ".parse()?;
        // let signature: Signature = log.value.signature.parse()?;
        let rpc_client_clone = Arc::clone(&rpc_client);
        let tx_sender_clone = tx_sender.clone();

        tokio::spawn(async move {
            let config = RpcTransactionConfig {
                encoding: Some(UiTransactionEncoding::Base64),
                commitment: Some(CommitmentConfig::confirmed()),
                max_supported_transaction_version: Some(0),
            };

            match rpc_client_clone
                .get_transaction_with_config(&signature, config)
                .await
            {
                Ok(tx) => match convert_rpc_to_unified(tx) {
                    Ok(unified_tx) => {
                        if !send_with_backpressure(&tx_sender_clone, unified_tx, "[WebSocket]")
                            .await
                        {
                            eprintln!(
                                "[WebSocket] Receiver dropped. Shutting down task for {}.",
                                signature
                            );
                        }
                    }
                    Err(e) => {
                        eprintln!("[WebSocket] Failed to convert tx {}: {:?}", signature, e);
                    }
                },
                Err(e) => {
                    eprintln!("[WebSocket] Failed to fetch tx {}: {:?}", signature, e);
                }
            }
        });
    }
    Err(anyhow!("WebSocket stream ended unexpectedly."))
}

// --- HELPER TO CONVERT GEYSER GRPC -> UnifiedTransaction ---
fn convert_grpc_to_unified(
    geyser_tx: SubscribeUpdateTransaction,
) -> Result<UnifiedTransaction<'static>> {
    let tx_info = geyser_tx
        .transaction
        .ok_or_else(|| anyhow!("Missing tx_info"))?;
    let transaction = tx_info
        .transaction
        .ok_or_else(|| anyhow!("Missing transaction"))?;
    let meta = tx_info.meta.ok_or_else(|| anyhow!("Missing meta"))?;
    let signature: Signature = tx_info.signature.as_slice().try_into()?;
    let message = transaction
        .message
        .ok_or_else(|| anyhow!("Missing message"))?;
    let header = message.header.ok_or_else(|| anyhow!("Missing header"))?;
    let transaction_index = u32::try_from(tx_info.index)
        .map_err(|_| anyhow!("Transaction index overflow for slot {}", geyser_tx.slot))?;

    let sdk_msg = Message::new_with_compiled_instructions(
        header.num_required_signatures as u8,
        header.num_readonly_signed_accounts as u8,
        header.num_readonly_unsigned_accounts as u8,
        message
            .account_keys
            .iter()
            .filter_map(|key| Pubkey::try_from(key.as_slice()).ok())
            .collect(),
        Hash::new(&message.recent_blockhash),
        message
            .instructions
            .into_iter()
            .map(|ix| CompiledInstruction {
                program_id_index: ix.program_id_index as u8,
                accounts: ix.accounts,
                data: ix.data,
            })
            .collect(),
    );
    let versioned_tx = VersionedTransaction {
        signatures: transaction
            .signatures
            .iter()
            .filter_map(|s| Signature::try_from(s.as_slice()).ok())
            .collect(),
        message: VersionedMessage::Legacy(sdk_msg),
    };

    let account_keys = build_full_account_keys(&versioned_tx, &Some(meta.clone()));
    let signers: Vec<String> = versioned_tx.message.static_account_keys()
        [..header.num_required_signatures as usize]
        .iter()
        .map(|s| s.to_string())
        .collect();

    let pre_balances =
        build_balance_map_grpc(&account_keys, &meta.pre_token_balances, &meta.pre_balances);
    let post_balances = build_balance_map_grpc(
        &account_keys,
        &meta.post_token_balances,
        &meta.post_balances,
    );

    let mut token_decimals = HashMap::new();
    for tb in meta
        .pre_token_balances
        .iter()
        .chain(meta.post_token_balances.iter())
    {
        if let Some(amount) = &tb.ui_token_amount {
            token_decimals
                .entry(tb.mint.clone())
                .or_insert(amount.decimals as u8);
        }
    }

    let static_tx = Box::leak(Box::new(versioned_tx));
    let static_meta = Box::leak(Box::new(meta.clone()));

    Ok(UnifiedTransaction {
        signature: signature.to_string(),
        slot: geyser_tx.slot,
        transaction_index,
        block_time: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as u32,
        signers,
        error: static_meta
            .err
            .as_ref()
            .map(|e| String::from_utf8_lossy(&e.err).to_string()),
        account_keys,
        formatted_instructions: format_transaction(static_tx, static_meta),
        logs: static_meta.log_messages.clone(),
        pre_balances,
        post_balances,
        token_decimals,
    })
}

// --- HELPER TO CONVERT RPC -> UnifiedTransaction ---
fn convert_rpc_to_unified(
    rpc_tx: EncodedConfirmedTransactionWithStatusMeta,
) -> Result<UnifiedTransaction<'static>> {
    let slot = rpc_tx.slot;
    let block_time = rpc_tx.block_time.unwrap_or(0);
    let meta = rpc_tx
        .transaction
        .meta
        .as_ref()
        .ok_or_else(|| anyhow!("Missing transaction meta"))?;

    // Step 1: Decode the transaction directly from the Base64 data
    let versioned_tx: VersionedTransaction = match rpc_tx.transaction.transaction {
        EncodedTransaction::Binary(encoded_tx, _) => {
            // 👇 THE ONLY CHANGE IS ADDING `&` HERE 👇
            let tx_data = base64::decode(&encoded_tx)?;
            bincode::deserialize(&tx_data)?
        }
        _ => {
            return Err(anyhow!(
                "Expected Base64 encoded transaction but received another format."
            ));
        }
    };

    // Step 2: Convert the RPC meta to your gRPC-compatible format
    let account_keys_from_tx = versioned_tx.message.static_account_keys();
    let geyser_meta = rpc_meta_to_grpc_meta(meta, account_keys_from_tx)?;

    // Step 3: Build the final UnifiedTransaction struct
    let full_account_keys_list = build_full_account_keys(&versioned_tx, &Some(geyser_meta.clone()));

    let signature = versioned_tx.signatures.get(0).cloned().unwrap_or_default();
    return Err(anyhow!(
        "RPC transaction {} missing transaction index; use geyser or CAR ingestion",
        signature.to_string()
    ));
}

// --- NEW HELPER TO CONVERT RPC TYPES TO GEYSER-COMPATIBLE TYPES ---
fn rpc_to_geyser_compatible(
    tx_with_meta: &EncodedTransactionWithStatusMeta,
) -> Result<(VersionedTransaction, GrpcTransactionStatusMeta)> {
    let ui_tx = match &tx_with_meta.transaction {
        EncodedTransaction::Json(ui_tx) => ui_tx,
        _ => return Err(anyhow!("Unsupported transaction encoding")),
    };
    let meta = tx_with_meta
        .meta
        .as_ref()
        .ok_or_else(|| anyhow!("Missing transaction meta"))?;

    let (message, account_keys) = match &ui_tx.message {
        solana_transaction_status::UiMessage::Parsed(msg) => {
            let keys: Vec<Pubkey> = msg
                .account_keys
                .iter()
                .filter_map(|k| Pubkey::from_str(&k.pubkey).ok())
                .collect();

            // ========================================================================
            // >> THIS IS THE FIX <<
            // This logic now correctly handles ALL instruction types from the RPC,
            // ensuring no instructions are dropped.
            // ========================================================================
            let instructions: Vec<CompiledInstruction> = msg
                .instructions
                .iter()
                .filter_map(|ix| {
                    let (program_id_str, accounts_str, data_str) = match ix {
                        UiInstruction::Parsed(UiParsedInstruction::Parsed(p)) => {
                            // Handle fully parsed instructions (like SPL Token calls)
                            // Note: These often don't have raw data, so data_str is empty.
                            (p.program_id.clone(), vec![], String::new())
                        }
                        UiInstruction::Parsed(UiParsedInstruction::PartiallyDecoded(pd)) => {
                            // Handle partially decoded instructions (like our pAMM call)
                            (pd.program_id.clone(), pd.accounts.clone(), pd.data.clone())
                        }
                        _ => return None, // Ignore unknown/unsupported formats
                    };

                    keys.iter()
                        .position(|k| k.to_string() == program_id_str)
                        .map(|pid_idx| CompiledInstruction {
                            program_id_index: pid_idx as u8,
                            accounts: accounts_str
                                .iter()
                                .filter_map(|acc| keys.iter().position(|k| k.to_string() == *acc))
                                .map(|acc_idx| acc_idx as u8)
                                .collect(),
                            data: bs58::decode(data_str).into_vec().unwrap_or_default(),
                        })
                })
                .collect();
            // ========================================================================

            let (
                num_required_signatures,
                num_readonly_signed_accounts,
                num_readonly_unsigned_accounts,
            ) = msg.account_keys.iter().fold(
                (0, 0, 0),
                |(mut sigs, mut ro_signed, mut ro_unsigned), key| {
                    if key.signer {
                        sigs += 1;
                        if !key.writable {
                            ro_signed += 1;
                        }
                    } else if !key.writable {
                        ro_unsigned += 1;
                    }
                    (sigs, ro_signed, ro_unsigned)
                },
            );

            let legacy_msg = Message::new_with_compiled_instructions(
                num_required_signatures,
                num_readonly_signed_accounts,
                num_readonly_unsigned_accounts,
                keys.clone(),
                Hash::from_str(&msg.recent_blockhash)?,
                instructions,
            );
            (VersionedMessage::Legacy(legacy_msg), keys)
        }
        _ => return Err(anyhow!("Expected a parsed message")),
    };

    let versioned_tx = VersionedTransaction {
        signatures: ui_tx
            .signatures
            .iter()
            .filter_map(|s| s.parse().ok())
            .collect(),
        message,
    };

    let geyser_meta = rpc_meta_to_grpc_meta(meta, &account_keys)?;
    Ok((versioned_tx, geyser_meta))
}

fn rpc_meta_to_grpc_meta(
    meta: &UiTransactionStatusMeta,
    account_keys: &[Pubkey],
) -> Result<GrpcTransactionStatusMeta, anyhow::Error> {
    let pre_token_balances_opt: Option<Vec<UiTransactionTokenBalance>> =
        meta.pre_token_balances.clone().into();
    let post_token_balances_opt: Option<Vec<UiTransactionTokenBalance>> =
        meta.post_token_balances.clone().into();
    let inner_instructions_opt: Option<Vec<UiInnerInstructions>> =
        meta.inner_instructions.clone().into();
    let log_messages_opt: Option<Vec<String>> = meta.log_messages.clone().into();

    let loaded_addresses_opt: Option<UiLoadedAddresses> = meta.loaded_addresses.clone().into();

    // --- FIX FOR LOADED ADDRESSES (UPDATED TO USE THE CORRECT TYPE) ---
    let (loaded_writable_addresses, loaded_readonly_addresses) =
        if let Some(loaded_addresses) = loaded_addresses_opt {
            let writable = loaded_addresses
                .writable
                .iter()
                .filter_map(|s| Pubkey::from_str(s).ok())
                .map(|p| p.to_bytes().to_vec())
                .collect();
            let readonly = loaded_addresses
                .readonly
                .iter()
                .filter_map(|s| Pubkey::from_str(s).ok())
                .map(|p| p.to_bytes().to_vec())
                .collect();
            (writable, readonly)
        } else {
            (vec![], vec![])
        };

    Ok(GrpcTransactionStatusMeta {
        err: meta.err.as_ref().map(|e| {
            // Use the helper to get a clean string like "InstructionError(0, Custom(1))"
            let error_string = format_tx_error_from_rpc(e);
            GrpcTransactionError {
                // Store that clean string as bytes
                err: error_string.into_bytes(),
            }
        }),
        fee: meta.fee,
        pre_balances: meta.pre_balances.clone(),
        post_balances: meta.post_balances.clone(),

        inner_instructions: inner_instructions_opt
            .unwrap_or_default()
            .into_iter()
            .map(|rpc_inner_ix_block| GrpcInnerInstructions {
                index: rpc_inner_ix_block.index as u32,
                instructions: rpc_inner_ix_block
                    .instructions
                    .into_iter()
                    .filter_map(|rpc_ix| match rpc_ix {
                        UiInstruction::Compiled(c) => Some(GrpcInnerInstruction {
                            program_id_index: c.program_id_index as u32,
                            accounts: c.accounts.clone(),
                            data: bs58::decode(c.data).into_vec().unwrap_or_default(),
                            stack_height: None,
                        }),
                        UiInstruction::Parsed(UiParsedInstruction::PartiallyDecoded(pd)) => {
                            Some(GrpcInnerInstruction {
                                program_id_index: account_keys
                                    .iter()
                                    .position(|k| k.to_string() == pd.program_id)
                                    .unwrap_or(0)
                                    as u32,
                                accounts: pd
                                    .accounts
                                    .iter()
                                    .filter_map(|acc| {
                                        account_keys
                                            .iter()
                                            .position(|k| k.to_string() == *acc)
                                            .map(|idx| idx as u8)
                                    })
                                    .collect(),
                                data: bs58::decode(&pd.data).into_vec().unwrap_or_default(),
                                stack_height: pd.stack_height,
                            })
                        }
                        _ => None,
                    })
                    .collect(),
            })
            .collect(),

        log_messages: log_messages_opt.unwrap_or_default(),
        pre_token_balances: pre_token_balances_opt
            .unwrap_or_default()
            .into_iter()
            .map(|tb| RpcTokenBalanceWrapper(tb).into_grpc())
            .collect(),
        post_token_balances: post_token_balances_opt
            .unwrap_or_default()
            .into_iter()
            .map(|tb| RpcTokenBalanceWrapper(tb).into_grpc())
            .collect(),
        rewards: vec![],

        loaded_writable_addresses,
        loaded_readonly_addresses,

        return_data: None,
        compute_units_consumed: meta.compute_units_consumed.clone().into(),
        inner_instructions_none: meta.inner_instructions.is_none(),
        log_messages_none: meta.log_messages.is_none(),
        return_data_none: true,
    })
}

fn build_balance_map_grpc(
    account_keys: &[Pubkey],
    token_balances: &[GrpcTokenBalance],
    sol_balances: &[u64],
) -> TokenBalanceMap {
    let mut balance_map: TokenBalanceMap = HashMap::new();

    // Build SOL balance map (this part is correct)
    let mut sol_balance_map = BalanceMap::new();
    for (i, &balance) in sol_balances.iter().enumerate() {
        if let Some(key) = account_keys.get(i) {
            sol_balance_map.insert(key.to_string(), balance);
        }
    }
    balance_map.insert(
        "So11111111111111111111111111111111111111112".to_string(),
        sol_balance_map,
    );

    // --- START: THE FIX ---
    // Build SPL token balance maps with the correct keys
    for tb in token_balances {
        // `tb.account_index` gives us the position of the TOKEN ACCOUNT in the master `account_keys` list.
        if let Some(token_account_key) = account_keys.get(tb.account_index as usize) {
            let mint_map = balance_map.entry(tb.mint.clone()).or_default();

            if let Some(amount) = &tb.ui_token_amount {
                if let Ok(parsed_amount) = amount.amount.parse::<u64>() {
                    // Use the TOKEN ACCOUNT address as the key, NOT the owner.
                    mint_map.insert(token_account_key.to_string(), parsed_amount);
                }
            }
        }
    }
    // --- END: THE FIX ---

    balance_map
}
struct RpcTokenBalanceWrapper(UiTransactionTokenBalance);

impl RpcTokenBalanceWrapper {
    fn into_grpc(self) -> GrpcTokenBalance {
        let tb = self.0;
        GrpcTokenBalance {
            account_index: tb.account_index as u32,
            mint: tb.mint,
            owner: Into::<Option<String>>::into(tb.owner).unwrap_or_default(),
            program_id: Into::<Option<String>>::into(tb.program_id).unwrap_or_default(),
            ui_token_amount: Some(GrpcUiTokenAmount {
                ui_amount: tb.ui_token_amount.ui_amount.unwrap_or_default(),
                decimals: tb.ui_token_amount.decimals as u32,
                amount: tb.ui_token_amount.amount,
                ui_amount_string: tb.ui_token_amount.ui_amount_string,
            }),
        }
    }
}

fn format_tx_error_from_rpc(e: &TransactionError) -> String {
    e.to_string()
}
// Helper to track our download progress
struct Progress {
    bytes_processed: Arc<Mutex<u64>>,
    total_size: Arc<Mutex<Option<u64>>>,
}

// Our custom reader that transparently handles reconnections.
// This is the core of the solution.
struct ResumableStreamReader {
    client: Client,
    url: String,
    progress: Progress,
    // The current, active HTTP response stream. None if we're disconnected.
    stream: Option<reqwest::blocking::Response>,
}

impl ResumableStreamReader {
    // This function handles the logic of making a new connection.
    fn reconnect(&mut self) -> io::Result<()> {
        let mut retries = 0;
        const MAX_RETRIES: u32 = 10;

        loop {
            let bytes_processed = *self.progress.bytes_processed.lock().unwrap();
            let range_header = format!("bytes={}-", bytes_processed);

            println!(
                "[ResumableStream] 📡 Attempting to connect, requesting range: {}",
                range_header
            );

            let mut headers = HeaderMap::new();
            headers.insert(RANGE, HeaderValue::from_str(&range_header).unwrap());
            headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("identity"));

            match self.client.get(&self.url).headers(headers).send() {
                Ok(resp) => {
                    match resp.status() {
                        StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                            // On the first successful connection, discover the total file size.
                            let mut total_size_guard = self.progress.total_size.lock().unwrap();
                            if total_size_guard.is_none() {
                                if let Some(content_range) = resp.headers().get("content-range") {
                                    if let Ok(range_str) = content_range.to_str() {
                                        if let Some(size_str) = range_str.split('/').last() {
                                            if let Ok(size) = size_str.parse::<u64>() {
                                                *total_size_guard = Some(size);
                                                println!(
                                                    "[ResumableStream] ✅ Discovered file size: {:.2} GB",
                                                    size as f64 / 1e9
                                                );
                                            }
                                        }
                                    }
                                }
                            }

                            println!("[ResumableStream] ✅ Connection successful.");
                            self.stream = Some(resp);
                            return Ok(());
                        }
                        StatusCode::RANGE_NOT_SATISFIABLE => {
                            // This is not an error. It means we have already downloaded the entire file.
                            println!(
                                "[ResumableStream] 🏁 Server indicated range not satisfiable. Assuming download is complete."
                            );
                            return Err(io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                "Download complete",
                            ));
                        }
                        status => {
                            let err_msg = format!("Server returned unexpected status: {}", status);
                            eprintln!("[ResumableStream] ❌ {}", err_msg);
                            // Fall through to retry logic
                        }
                    }
                }
                Err(e) => {
                    eprintln!("[ResumableStream] ❌ Connection error: {}", e);
                    // Fall through to retry logic
                }
            }

            retries += 1;
            if retries >= MAX_RETRIES {
                let err_msg = format!("Failed to reconnect after {} attempts.", MAX_RETRIES);
                eprintln!("[ResumableStream] 🔴 {}", err_msg);
                return Err(io::Error::new(io::ErrorKind::BrokenPipe, err_msg));
            }
            let sleep_duration = Duration::from_secs(5 * retries as u64);
            println!("[ResumableStream] 🔁 Retrying in {:?}...", sleep_duration);
            std::thread::sleep(sleep_duration);
        }
    }
}

// We implement the `Read` trait, making our struct usable by `BufReader` and `NodeReader`.
impl Read for ResumableStreamReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // Loop to handle transparent reconnection within a single read call.
        loop {
            if self.stream.is_none() {
                // If we're disconnected, try to reconnect.
                // If reconnect fails with a permanent error, it will be propagated here.
                self.reconnect()?;
            }

            if let Some(stream) = self.stream.as_mut() {
                match stream.read(buf) {
                    Ok(0) => {
                        // The server closed the connection for this chunk cleanly.
                        // Set stream to None so we reconnect on the next iteration.
                        println!(
                            "[ResumableStream] ℹ️ Current stream ended. Will reconnect on next read."
                        );
                        self.stream = None;
                        // Don't return Ok(0) yet, loop again to reconnect immediately.
                        continue;
                    }
                    Ok(bytes_read) => {
                        // Success! We read some bytes.
                        let mut count = self.progress.bytes_processed.lock().unwrap();
                        *count += bytes_read as u64;
                        return Ok(bytes_read);
                    }
                    Err(e) => {
                        // The connection was likely dropped unexpectedly.
                        eprintln!(
                            "[ResumableStream] ⚠️ Read error (connection dropped?): {}. Attempting to reconnect.",
                            e
                        );
                        self.stream = None;
                        // Loop again to attempt reconnection.
                        continue;
                    }
                }
            }
        }
    }
}

// ====================================================================================
// START: OPTIMIZATION IMPLEMENTATION
// ====================================================================================

/// ## New Helper Function: Lightweight Deserializer
///
/// Efficiently decodes a compact-u16 length from a byte slice.
/// This is used to quickly find the length of arrays (like signatures and account keys)
/// in a raw transaction without parsing the whole object.
fn decode_compact_u16(bytes: &mut &[u8]) -> Option<usize> {
    if bytes.is_empty() {
        return None;
    }
    let first_byte = bytes[0];
    *bytes = &bytes[1..];
    let mut len = (first_byte & 0x7f) as usize;

    if (first_byte & 0x80) != 0 {
        if bytes.is_empty() {
            return None;
        }
        let second_byte = bytes[0];
        *bytes = &bytes[1..];
        len |= ((second_byte & 0x7f) as usize) << 7;

        if (second_byte & 0x80) != 0 {
            if bytes.is_empty() {
                return None;
            }
            let third_byte = bytes[0];
            *bytes = &bytes[1..];
            len |= (third_byte as usize) << 14;
        }
    }
    Some(len)
}

/// ## New Helper Function: Lightweight Key Extractor
///
/// Parses the raw byte array of a serialized Solana transaction to extract
/// only the list of account public keys. This avoids the high CPU cost of
/// full deserialization for transactions that will be discarded anyway.
fn lightweight_get_account_keys(mut raw_tx_bytes: &[u8]) -> Option<Vec<Pubkey>> {
    // 1. Skip Signatures
    let sig_len = decode_compact_u16(&mut raw_tx_bytes)?;
    let sig_bytes_len = sig_len.checked_mul(64)?;
    if raw_tx_bytes.len() < sig_bytes_len {
        return None;
    }
    raw_tx_bytes = &raw_tx_bytes[sig_bytes_len..];

    // Handle Versioned Transactions (we only care about legacy for this fast path)
    let version_byte = *raw_tx_bytes.get(0)?;
    if (version_byte & 0x80) != 0 {
        // It's a versioned transaction. Skip the light parse and let the full parser handle it.
        return None;
    }

    // 2. Read Legacy Message Header
    if raw_tx_bytes.len() < 3 {
        return None;
    }
    let header = MessageHeader {
        num_required_signatures: raw_tx_bytes[0],
        num_readonly_signed_accounts: raw_tx_bytes[1],
        num_readonly_unsigned_accounts: raw_tx_bytes[2],
    };
    raw_tx_bytes = &raw_tx_bytes[3..];

    // 3. Read Account Keys Array
    let mut account_keys_bytes = raw_tx_bytes;
    let keys_len = decode_compact_u16(&mut account_keys_bytes)?;

    let keys_bytes_len = keys_len.checked_mul(32)?;
    if account_keys_bytes.len() < keys_bytes_len {
        return None;
    }

    let mut keys = Vec::with_capacity(keys_len);
    for _ in 0..keys_len {
        keys.push(Pubkey::new_from_array(
            account_keys_bytes[..32].try_into().ok()?,
        ));
        account_keys_bytes = &account_keys_bytes[32..];
    }

    Some(keys)
}

struct PreprocessedTransaction<'a> {
    transaction_node: &'a Transaction,
    meta_bytes: Vec<u8>,
}

/// ## Modified Function: The Optimized Processing Logic
///
/// This function is now much more efficient. It performs a two-pass filtering system.
fn process_nodes_in_parallel(
    nodes: &node::NodesWithCids,
    programs_of_interest: &HashSet<Pubkey>,
) -> Result<(u64, Vec<UnifiedTransaction<'static>>)> {
    let block = nodes.get_block().map_err(|e| anyhow!(e.to_string()))?;
    let slot = block.slot;
    let block_time = block.meta.blocktime as u32;

    let all_transaction_nodes: Vec<&Transaction> = nodes
        .0
        .iter()
        .filter_map(|nwc| match nwc.get_node() {
            node::Node::Transaction(tx) => Some(tx),
            _ => None,
        })
        .collect();

    let total_tx_nodes = all_transaction_nodes.len() as u64;

    // --- STAGE 1: SERIAL PRE-PROCESSING ---
    // Do all the work that requires access to the shared `nodes` object here, in a single pass.
    let work_packets: Vec<PreprocessedTransaction> = all_transaction_nodes
        .iter()
        .filter_map(|transaction_node| {
            if !transaction_node.is_complete_data() || !transaction_node.is_complete_metadata() {
                return None;
            }

            // The expensive, contentious call is done ONCE per transaction, serially.
            let meta_bytes = nodes
                .reassemble_dataframes(transaction_node.metadata.clone())
                .ok()?;

            Some(PreprocessedTransaction {
                transaction_node,
                meta_bytes,
            })
        })
        .collect();

    // --- STAGE 2: FULLY PARALLEL, CONTENTION-FREE PROCESSING ---
    // Now, each thread gets a `PreprocessedTransaction` which is completely self-contained.
    // There is NO shared data access inside this hot loop.
    let unified_txs: Vec<UnifiedTransaction<'static>> = work_packets
        .into_par_iter()
        .filter_map(|work_packet| {
            // Lightweight filter pass
            if let Some(account_keys) = lightweight_get_account_keys(&work_packet.transaction_node.data.data.to_vec()) {
                let mut is_interesting = programs_of_interest.is_empty();
                for key in &account_keys {
                    if *key == VOTE_PROGRAM_ID { return None; }
                    if !is_interesting && programs_of_interest.contains(key) {
                        is_interesting = true;
                    }
                }
                if !is_interesting { return None; }
            }

            // Heavyweight parsing (only for interesting transactions)
            let versioned_tx: VersionedTransaction = work_packet.transaction_node.as_parsed().ok()?;

            // Final check for versioned transactions or those that failed the light parse
            let account_keys = versioned_tx.message.static_account_keys();
            let is_interesting = programs_of_interest.is_empty() || account_keys.iter().any(|key| programs_of_interest.contains(key));
            let is_vote = account_keys.iter().any(|key| *key == VOTE_PROGRAM_ID);

            if !is_interesting || is_vote {
                return None;
            }

            // Decompression and decoding (this is now perfectly parallel)
            let decompressed_meta = utils::decompress_zstd(work_packet.meta_bytes).ok()?;
            let proto_meta: solana_storage_proto::convert::generated::TransactionStatusMeta = ProstMessage::decode(decompressed_meta.as_slice()).ok()?;
            let native_meta: TransactionStatusMeta = proto_meta.try_into().ok()?;

            let raw_index = match work_packet.transaction_node.index {
                Some(idx) => idx,
                None => {
                    eprintln!(
                        "[CARWorker] Missing transaction index for slot {}. Skipping tx {}",
                        slot,
                        versioned_tx
                            .signatures
                            .get(0)
                            .map(|s| s.to_string())
                            .unwrap_or_default()
                    );
                    return None;
                }
            };

            let transaction_index = match u32::try_from(raw_index) {
                Ok(v) => v,
                Err(_) => {
                    eprintln!(
                        "[CARWorker] Transaction index overflow for slot {} (value = {}). Skipping tx {}",
                        slot,
                        raw_index,
                        versioned_tx
                            .signatures
                            .get(0)
                            .map(|s| s.to_string())
                            .unwrap_or_default()
                    );
                    return None;
                }
            };

            convert_car_tx_to_unified(
                slot,
                block_time,
                transaction_index,
                versioned_tx,
                native_meta,
            )
            .ok()
        })
        .collect();

    Ok((total_tx_nodes, unified_txs))
}

// ====================================================================================
// END: OPTIMIZATION IMPLEMENTATION
// ====================================================================================

fn reader_task(
    car_file_path: String,
    raw_node_sender: mpsc::Sender<node::RawNode>,
    pre_filter_count: Arc<AtomicU64>,
    post_filter_count: Arc<AtomicU64>,
) -> anyhow::Result<()> {
    println!(
        "[Reader] 📦 Starting RESUMABLE CAR file reader for: {}",
        car_file_path
    );

    // --- THIS IS THE FIX ---
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(30)) // Keep a timeout for making the initial connection.
        // REMOVED: .timeout(Duration::from_secs(180)) // REMOVE the total request timeout.
        .pool_idle_timeout(None) // Keep this to prevent using stale connections.
        .gzip(false)
        .no_deflate()
        .no_brotli()
        .build()?;
    // --- END FIX ---

    // The rest of the function remains exactly the same.
    let mut last_log_time = Instant::now();
    let mut last_log_bytes: u64 = 0;
    let mut last_log_pre_count: u64 = 0;
    let mut last_log_post_count: u64 = 0;
    let progress = Progress {
        bytes_processed: Arc::new(Mutex::new(0u64)),
        total_size: Arc::new(Mutex::new(None)),
    };
    let mut resumable_reader = ResumableStreamReader {
        client,
        url: car_file_path,
        stream: None,
        progress: Progress {
            bytes_processed: Arc::clone(&progress.bytes_processed),
            total_size: Arc::clone(&progress.total_size),
        },
    };
    let mut node_reader = node::NodeReader::new(&mut resumable_reader)
        .map_err(|e| anyhow!("CAR parser init failed: {}", e))?;
    node_reader
        .read_raw_header()
        .map_err(|e| anyhow!("CAR header read failed: {}", e))?;
    last_log_bytes = *progress.bytes_processed.lock().unwrap();

    loop {
        match node_reader.next() {
            Ok(raw_node) => {
                if raw_node_sender.blocking_send(raw_node).is_err() {
                    println!("[Reader] 🏁 Assembler has shut down. Stopping reader.");
                    break;
                }
            }
            Err(e) => {
                if let Some(io_err) = e.downcast_ref::<io::Error>() {
                    if io_err.kind() == io::ErrorKind::UnexpectedEof {
                        println!("[Reader] 🏁 Reached end of file stream. Closing channel.");
                        break;
                    }
                }
                eprintln!("[Reader] 🔴 FATAL: Failed reading raw node: {}", e);
                return Err(anyhow!(e.to_string()));
            }
        };

        let now = Instant::now();
        if now.duration_since(last_log_time) >= Duration::from_secs(10) {
            let bytes_total = *progress.bytes_processed.lock().unwrap();
            let total_size_opt = *progress.total_size.lock().unwrap();
            let total_txs_in_car = pre_filter_count.load(Ordering::Relaxed);
            let total_processed_txs = post_filter_count.load(Ordering::Relaxed);
            let bytes_delta = bytes_total.saturating_sub(last_log_bytes);
            let car_tx_delta = total_txs_in_car.saturating_sub(last_log_pre_count);
            let processed_tx_delta = total_processed_txs.saturating_sub(last_log_post_count);
            let seconds_delta = now.duration_since(last_log_time).as_secs_f64();

            if seconds_delta > 0.0 {
                let speed_mbps = (bytes_delta as f64 / 1_048_576.0) / seconds_delta;
                let car_tx_rate = car_tx_delta as f64 / seconds_delta;
                let filtered_tx_rate = processed_tx_delta as f64 / seconds_delta;
                let filter_percentage = if total_txs_in_car > 0 {
                    (total_processed_txs as f64 / total_txs_in_car as f64) * 100.0
                } else {
                    0.0
                };

                if let Some(size) = total_size_opt {
                    let percent = (bytes_total as f64 / size as f64) * 100.0;
                    println!(
                        "[Progress] 📊 {:.2}% | {:.2} GB/{:.2} GB | {:.2} MB/s | Rate In: {:.0} tx/s | Rate Out: {:.0} tx/s | Filtered: {}/{} ({:.2}%)",
                        percent,
                        bytes_total as f64 / 1e9,
                        size as f64 / 1e9,
                        speed_mbps,
                        car_tx_rate,
                        filtered_tx_rate,
                        total_processed_txs,
                        total_txs_in_car,
                        filter_percentage
                    );
                } else {
                    println!(
                        "[Progress] 📊 {:.2} GB | {:.2} MB/s | Rate In: {:.0} tx/s | Rate Out: {:.0} tx/s | Filtered: {}/{} ({:.2}%)",
                        bytes_total as f64 / 1e9,
                        speed_mbps,
                        car_tx_rate,
                        filtered_tx_rate,
                        total_processed_txs,
                        total_txs_in_car,
                        filter_percentage
                    );
                }
            }
            last_log_time = now;
            last_log_bytes = bytes_total;
            last_log_pre_count = total_txs_in_car;
            last_log_post_count = total_processed_txs;
        }
    }
    Ok(())
}

async fn block_assembler_task(
    mut raw_node_receiver: mpsc::Receiver<node::RawNode>,
    block_sender: mpsc::Sender<node::NodesWithCids>,
) {
    println!("[Assembler] 🧩 Started and waiting for raw nodes.");
    let mut nodes = node::NodesWithCids::new();
    while let Some(raw_node) = raw_node_receiver.recv().await {
        // --- THIS IS THE FIX ---
        // We parse the node and immediately map the error to an anyhow::Error,
        // which IS thread-safe. This prevents the non-Send error from
        // ever existing across an .await point.
        let parsed_result = raw_node.parse().map_err(|e| anyhow!(e.to_string()));
        // --- END FIX ---

        match parsed_result {
            Ok(parsed_node) => {
                let node_with_cid = node::NodeWithCid::new(raw_node.cid(), parsed_node);
                let is_block = node_with_cid.get_node().is_block();
                nodes.push(node_with_cid);

                if is_block {
                    if !send_with_backpressure_wait(&block_sender, Some(nodes), "[Assembler]").await
                    {
                        eprintln!("[Assembler] 🔴 Worker channel closed. Shutting down.");
                        return;
                    }
                    nodes = node::NodesWithCids::new();
                }
            }
            Err(e) => {
                eprintln!("[Assembler] ⚠️ Failed to parse raw node: {}", e);
            }
        }
    }
    println!("[Assembler] 🏁 No more raw nodes. Shutting down.");
}

async fn worker_task(
    worker_id: usize,
    receiver: Arc<TokioMutex<mpsc::Receiver<node::NodesWithCids>>>,
    sender: mpsc::Sender<UnifiedTransaction<'static>>,
    programs_of_interest: Arc<HashSet<Pubkey>>,
    pre_filter_count: Arc<AtomicU64>,
    post_filter_count: Arc<AtomicU64>,
) {
    println!("[Worker {}] 👷‍♂️ Started and waiting for blocks.", worker_id);
    loop {
        let nodes = {
            let mut guard = receiver.lock().await;
            guard.recv().await
        };

        if let Some(nodes) = nodes {
            match process_nodes_in_parallel(&nodes, &programs_of_interest) {
                Ok((pre_count_block, unified_txs)) => {
                    let post_count_block = unified_txs.len() as u64;

                    if pre_count_block > 0 {
                        pre_filter_count.fetch_add(pre_count_block, Ordering::Relaxed);
                    }
                    if post_count_block > 0 {
                        post_filter_count.fetch_add(post_count_block, Ordering::Relaxed);
                    }

                    for tx in unified_txs {
                        if !send_with_backpressure_wait(
                            &sender,
                            Some(tx),
                            &format!("[Worker {}]", worker_id),
                        )
                        .await
                        {
                            eprintln!(
                                "[Worker {}] 🔴 Receiver channel closed. Shutting down.",
                                worker_id
                            );
                            return;
                        }
                    }
                }
                // --- THIS BLOCK IS NOW CORRECTLY FILLED IN ---
                Err(e) => {
                    eprintln!("[Worker {}] ⚠️ Error processing block: {}", worker_id, e);
                } // --- END FIX ---
            }
        } else {
            println!(
                "[Worker {}] 🏁 No more blocks to process. Shutting down.",
                worker_id
            );
            break;
        }
    }
}

// REPLACE your car_file_subscriber function with this one.

pub async fn car_file_subscriber(
    car_file_path: String,
    programs_to_monitor: Vec<String>,
    final_tx_sender: mpsc::Sender<UnifiedTransaction<'static>>,
) -> anyhow::Result<()> {
    let raw_node_capacity = channel_capacity_from_env("RAW_NODE_CHANNEL_CAPACITY", 2000);
    let block_capacity = channel_capacity_from_env("BLOCK_CHANNEL_CAPACITY", 100);
    println!(
        "[CAR] 🧮 Channel capacities -> raw nodes: {}, blocks: {}",
        raw_node_capacity, block_capacity
    );

    // --- Shared State for Logging ---
    let pre_filter_count = Arc::new(AtomicU64::new(0));
    let post_filter_count = Arc::new(AtomicU64::new(0));

    // --- Channels for the 3-stage pipeline ---
    // Channel 1: Reader -> Assembler (sends raw, unparsed nodes)
    let (raw_node_sender, raw_node_receiver) = mpsc::channel(raw_node_capacity);
    // Channel 2: Assembler -> Workers (sends fully formed blocks)
    let (block_sender, block_receiver) = mpsc::channel(block_capacity);

    let shared_block_receiver = Arc::new(TokioMutex::new(block_receiver));

    // --- Stage 1: Spawn the dedicated I/O Reader Thread ---
    let reader_pre_clone = Arc::clone(&pre_filter_count);
    let reader_post_clone = Arc::clone(&post_filter_count);
    let reader_handle = tokio::task::spawn_blocking(move || {
        if let Err(e) = reader_task(
            car_file_path,
            raw_node_sender,
            reader_pre_clone,
            reader_post_clone,
        ) {
            eprintln!("[Reader] 🔴 Task failed: {}", e);
        }
    });
    println!("[Main] 🚀 Spawned Stage 1: Raw I/O Reader thread.");

    // --- Stage 2: Spawn the Block Assembler Thread ---
    let assembler_handle = tokio::spawn(async move {
        block_assembler_task(raw_node_receiver, block_sender).await;
    });
    println!("[Main] 🚀 Spawned Stage 2: Block Assembler thread.");

    // --- Stage 3: Spawn a Pool of Transaction Worker Tasks ---
    let num_workers = env::var("CAR_WORKER_COUNT")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or_else(|| num_cpus::get().saturating_sub(2).max(1));
    println!(
        "[Main] 🚀 Spawning Stage 3: {} parallel transaction workers...",
        num_workers
    );

    let programs_of_interest = Arc::new(
        programs_to_monitor
            .into_iter()
            .filter_map(|s| Pubkey::from_str(&s).ok())
            .collect(),
    );

    let mut worker_handles = Vec::new();
    for i in 0..num_workers {
        let receiver_clone = Arc::clone(&shared_block_receiver);
        let sender_clone = final_tx_sender.clone();
        let interests_clone = Arc::clone(&programs_of_interest);
        let worker_pre_clone = Arc::clone(&pre_filter_count);
        let worker_post_clone = Arc::clone(&post_filter_count);

        let handle = tokio::spawn(async move {
            worker_task(
                i,
                receiver_clone,
                sender_clone,
                interests_clone,
                worker_pre_clone,
                worker_post_clone,
            )
            .await;
        });
        worker_handles.push(handle);
    }

    // Await completion of all stages
    if let Err(e) = reader_handle.await {
        eprintln!("[Main] 🔴 Reader thread panicked: {}", e);
    }
    if let Err(e) = assembler_handle.await {
        eprintln!("[Main] 🔴 Assembler thread panicked: {}", e);
    }
    for handle in worker_handles {
        if let Err(e) = handle.await {
            eprintln!("[Main] 🔴 Worker thread panicked: {}", e);
        }
    }

    println!("[Main] ✅ All CAR file tasks have completed.");
    Ok(())
}

// HELPER TO CONVERT CAR DATA -> UnifiedTransaction (NEW)
fn convert_car_tx_to_unified(
    slot: u64,
    block_time: u32,
    transaction_index: u32,
    versioned_tx: VersionedTransaction,
    meta: TransactionStatusMeta,
) -> Result<UnifiedTransaction<'static>> {
    let signature = versioned_tx.signatures.get(0).cloned().unwrap_or_default();
    // Convert RPC-style metadata to the Geyser format your existing code uses
    let geyser_meta =
        rpc_meta_to_grpc_meta(&meta.into(), versioned_tx.message.static_account_keys())?;

    let full_account_keys_list = build_full_account_keys(&versioned_tx, &Some(geyser_meta.clone()));
    let signers: Vec<String> = versioned_tx.message.static_account_keys()
        [..versioned_tx.message.header().num_required_signatures as usize]
        .iter()
        .map(|s| s.to_string())
        .collect();

    let pre_balances = build_balance_map_grpc(
        &full_account_keys_list,
        &geyser_meta.pre_token_balances,
        &geyser_meta.pre_balances,
    );
    let post_balances = build_balance_map_grpc(
        &full_account_keys_list,
        &geyser_meta.post_token_balances,
        &geyser_meta.post_balances,
    );

    let mut token_decimals = HashMap::new();
    for tb in geyser_meta
        .pre_token_balances
        .iter()
        .chain(geyser_meta.post_token_balances.iter())
    {
        if let Some(amount) = &tb.ui_token_amount {
            token_decimals
                .entry(tb.mint.clone())
                .or_insert(amount.decimals as u8);
        }
    }

    let static_tx = Box::leak(Box::new(versioned_tx));
    let static_meta = Box::leak(Box::new(geyser_meta));

    Ok(UnifiedTransaction {
        signature: signature.to_string(),
        slot,
        transaction_index,
        block_time,
        signers,
        error: static_meta
            .err
            .as_ref()
            .map(|e| String::from_utf8_lossy(&e.err).to_string()),
        account_keys: full_account_keys_list,
        formatted_instructions: format_transaction(static_tx, static_meta),
        logs: static_meta.log_messages.clone(),
        pre_balances,
        post_balances,
        token_decimals,
    })
}

// HELPER TO OPEN FILES (NEW, from Old Faithful example)
pub fn open_reader(path: &str) -> Result<Box<dyn Read + Send>, Box<dyn std::error::Error>> {
    if path.starts_with("http://") || path.starts_with("https://") {
        println!("[CARSubscriber] Opening URL: {}", path);
        let resp = reqwest::blocking::get(path)?.error_for_status()?;
        Ok(Box::new(resp))
    } else {
        println!("[CARSubscriber] Opening file: {}", path);
        let file = std::fs::File::open(path)?;
        Ok(Box::new(file))
    }
}
