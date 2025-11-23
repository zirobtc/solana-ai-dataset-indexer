use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;
use clickhouse::Client;
use redis::aio::MultiplexedConnection;
use redis::AsyncCommands;
use once_cell::sync::Lazy;
use solana_sdk::{native_token::LAMPORTS_PER_SOL, pubkey::Pubkey};
use std::str::FromStr;

use super::TransactionHandler;
use crate::{
    database::insert_rows,
    handlers::constants,
    spl_system_decoder,
    types::{EventPayload, EventType, TokenBalanceMap, TransferRow, UnifiedTransaction},
    utils::{get_asset_balances, get_priority_fee, TIP_ACCOUNTS},
};

const SYSTEM_PROGRAM_ID: Pubkey = solana_sdk::system_program::ID;
static TRANSFER_THRESHOLD_LAMPORTS: Lazy<u64> = Lazy::new(|| env_parse("SYSTEM_TRANSFER_THRESHOLD_LAMPORTS", 10_000_000_u64));
static SPAM_TRANSFER_COUNT_THRESHOLD: Lazy<usize> = Lazy::new(|| env_parse("SYSTEM_SPAM_TRANSFER_COUNT_THRESHOLD", 3_usize));

fn env_parse<T: FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<T>().ok())
        .unwrap_or(default)
}


pub struct SystemHandler {}

impl SystemHandler {
    pub fn new() -> Result<Self> {
        Ok(Self {})
    }
}

#[async_trait]
impl TransactionHandler for SystemHandler {
    fn name(&self) -> &'static str {
        "System"
    }

    fn is_of_interest(&self, tx: &UnifiedTransaction) -> bool {
        let mut is_pure_transfer = false;

        for log in &tx.logs {
            // Condition 1: Check for the top-level System Program invocation.
            if log == "Program 11111111111111111111111111111111 invoke [1]" {
                is_pure_transfer = true;
            }

            // Condition 2: Check for any program invocation that is NOT the System or Compute Budget program.
            if log.starts_with("Program ") && log.contains(" invoke [") {
                // A quick check to see if the log is for one of the two allowed programs.
                let is_allowed = log.contains("Program 11111111111111111111111111111111")
                    || log.contains("Program ComputeBudget111111111111111111111111111111");

                // If it's a program invocation and NOT an allowed one, it's a dirty transfer.
                if !is_allowed {
                    return false; // Exit immediately, no need to continue checking.
                }
            }
        }

        // Return the final state. It's only true if a system transfer was found and no dirty programs were.
        is_pure_transfer
    }

    async fn process_transaction(
        &self,
        tx: &UnifiedTransaction,
        native_price_usd: f64,
    ) -> Result<Vec<EventPayload>> {
        
        // --- NEW: EFFICIENT LOG-BASED SPAM FILTER ---

        // 1. Pre-scan logs to count System Program invocations.
        let system_transfer_count = tx.logs.iter()
            .filter(|log| log.starts_with("Program 11111111111111111111111111111111 invoke"))
            .count();

        if system_transfer_count == 0 {
            return Ok(Vec::new());
        }

        // 2. Apply the filter: Skip if there are too many transfers.
        if system_transfer_count > *SPAM_TRANSFER_COUNT_THRESHOLD {

            return Ok(Vec::new());
        }

        // --- END FILTER ---


        let mut transfer_rows: Vec<TransferRow> = Vec::new();
        let priority_fee = get_priority_fee(tx);

        let mut events: Vec<EventPayload> = Vec::new();

        for formatted_ix in &tx.formatted_instructions {
            let ix = &formatted_ix.instruction;

            // OPTIMIZATION: Check the program ID before attempting to decode.
            if let Some(program_id) = tx.account_keys.get(ix.program_id_index as usize) {
                if program_id == &SYSTEM_PROGRAM_ID {
                    // Only if it's a system instruction, try to decode it.
                    if let Ok(Some(spl_system_decoder::DecodedInstruction::SystemTransfer {
                        source,
                        destination,
                        lamports,
                    })) = spl_system_decoder::decode_instruction(ix, &tx.account_keys)
                    {

                        // 1. Ignore self-sent transfers
                        if source == destination {
                            continue;
                        }

                        // 2. Ignore transfers to blacklisted tip accounts
                        let destination_str = destination.to_string();
                        if TIP_ACCOUNTS.contains(&destination_str.as_str()) {
                            continue;
                        }
                        
                        if lamports < *TRANSFER_THRESHOLD_LAMPORTS {
                            continue; // Skip this instruction
                        }


                        let source_str = source.to_string();
                        let destination_str = destination.to_string();

                        let (source_pre_balance, source_post_balance) = get_asset_balances(tx, &source, constants::NATIVE_MINT);
                        let (destination_pre_balance, destination_post_balance) = get_asset_balances(tx, &destination, constants::NATIVE_MINT);

                        let transfer_rows = TransferRow {
                            signature: tx.signature.to_string(),
                            timestamp: tx.block_time,
                            slot: tx.slot,
                            success: tx.error.is_none(),
                            error: tx.error.clone(),
                            priority_fee: priority_fee,
                            source: source.to_string(),
                            destination: destination.to_string(),
                            mint_address: constants::NATIVE_MINT.to_string(),
                            amount: lamports,
                            amount_decimal: lamports as f64 / LAMPORTS_PER_SOL as f64,
                            source_balance: source_post_balance,
                            destination_balance: destination_post_balance
                        };
                        
                        events.push(
                            EventPayload { 
                                timestamp: tx.block_time,
                                event: EventType::Transfer(transfer_rows), 
                                balances: tx.post_balances.clone(), 
                                token_decimals: tx.token_decimals.clone() 
                            }
                        )
                    }
                }
            }
        }

        Ok(events)
    }
}

fn is_sol_transfer_activity(logs: &[String]) -> bool {
    logs.iter().any(|log| log.contains("Program 11111111111111111111111111111111 invoke [1]"))
}
