use anyhow::Result;
use async_trait::async_trait;
use clickhouse::Client;
use once_cell::sync::Lazy;
use redis::AsyncCommands;
use redis::aio::MultiplexedConnection;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;

use super::TransactionHandler;
use crate::{
    database::insert_rows,
    handlers::constants,
    spl_system_decoder,
    types::{BurnRow, EventPayload, EventType, TransferRow, UnifiedTransaction},
    utils::{TIP_ACCOUNTS, get_asset_balances, get_priority_fee},
};
const SPL_TOKEN_PROGRAM_ID: Pubkey = spl_token::ID;

static SPAM_TRANSFER_COUNT_THRESHOLD: Lazy<usize> =
    Lazy::new(|| env_parse("SPL_TOKEN_SPAM_TRANSFER_COUNT_THRESHOLD", 3_usize));

fn env_parse<T: FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<T>().ok())
        .unwrap_or(default)
}

pub struct SplTokenHandler {}

impl SplTokenHandler {
    pub fn new() -> Result<Self> {
        Ok(Self {})
    }
}

#[async_trait]
impl TransactionHandler for SplTokenHandler {
    fn name(&self) -> &'static str {
        "SPL Token"
    }

    fn is_of_interest(&self, tx: &UnifiedTransaction) -> bool {
        let mut is_pure_transfer_candidate = false;
        let mut is_pure_burn_candidate = false;

        // Check for both Transfer and Burn instructions in a single pass
        for log in &tx.logs {
            if let Some(instruction) = log.strip_prefix("Program log: Instruction: ") {
                if matches!(instruction, "Transfer" | "TransferChecked") {
                    is_pure_transfer_candidate = true;
                } else if matches!(instruction, "Burn" | "BurnChecked") {
                    is_pure_burn_candidate = true;
                }
            }
        }

        // Now, validate the candidates based on purity.
        // If a burn was found, it's of interest, regardless of purity.
        if is_pure_burn_candidate {
            return true;
        }

        // If a transfer was found, check for purity.
        if is_pure_transfer_candidate {
            for log in &tx.logs {
                // Check for any top-level invocation that is NOT the SPL Token or Compute Budget program.
                if log.contains(" invoke [1]")
                    && !log.contains("Program TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
                    && !log.contains("Program ComputeBudget111111111111111111111111111111")
                {
                    // Found a "dirty" top-level invocation, so this is NOT a pure transfer.
                    return false;
                }
            }
            // If the second loop completes without finding a dirty program, it's a pure transfer.
            return true;
        }

        // If neither a burn nor a transfer was found, it's not of interest.
        false
    }

    async fn process_transaction(
        &self,
        tx: &UnifiedTransaction,
        native_price_usd: f64,
    ) -> Result<Vec<EventPayload>> {
        let mut events: Vec<EventPayload> = Vec::new();

        let mut transfer_count = 0;
        let mut has_burn = false;
        for log in &tx.logs {
            if let Some(instruction) = log.strip_prefix("Program log: Instruction: ") {
                if matches!(instruction, "Transfer" | "TransferChecked") {
                    transfer_count += 1;
                } else if matches!(instruction, "Burn" | "BurnChecked") {
                    has_burn = true;
                }
            }
        }

        // 2. Apply the filter: Skip only if there are too many transfers AND no burns.
        if !has_burn && transfer_count > *SPAM_TRANSFER_COUNT_THRESHOLD {
            return Ok(Vec::new());
        }

        let mut transfer_rows: Vec<TransferRow> = Vec::new();
        let mut burn_rows: Vec<BurnRow> = Vec::new();
        let priority_fee = get_priority_fee(tx);

        // CORRECTED: Iterate through formatted_instructions
        for formatted_ix in &tx.formatted_instructions {
            let ix = &formatted_ix.instruction; // Get the top-level instruction

            if let Some(program_id) = tx.account_keys.get(ix.program_id_index as usize) {
                if program_id == &SPL_TOKEN_PROGRAM_ID {
                    if let Ok(Some(decoded)) =
                        spl_system_decoder::decode_instruction(ix, &tx.account_keys)
                    {
                        let event_type = match decoded {
                            spl_system_decoder::DecodedInstruction::SplTokenTransferChecked {
                                source,
                                destination,
                                amount,
                                mint,
                                decimals,
                                ..
                            } => {
                                if source == destination {
                                    continue;
                                }
                                let destination_str = destination.to_string();
                                if TIP_ACCOUNTS.contains(&destination_str.as_str()) {
                                    continue;
                                }

                                let (source_pre_balance, source_post_balance) =
                                    get_asset_balances(tx, &source, constants::NATIVE_MINT);
                                let (destination_pre_balance, destination_post_balance) =
                                    get_asset_balances(tx, &destination, constants::NATIVE_MINT);

                                let transfer_row = TransferRow {
                                    signature: tx.signature.to_string(),
                                    timestamp: tx.block_time,
                                    slot: tx.slot,
                                    success: tx.error.is_none(),
                                    error: tx.error.clone(),
                                    priority_fee,
                                    source: source.to_string(),
                                    destination: destination.to_string(),
                                    mint_address: mint.to_string(),
                                    amount,
                                    amount_decimal: amount as f64 / 10f64.powi(decimals.into()),
                                    destination_balance: destination_post_balance,
                                    source_balance: source_post_balance,
                                };
                                Some(EventType::Transfer(transfer_row))
                            }
                            spl_system_decoder::DecodedInstruction::SplTokenTransfer {
                                source,
                                destination,
                                authority,
                                amount,
                            } => {
                                if source == destination {
                                    continue;
                                }
                                let destination_str = destination.to_string();
                                if TIP_ACCOUNTS.contains(&destination_str.as_str()) {
                                    continue;
                                }

                                let mut found_mint_str: Option<String> = None;
                                let mut found_decimals: Option<u8> = None;

                                // --- THE KEY CHANGE: We go back to using `source` for the lookup. ---
                                // This now works because the balance map is keyed by Token Accounts.
                                for (mint_key, balance_map) in &tx.pre_balances {
                                    if mint_key == constants::NATIVE_MINT {
                                        continue;
                                    }

                                    if balance_map.contains_key(&source.to_string()) {
                                        found_mint_str = Some(mint_key.clone());
                                        if let Some(d) = tx.token_decimals.get(mint_key) {
                                            found_decimals = Some(*d);
                                        }
                                        break;
                                    }
                                }

                                if let (Some(mint_address), Some(decimals)) =
                                    (found_mint_str, found_decimals)
                                {
                                    let (_, source_post_balance) =
                                        get_asset_balances(tx, &source, &mint_address);
                                    let (_, destination_post_balance) =
                                        get_asset_balances(tx, &destination, &mint_address);

                                    let transfer_rows = TransferRow {
                                        signature: tx.signature.to_string(),
                                        timestamp: tx.block_time,
                                        slot: tx.slot,
                                        success: tx.error.is_none(),
                                        error: tx.error.clone(),
                                        priority_fee,
                                        source: source.to_string(),
                                        destination: destination.to_string(),
                                        mint_address,
                                        amount,
                                        amount_decimal: amount as f64 / 10f64.powi(decimals.into()),
                                        destination_balance: destination_post_balance,
                                        source_balance: source_post_balance,
                                    };
                                    Some(EventType::Transfer(transfer_rows))
                                } else {
                                    println!(
                                        "[splToken] WARN: Could not determine mint for transfer. Source account {} was not found in any pre_balance map.",
                                        source
                                    );
                                    None
                                }
                            }
                            spl_system_decoder::DecodedInstruction::SplTokenBurn {
                                source_account,
                                mint,
                                amount,
                                ..
                            } => {
                                let decimals = tx
                                    .token_decimals
                                    .get(&mint.to_string())
                                    .cloned()
                                    .unwrap_or(0);

                                // Get the post-burn token balance of the source account
                                let (source_pre_balance, source_post_balance) =
                                    get_asset_balances(tx, &source_account, &mint.to_string());

                                let burn_rows = BurnRow {
                                    signature: tx.signature.to_string(),
                                    timestamp: tx.block_time,
                                    slot: tx.slot,
                                    success: tx.error.is_none(),
                                    error: tx.error.clone(),
                                    priority_fee,
                                    source: source_account.to_string(),
                                    mint_address: mint.to_string(),
                                    amount,
                                    amount_decimal: amount as f64 / 10f64.powi(decimals as i32),
                                    source_balance: source_post_balance,
                                };
                                Some(EventType::Burn(burn_rows))
                            }
                            _ => None,
                        };

                        if let Some(event) = event_type {
                            events.push(EventPayload {
                                timestamp: tx.block_time,
                                event: event,
                                balances: tx.post_balances.clone(),
                                token_decimals: tx.token_decimals.clone(),
                            })
                        }
                    }
                }
            }
        }

        Ok(events)
    }
}
