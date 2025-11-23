// File: src/utils.rs

use solana_sdk::native_token::LAMPORTS_PER_SOL;
use solana_sdk::system_instruction::SystemInstruction;
use solana_sdk::{
    instruction::CompiledInstruction, pubkey::Pubkey, transaction::VersionedTransaction,
};

use spl_associated_token_account::get_associated_token_address;
use yellowstone_grpc_proto::prelude::{InnerInstruction, Transaction, TransactionStatusMeta};

use crate::handlers::pump_fun_amm_handler::DecodedAction;
use crate::handlers::{TransactionInfo, constants};
use crate::types::{FormattedInstruction, UnifiedTransaction};
use borsh::BorshDeserialize;
use std::borrow::Cow;
use std::collections::HashMap;
use std::io;
use std::str::FromStr;

pub const TIP_ACCOUNTS: [&str; 11] = [
    "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5", // Jito Tip Account
    "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe", // Jito Tip Account
    "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY", // Jito Tip Account
    "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49", // Jito Tip Account
    "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh", // Jito Tip Account
    "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt", // Jito Tip Account
    "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL", // Jito Tip Account
    "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT", // Jito Tip Account
    "4iUgjMT8q2hNZnLuhpqZ1QtiV8deFPy2ajvvjEpKKgsS", // 0slot_dot_trade_tip23.sol (Axiom)
    "axmWxBPqgRmcBN2cV12quqaQzsk16SazVXq8397KFKu",  // (Axiom)
    "axmhpocX3hU7nT7KtsLBzNBR1Ur3HtU22Q5P313FREY",  // (Axiom)
];

pub fn deserialize_lax<'a, T: BorshDeserialize>(data: &'a [u8]) -> Result<T, io::Error> {
    // <-- The only change is here
    let mut reader = data;
    T::deserialize_reader(&mut reader)
}

/// Decodes and formats a transaction, grouping top-level instructions
/// with their corresponding inner instructions and logs. This function serves
/// as a universal pre-processor for any specific decoder.
pub fn format_transaction<'a>(
    tx: &'a VersionedTransaction,
    meta: &'a TransactionStatusMeta, // <-- This is the Geyser type, which is good.
) -> Vec<FormattedInstruction<'a>> {
    let mut formatted_instructions = Vec::new();
    let top_level_ixs = tx.message.instructions();

    let inner_ix_map: HashMap<u8, &'a Vec<InnerInstruction>> =
        if !meta.inner_instructions.is_empty() {
            meta.inner_instructions
                .iter()
                .map(|ix_block| (ix_block.index as u8, &ix_block.instructions))
                .collect()
        } else {
            HashMap::new()
        };

    let log_messages: Vec<&'a str> = meta.log_messages.iter().map(String::as_str).collect();

    let mut top_level_invoke_indices: Vec<usize> = log_messages
        .iter()
        .enumerate()
        .filter(|(_, log)| log.ends_with(" invoke [1]"))
        .map(|(i, _)| i)
        .collect();

    top_level_invoke_indices.push(log_messages.len());

    for (i, top_ix) in top_level_ixs.iter().enumerate() {
        // --- FIX STARTS HERE ---
        // The type of `inners` must match the type from `inner_ix_map`.
        let inners: Vec<Cow<'a, InnerInstruction>> = inner_ix_map // <-- CHANGE THIS TYPE
            .get(&(i as u8))
            .map_or(vec![], |ixs| ixs.iter().map(Cow::Borrowed).collect());
        // --- FIX ENDS HERE ---

        let log_start_index = top_level_invoke_indices
            .get(i)
            .cloned()
            .unwrap_or(log_messages.len());
        let log_end_index = top_level_invoke_indices
            .get(i + 1)
            .cloned()
            .unwrap_or(log_messages.len());

        let logs_for_ix: Vec<Cow<'a, str>> = log_messages
            .get(log_start_index..log_end_index)
            .unwrap_or(&[])
            .iter()
            .map(|s| Cow::Borrowed(*s))
            .collect();

        formatted_instructions.push(FormattedInstruction {
            instruction: Cow::Borrowed(top_ix),
            inner_instructions: inners,
            logs: logs_for_ix,
        });
    }

    formatted_instructions
}

pub fn get_asset_balances(
    tx: &UnifiedTransaction,
    owner_pubkey: &Pubkey,
    mint_pubkey_str: &str,
) -> (f64, f64) {
    // 2. Determine the correct account to check for the BASE token balance
    let wallet = if mint_pubkey_str == constants::NATIVE_MINT {
        // For SOL, the holder is the main wallet
        owner_pubkey
    } else {
        // For SPL tokens, calculate the ATA
        &get_associated_token_address(&owner_pubkey, &Pubkey::from_str(&mint_pubkey_str).unwrap())
    };

    let owner_pubkey_str = wallet.to_string();

    let pre_balance_map = tx.pre_balances.get(mint_pubkey_str);
    let post_balance_map = tx.post_balances.get(mint_pubkey_str);
    let decimals = tx.token_decimals.get(mint_pubkey_str).cloned().unwrap_or(9); // Default to 9 for SOL

    let pre_balance = pre_balance_map
        .and_then(|balances| balances.get(&owner_pubkey_str))
        .cloned()
        .unwrap_or(0);

    let post_balance = post_balance_map
        .and_then(|balances| balances.get(&owner_pubkey_str))
        .cloned()
        .unwrap_or(0);

    let divisor = 10f64.powi(decimals as i32);

    // println!("[DEBUG PUMP] Balance map {:#?}, user wallet {}, associated {}", tx.post_balances, owner_pubkey_str, owner_pubkey_str);

    (pre_balance as f64 / divisor, post_balance as f64 / divisor)
}

// --- GENERIC ANALYSIS (Can stay here or move to utils.rs) ---
pub fn build_full_account_keys(
    tx: &VersionedTransaction,
    meta: &Option<TransactionStatusMeta>,
) -> Vec<Pubkey> {
    if let Some(meta) = meta {
        let mut full_keys = tx.message.static_account_keys().to_vec();

        // Convert the loaded writable addresses (Vec<Vec<u8>>) to Pubkey
        let writable_addresses: Vec<Pubkey> = meta
            .loaded_writable_addresses
            .iter()
            .filter_map(|addr_bytes| Pubkey::try_from(addr_bytes.as_slice()).ok())
            .collect();

        // Convert the loaded readonly addresses (Vec<Vec<u8>>) to Pubkey
        let readonly_addresses: Vec<Pubkey> = meta
            .loaded_readonly_addresses
            .iter()
            .filter_map(|addr_bytes| Pubkey::try_from(addr_bytes.as_slice()).ok())
            .collect();

        full_keys.extend(writable_addresses);
        full_keys.extend(readonly_addresses);

        return full_keys;
    }

    tx.message.static_account_keys().to_vec()
}

// --- JITO
pub fn get_tip(tx: &UnifiedTransaction) -> f64 {
    // A list of known public Jito and private platform tip accounts.

    for formatted_ix in &tx.formatted_instructions {
        let ix = &formatted_ix.instruction;
        let program_id = &tx.account_keys[ix.program_id_index as usize];
        if program_id == &solana_sdk::system_program::id() {
            if let Ok(SystemInstruction::Transfer { lamports }) = bincode::deserialize(&ix.data) {
                if let Some(dest_idx) = ix.accounts.get(1) {
                    let destination_account = &tx.account_keys[*dest_idx as usize];
                    if TIP_ACCOUNTS.contains(&destination_account.to_string().as_str()) {
                        return lamports as f64 / LAMPORTS_PER_SOL as f64;
                    }
                }
            }
        }
    }
    0 as f64
}

pub fn get_priority_fee(tx: &UnifiedTransaction) -> f64 {
    let mut unit_price_micro_lamports = 0;
    let mut unit_limit = 0;

    let compute_budget_id = solana_sdk::compute_budget::id();

    for formatted_ix in &tx.formatted_instructions {
        let ix = &formatted_ix.instruction;
        let program_id = &tx.account_keys[ix.program_id_index as usize];
        if program_id == &compute_budget_id {
            if let Some((instruction_type, instruction_data)) = ix.data.split_first() {
                match instruction_type {
                    // SetComputeUnitLimit instruction
                    2 if instruction_data.len() == 4 => {
                        unit_limit =
                            u32::from_le_bytes(instruction_data.try_into().unwrap()) as u64;
                    }
                    // SetComputeUnitPrice instruction
                    3 if instruction_data.len() == 8 => {
                        unit_price_micro_lamports =
                            u64::from_le_bytes(instruction_data.try_into().unwrap());
                    }
                    _ => {}
                }
            }
        }
    }

    // print!("DEBUG -- Priority Fee Calculation: unit_price_micro_lamports = {}, unit_limit = {}\n", unit_price_micro_lamports, unit_limit);

    if unit_price_micro_lamports > 0 && unit_limit > 0 {
        let prio_fee: f64 = (unit_price_micro_lamports * unit_limit) as f64 / 1_000_000.0;

        (prio_fee / LAMPORTS_PER_SOL as f64)
    } else {
        0.0
    }
}

pub fn get_mev_protection(tx: &UnifiedTransaction, tip: f64) -> u8 {
    // Level 2 Check: Look for a dedicated MEV protection account.
    for key in &tx.account_keys {
        if key.to_string().starts_with("jitodontfront") {
            return 2; // Top-level MEV protection detected
        }
    }

    // Level 1 Check: If no dedicated account, check for a tip.
    if tip > 0.0 {
        return 1; // Tip-based MEV protection detected
    }

    // Level 0: No protection found.
    0
}

pub fn format_balance(
    raw_balance: f64,
    mint_address: &str,
    token_decimals: &HashMap<String, u8>,
) -> f64 {
    // Look up the decimals for the token mint.
    if let Some(&decimals) = token_decimals.get(mint_address) {
        // Convert the raw balance to a floating-point number.
        let raw_f64 = raw_balance as f64;

        // Calculate the divisor: 10 raised to the power of the decimals.
        let divisor = 10u64.pow(decimals as u32) as f64;

        // Return the formatted balance.
        raw_f64 / divisor
    } else {
        // Return 0.0 if the decimals are not found.
        0.0
    }
}

// Decoders helpers
pub fn find_account_pubkey_in_instruction(
    instruction_layouts: &HashMap<String, HashMap<String, usize>>,
    instruction_name: &str,
    account_name: &str,
    instruction_accounts: &[u8],
    global_account_keys: &[Pubkey],
) -> Option<Pubkey> {
    // Returns an Option, allowing us to check for failure
    instruction_layouts
        .get(instruction_name)
        .and_then(|layout| layout.get(account_name))
        .and_then(|&index| instruction_accounts.get(index as usize))
        .and_then(|&key_index| global_account_keys.get(key_index as usize))
        .cloned()
}

// Platforms
pub fn get_trading_platform(tx: &UnifiedTransaction) -> u8 {
    let trojan_router_id = Pubkey::from_str(constants::TROJAN_ROUTER_PROGRAM_ID).unwrap();
    let axiom_router_id = Pubkey::from_str(constants::AXIOM_ROUTER_PROGRAM_ID).unwrap();
    let bullx_router_id = Pubkey::from_str(constants::BULLX_ROUTER_PROGRAM_ID).unwrap();

    for formatted_ix in &tx.formatted_instructions {
        // --- Strategy 1: Check top-level instruction for a known Router Program ID ---
        let top_level_ix = &formatted_ix.instruction;
        let top_level_program_id = &tx.account_keys[top_level_ix.program_id_index as usize];

        if top_level_program_id == &axiom_router_id {
            return constants::PLATFORM_AXIOM;
        }
        if top_level_program_id == &bullx_router_id {
            return constants::PLATFORM_BULLX;
        }
        if top_level_program_id == &trojan_router_id {
            return constants::PLATFORM_TROJAN;
        }

        // --- Strategy 2: Check top-level instruction for a direct fee transfer ---
        if top_level_program_id == &solana_sdk::system_program::id() {
            if let Ok(SystemInstruction::Transfer { .. }) = bincode::deserialize(&top_level_ix.data)
            {
                if let Some(dest_idx) = top_level_ix.accounts.get(1) {
                    let destination_account = tx.account_keys[*dest_idx as usize].to_string();
                    if constants::AXIOM_FEE_ADDRESSES.contains(&destination_account.as_str()) {
                        return constants::PLATFORM_AXIOM;
                    }
                    if destination_account.starts_with("axm") {
                        return constants::PLATFORM_AXIOM;
                    }
                    if constants::BULLX_FEE_ADDRESSES.contains(&destination_account.as_str()) {
                        return constants::PLATFORM_BULLX;
                    }
                }
            }
        }

        // --- Strategy 3: Check INNER instructions for fee transfers ---
        for inner_ix in &formatted_ix.inner_instructions {
            let inner_program_id = &tx.account_keys[inner_ix.program_id_index as usize];
            if inner_program_id == &solana_sdk::system_program::id() {
                if let Ok(SystemInstruction::Transfer { .. }) = bincode::deserialize(&inner_ix.data)
                {
                    if let Some(dest_idx) = inner_ix.accounts.get(1) {
                        let destination_account = tx.account_keys[*dest_idx as usize].to_string();
                        if constants::AXIOM_FEE_ADDRESSES.contains(&destination_account.as_str()) {
                            return constants::PLATFORM_AXIOM;
                        }
                        if destination_account.starts_with("axm") {
                            return constants::PLATFORM_AXIOM;
                        }
                        if constants::BULLX_FEE_ADDRESSES.contains(&destination_account.as_str()) {
                            return constants::PLATFORM_BULLX;
                        }
                    }
                }
            }
        }
    }

    constants::UNKNOWN
}

/// Calculates the user's slippage tolerance and the trade's price impact.
/// This function is designed for standard `x * y = k` AMMs.
///
/// # Arguments
/// * `min_amount_out`: The minimum amount of output tokens the user is willing to accept (from instruction args).
/// * `pre_trade_input_reserves`: The liquidity pool's reserve of the INPUT token, *before* the trade.
/// * `pre_trade_output_reserves`: The liquidity pool's reserve of the OUTPUT token, *before* the trade.
/// * `actual_amount_in`: The actual amount of the input token taken from the user (from event).
/// * `actual_amount_out`: The actual amount of the output token sent to the user (from event).
///
/// # Returns
/// A tuple containing `(slippage_tolerance_pct, price_impact_pct)`.
pub fn calculate_amm_sell_metrics(
    min_amount_out: u64,
    pre_trade_input_reserves: u64,
    pre_trade_output_reserves: u64,
    actual_amount_in: u64,
    // --- START: THE FIX ---
    // Add a parameter for the slippage baseline, which comes from the event.
    base_amount_out_for_slippage: u64,
    // --- END: THE FIX ---
) -> (f64, f64) {
    if pre_trade_input_reserves == 0 || pre_trade_output_reserves == 0 {
        return (0.0, 0.0);
    }

    // Calculate EXPECTED and SPOT amounts for price impact (this logic is correct)
    let expected_amount_out = ((pre_trade_output_reserves as u128 * actual_amount_in as u128)
        / (pre_trade_input_reserves + actual_amount_in) as u128)
        as u64;
    let spot_amount_out = ((pre_trade_output_reserves as u128 * actual_amount_in as u128)
        / pre_trade_input_reserves as u128) as u64;

    // --- START: THE FIX ---
    // The slippage calculation now uses the explicitly provided baseline.
    let slippage_tolerance_pct = if base_amount_out_for_slippage > 0 {
        (base_amount_out_for_slippage as f64 - min_amount_out as f64)
            / base_amount_out_for_slippage as f64
    } else {
        0.0
    };
    // --- END: THE FIX ---

    // The price impact calculation remains correct.
    let price_impact_pct = if spot_amount_out > 0 {
        (spot_amount_out as f64 - expected_amount_out as f64) / spot_amount_out as f64
    } else {
        0.0
    };

    (slippage_tolerance_pct, price_impact_pct)
}

pub fn calculate_amm_buy_metrics(
    max_amount_in: u64,
    desired_amount_out: u64,
    pre_trade_input_reserves: u64,
    pre_trade_output_reserves: u64,
    actual_amount_in: u64,
    // --- START: THE FIX ---
    // Add a parameter for the slippage baseline. This allows the caller
    // to provide the correct value (e.g., user_quote_amount_in).
    base_amount_for_slippage: u64,
    // --- END: THE FIX ---
) -> (f64, f64) {
    if pre_trade_input_reserves == 0 || pre_trade_output_reserves < desired_amount_out {
        return (0.0, 0.0);
    }

    // Calculate SPOT COST for price impact (this is correct)
    let spot_cost = ((pre_trade_input_reserves as u128 * desired_amount_out as u128)
        / pre_trade_output_reserves as u128) as u64;

    // --- START: THE FIX ---
    // The slippage calculation now uses the explicitly provided baseline.
    let slippage_tolerance_pct = if base_amount_for_slippage > 0 {
        (max_amount_in as f64 - base_amount_for_slippage as f64) / base_amount_for_slippage as f64
    } else {
        0.0
    };
    // --- END: THE FIX ---

    // The price impact calculation is still correct.
    let price_impact_pct = if spot_cost > 0 {
        (actual_amount_in as f64 - spot_cost as f64) / spot_cost as f64
    } else {
        0.0
    };

    (slippage_tolerance_pct, price_impact_pct)
}
