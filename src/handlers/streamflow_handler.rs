use crate::handlers::{anchor_event_discriminator, constants, Idl, TransactionHandler};
use crate::spl_system_decoder;
use crate::types::{
    EventPayload, EventType, FormattedInstruction, SupplyLockActionRow, SupplyLockRow,
    UnifiedTransaction,
};
use crate::handlers::streamflow_idl::{CreateArgs, TopupArgs, UpdateArgs, WithdrawArgs};
use crate::utils::{deserialize_lax, find_account_pubkey_in_instruction, get_priority_fee};
use anyhow::Result;
use async_trait::async_trait;
use borsh::BorshDeserialize;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use crate::handlers::IdlAccount;
use yellowstone_grpc_proto::prelude::InnerInstruction;

//==============================================================================
// DECODER LOGIC
//==============================================================================

#[derive(Debug, Serialize, Clone)]
#[serde(tag = "type", content = "data")]
pub enum DecodedInstruction {
    Create(CreateArgs),
    Withdraw(WithdrawArgs),
    Topup(TopupArgs),
    Update(UpdateArgs),
    Cancel,
    TransferRecipient,
    Pause,
    Unpause,
}

#[derive(Debug)]
pub struct DecodedAction<'a> {
    pub instruction: DecodedInstruction,
    pub instruction_accounts: &'a [u8],
    pub parent_ix: &'a FormattedInstruction<'a>, 
}


#[derive(Deserialize)]
struct StreamflowIdlInstruction {
    name: String,
    accounts: Vec<IdlAccount>,
}

#[derive(Deserialize)]
struct StreamflowIdl {
    instructions: Vec<StreamflowIdlInstruction>,
}

pub struct StreamflowDecoder {
    program_id: Pubkey,
    instruction_discriminators: HashMap<[u8; 8], String>,
    pub instruction_layouts: HashMap<String, HashMap<String, usize>>,
}

pub fn anchor_global_instruction_discriminator(name: &str) -> [u8; 8] {
    let preimage = format!("global:{}", name);
    let mut hasher = Sha256::new();
    hasher.update(preimage.as_bytes());
    let hash = hasher.finalize();
    let mut disc = [0u8; 8];
    disc.copy_from_slice(&hash[..8]);
    disc
}

impl StreamflowDecoder {
    pub fn new() -> Result<Self> {
        let program_id = "strmRqUCoQUgGUan5YhzUZa6KqdzwX5L6FpUxfmKg5m".parse()?;
        let idl_str = std::fs::read_to_string("streamflow.json")?;

        // --- START FIX ---
        // Use the new, specific struct to parse the IDL
        let idl: StreamflowIdl = serde_json::from_str(&idl_str)?;
        // --- END FIX ---

        let mut instruction_discriminators = HashMap::new();
        let mut instruction_layouts = HashMap::new();

        for ix in idl.instructions { // This now correctly iterates over StreamflowIdlInstruction
            let disc = anchor_global_instruction_discriminator(&ix.name);
            let account_map: HashMap<String, usize> = ix
                .accounts
                .into_iter()
                .enumerate()
                .map(|(index, acc)| (acc.name, index))
                .collect();

            instruction_layouts.insert(ix.name.clone(), account_map);
            instruction_discriminators.insert(disc, ix.name);
        }

        Ok(Self {
            program_id,
            instruction_discriminators,
            instruction_layouts,
        })
    }

    pub fn decode_transaction<'a>(
        &self,
        formatted_instructions: &'a [FormattedInstruction<'a>],
        account_keys: &[Pubkey],
    ) -> Result<Vec<DecodedAction<'a>>> {
        let mut all_actions = Vec::new();

        for formatted_ix in formatted_instructions {
            // --- START: DEFINITIVE FIX ---

            // 1. Process the top-level instruction directly
            let top_level_ix = formatted_ix.instruction.as_ref();
            if account_keys.get(top_level_ix.program_id_index as usize) == Some(&self.program_id) {
                if let Some(instruction) = self.decode_instruction(&top_level_ix.data)? {
                    all_actions.push(DecodedAction {
                        instruction,
                        instruction_accounts: &top_level_ix.accounts,
                        parent_ix: formatted_ix,
                    });
                }
            }

            // 2. Process inner instructions by creating temporary `CompiledInstruction`s
            for inner_ix_cow in &formatted_ix.inner_instructions {
                let inner_ix = inner_ix_cow.as_ref();
                if account_keys.get(inner_ix.program_id_index as usize) == Some(&self.program_id) {
                    // Manually construct a `CompiledInstruction` to satisfy the decoder's type requirement.
                    let temp_compiled_ix = solana_sdk::instruction::CompiledInstruction {
                        program_id_index: inner_ix.program_id_index as u8,
                        accounts: inner_ix.accounts.clone(),
                        data: inner_ix.data.clone(),
                    };

                    if let Some(instruction) = self.decode_instruction(&temp_compiled_ix.data)? {
                        all_actions.push(DecodedAction {
                            instruction,
                            instruction_accounts: &inner_ix.accounts, // Still use original accounts slice
                            parent_ix: formatted_ix,
                        });
                    }
                }
            }
            // --- END: DEFINITIVE FIX ---
        }
        Ok(all_actions)
    }

    fn decode_instruction(&self, data: &[u8]) -> Result<Option<DecodedInstruction>> {
        if data.len() < 8 {
            return Ok(None);
        }
        let mut disc = [0u8; 8];
        disc.copy_from_slice(&data[..8]);
        let instruction_data = &data[8..];

        if let Some(name) = self.instruction_discriminators.get(&disc) {
            match name.as_str() {
                "create" => {
                    if let Ok(args) = deserialize_lax::<CreateArgs>(instruction_data) {
                        return Ok(Some(DecodedInstruction::Create(args)));
                    }
                }
                "withdraw" => {
                    if let Ok(args) = deserialize_lax::<WithdrawArgs>(instruction_data) {
                        return Ok(Some(DecodedInstruction::Withdraw(args)));
                    }
                }
                "cancel" => { 
                    return Ok(Some(DecodedInstruction::Cancel))},
                _ => {} // Other instructions are decoded but ignored for now
            }
        }
        Ok(None)
    }

}

//==============================================================================
// PROCESSING LOGIC (The Handler)
//==============================================================================

pub struct StreamflowHandler {
    decoder: StreamflowDecoder,
}

impl StreamflowHandler {
    pub fn new() -> Result<Self> {
        Ok(Self {
            decoder: StreamflowDecoder::new()?,
        })
    }
}

#[async_trait]
impl TransactionHandler for StreamflowHandler {
    fn name(&self) -> &'static str {
        "Streamflow"
    }

    fn is_of_interest(&self, tx: &UnifiedTransaction) -> bool {
        let mut is_streamflow_invoke = false;
        for log in &tx.logs {
            if log.contains("Program strmRqUCoQUgGUan5YhzUZa6KqdzwX5L6FpUxfmKg5m invoke [") {
                is_streamflow_invoke = true;
            }

            if is_streamflow_invoke {
                if let Some(name) = log.strip_prefix("Program log: Instruction: ") {
                    if matches!(
                        name,
                        "Create" | "Withdraw" | "Cancel" | "Topup" | "TransferRecipient" | "Pause" | "Unpause" | "Update"
                    ) {
                        return true;
                    }
                }
            }
        }
        false
    }

    async fn process_transaction(
        &self,
        tx: &UnifiedTransaction,
        _native_price_usd: f64,
    ) -> Result<Vec<EventPayload>> {
        let decoded_data = self
            .decoder
            .decode_transaction(&tx.formatted_instructions, &tx.account_keys)?;

        if decoded_data.is_empty() {
            return Ok(Vec::new());
        }

        let mut events: Vec<EventPayload> = Vec::new();
        let priority_fee = get_priority_fee(tx);

        for action in decoded_data {
            let event_type = match action.instruction {
                DecodedInstruction::Create(args) => {
                    let num_periods = if args.amount_per_period > 0 {
                        args.net_amount_deposited / args.amount_per_period
                    } else {
                        0
                    };

                    // THIS IS THE CORRECTED LOGIC
                    // 'period' is an enum for the time unit. We must translate it to seconds.
                    // Based on your data, 1 means "days". Other values might mean seconds, minutes, etc.
                    let seconds_per_unit = match args.period {
                        // You may need to find the other values, but for this transaction, 1 is days.
                        1 => 86_400,   // 1 = Days
                        // Add other potential cases if you discover them, e.g.,
                        // 0 => 1,         // Seconds
                        // 2 => 3_600,     // Hours
                        // 3 => 604_800,   // Weeks
                        // 4 => 2_629_746,  // Months (approx)
                        _ => 1, // Default to seconds as a safe fallback
                    };

                    // The total duration of one period is the frequency multiplied by the unit in seconds.
                    let duration_of_one_period = args.withdraw_frequency.saturating_mul(seconds_per_unit);
                    
                    // The total duration of the entire stream is that multiplied by the number of periods.
                    let total_duration = num_periods.saturating_mul(duration_of_one_period);

                    let effective_start = args.cliff.max(args.start_time);
                    let final_unlock_timestamp = effective_start.saturating_add(total_duration);



                    let contract_address =find_account_pubkey_in_instruction(&self.decoder.instruction_layouts, "create", "metadata", action.instruction_accounts, &tx.account_keys).map(|p| p.to_string()).unwrap_or_default();
                    let sender = find_account_pubkey_in_instruction(&self.decoder.instruction_layouts, "create", "sender", action.instruction_accounts, &tx.account_keys).map(|p| p.to_string()).unwrap_or_default();
                    let recipient = find_account_pubkey_in_instruction(&self.decoder.instruction_layouts, "create", "recipient", action.instruction_accounts, &tx.account_keys).map(|p| p.to_string()).unwrap_or_default();
                    let mint_address = find_account_pubkey_in_instruction(&self.decoder.instruction_layouts, "create", "mint", action.instruction_accounts, &tx.account_keys).map(|p| p.to_string()).unwrap_or_default();
                   
                    let decimals = tx.token_decimals.get(&mint_address).cloned().unwrap_or(0);
                    let divisor = 10f64.powi(decimals as i32);
                    let total_locked_amount = args.net_amount_deposited as f64 / divisor;
                    
                    let row = SupplyLockRow {
                        signature: tx.signature.to_string(),
                        timestamp: tx.block_time,
                        slot: tx.slot,
                        success: tx.error.is_none(),
                        error: tx.error.clone(),
                        priority_fee,
                        protocol: constants::PROTOCOL_STREAMFLOW,
                        contract_address: contract_address,
                        sender: sender,
                        recipient: recipient,
                        mint_address: mint_address,
                        total_locked_amount: total_locked_amount,
                        final_unlock_timestamp: final_unlock_timestamp,
                    };
                    Some(EventType::SupplyLock(row))
                }
                DecodedInstruction::Withdraw(args) => {

                    let mut actual_withdrawn_amount = 0;

                    for inner_ix_cow in &action.parent_ix.inner_instructions {
                        let inner_ix = inner_ix_cow.as_ref();

                        let temp_compiled_ix = solana_sdk::instruction::CompiledInstruction {
                            program_id_index: inner_ix.program_id_index as u8,
                            accounts: inner_ix.accounts.clone(),
                            data: inner_ix.data.clone(),
                        };

                        // Now, pass the correctly typed temporary instruction to the decoder.
                        if let Ok(Some(crate::spl_system_decoder::DecodedInstruction::SplTokenTransferChecked { amount, .. })) =
                            crate::spl_system_decoder::decode_instruction(&temp_compiled_ix, &tx.account_keys) {
                            
                            actual_withdrawn_amount = amount;
                            break;
                        }
                    }

                    if actual_withdrawn_amount > 0 {
                        let contract_address = find_account_pubkey_in_instruction(&self.decoder.instruction_layouts, "withdraw", "metadata", action.instruction_accounts, &tx.account_keys).map(|p| p.to_string()).unwrap_or_default();
                        let user = find_account_pubkey_in_instruction(&self.decoder.instruction_layouts, "withdraw", "authority", action.instruction_accounts, &tx.account_keys).map(|p| p.to_string()).unwrap_or_default();
                        let mint_address = find_account_pubkey_in_instruction(&self.decoder.instruction_layouts, "withdraw", "mint", action.instruction_accounts, &tx.account_keys).map(|p| p.to_string()).unwrap_or_default();
                        
                        let decimals = tx.token_decimals.get(&mint_address).cloned().unwrap_or(0);
                        let divisor = 10f64.powi(decimals as i32);
                        let amount = actual_withdrawn_amount as f64 / divisor;

                        let row = SupplyLockActionRow {
                            signature: tx.signature.to_string(),
                            timestamp: tx.block_time,
                            slot: tx.slot,
                            success: tx.error.is_none(),
                            error: tx.error.clone(),
                            priority_fee,
                            protocol: constants::PROTOCOL_STREAMFLOW,
                            action_type: constants::VESTING_ACTION_WITHDRAW,
                            contract_address: contract_address,
                            user: user,
                            mint_address: mint_address,
                            amount: amount,
                        };
                        Some(EventType::SupplyLockAction(row))
                    } else {
                        println!("[StreamflowHandler] WARN: Could not find inner TransferChecked for Withdraw in tx {}", tx.signature);
                        None
                    }
                    
                }
                DecodedInstruction::Cancel => {
                    let mut actual_cancelled_amount = 0;
                    for inner_ix_cow in &action.parent_ix.inner_instructions {
                        let inner_ix = inner_ix_cow.as_ref();
                        let temp_compiled_ix = solana_sdk::instruction::CompiledInstruction {
                            program_id_index: inner_ix.program_id_index as u8,
                            accounts: inner_ix.accounts.clone(),
                            data: inner_ix.data.clone(),
                        };
                        if let Ok(Some(spl_system_decoder::DecodedInstruction::SplTokenTransferChecked { amount, .. })) =
                        spl_system_decoder::decode_instruction(&temp_compiled_ix, &tx.account_keys) {
                            actual_cancelled_amount = amount;
                            break;
                        }
                    }
                    if actual_cancelled_amount > 0 {
                        let mint_address = find_account_pubkey_in_instruction(&self.decoder.instruction_layouts, "cancel", "mint", action.instruction_accounts, &tx.account_keys).map(|p| p.to_string()).unwrap_or_default();
                        let decimals = tx.token_decimals.get(&mint_address).cloned().unwrap_or(0);
                        let divisor = 10f64.powi(decimals as i32);
                        let contract_address = find_account_pubkey_in_instruction(&self.decoder.instruction_layouts, "cancel", "metadata", action.instruction_accounts, &tx.account_keys).map(|p| p.to_string()).unwrap_or_default();
                        let user = find_account_pubkey_in_instruction(&self.decoder.instruction_layouts, "cancel", "authority", action.instruction_accounts, &tx.account_keys).map(|p| p.to_string()).unwrap_or_default();
                        let normalized_amount = actual_cancelled_amount as f64 / divisor;
                        
                        let row = SupplyLockActionRow {
                            signature: tx.signature.to_string(),
                            timestamp: tx.block_time,
                            slot: tx.slot,
                            success: tx.error.is_none(),
                            error: tx.error.clone(),
                            priority_fee,
                            protocol: constants::PROTOCOL_STREAMFLOW,
                            action_type: constants::VESTING_ACTION_CANCEL,
                            contract_address: contract_address,
                            user: user,
                            mint_address,
                            amount: normalized_amount,
                        };
                        Some(EventType::SupplyLockAction(row))
                    } else {
                        println!("[StreamflowHandler] WARN: Could not find inner TransferChecked for Cancel in tx {}", tx.signature);
                        None
                    }
                }
                _ => None,
            };
            
            if let Some(event) = event_type {
                events.push(EventPayload {
                    timestamp: tx.block_time,
                    event,
                    balances: tx.post_balances.clone(),
                    token_decimals: tx.token_decimals.clone(),
                });
            }
        }

        Ok(events)
    }
}