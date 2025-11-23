// File: src/handlers/raydium_cpmm_handler.rs

use crate::database::insert_rows;
use crate::handlers::{Idl, anchor_event_discriminator, constants};
use crate::types::{FeeCollectionRow, LiquidityRow, PoolCreationRow, TradeRow};
use crate::utils::{FormattedInstruction, get_asset_balances};
use anchor_lang::{AnchorDeserialize, AnchorSerialize};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use base64;
use borsh::BorshDeserialize;
use clickhouse::{Client, Row};
use redis::aio::MultiplexedConnection;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use solana_sdk::instruction::CompiledInstruction;
use solana_sdk::pubkey::Pubkey;
use std::{borrow::Cow, collections::HashMap, fs, str::FromStr};

use super::{TransactionHandler, TransactionInfo};

// --- INSTRUCTION ARG STRUCTS ---
#[derive(Debug, BorshDeserialize, Serialize, Clone)]
pub struct SwapBaseInputArgs {
    pub amount_in: u64,
    pub minimum_amount_out: u64,
}

#[derive(Debug, BorshDeserialize, Serialize, Clone)]
pub struct SwapBaseOutputArgs {
    pub max_amount_in: u64,
    pub amount_out: u64,
}

#[derive(Debug, BorshDeserialize, Serialize, Clone)]
pub struct CollectFeeArgs {
    pub amount_0_requested: u64,
    pub amount_1_requested: u64,
}

// NEW: Correctly added from IDL
#[derive(Debug, BorshDeserialize, Serialize, Clone)]
pub struct InitializeArgs {
    pub init_amount_0: u64,
    pub init_amount_1: u64,
    pub open_time: u64,
}

// NEW: Correctly added from IDL
#[derive(Debug, BorshDeserialize, Serialize, Clone)]
pub struct DepositArgs {
    pub lp_token_amount: u64,
    pub maximum_token_0_amount: u64,
    pub maximum_token_1_amount: u64,
}

// NEW: Correctly added from IDL
#[derive(Debug, BorshDeserialize, Serialize, Clone)]
pub struct WithdrawArgs {
    pub lp_token_amount: u64,
    pub minimum_token_0_amount: u64,
    pub minimum_token_1_amount: u64,
}

// --- EVENT STRUCTS ---
#[derive(AnchorSerialize, AnchorDeserialize, Clone, Debug, Serialize)]
pub struct SwapEvent {
    pub pool_id: Pubkey,
    pub input_vault_before: u64,
    pub output_vault_before: u64,
    pub input_amount: u64,
    pub output_amount: u64,
    pub input_transfer_fee: u64,
    pub output_transfer_fee: u64,
    pub base_input: bool,
    pub input_mint: Pubkey,
    pub output_mint: Pubkey,
    pub trade_fee: u64,
    pub creator_fee: u64,
    pub creator_fee_on_input: bool,
}

#[derive(AnchorSerialize, AnchorDeserialize, Clone, Debug, Serialize)]
pub struct LpChangeEvent {
    pub pool_id: Pubkey,
    pub lp_amount_before: u64,
    pub token_0_vault_before: u64,
    pub token_1_vault_before: u64,
    pub token_0_amount: u64,
    pub token_1_amount: u64,
    pub token_0_transfer_fee: u64,
    pub token_1_transfer_fee: u64,
    pub change_type: u8,
}

#[derive(Debug, Serialize, Clone)]
#[serde(tag = "type", content = "data")]
pub enum DecodedInstruction {
    SwapBaseInput(SwapBaseInputArgs),
    SwapBaseOutput(SwapBaseOutputArgs),
    Deposit,
    Withdraw,
    CollectProtocolFee(CollectFeeArgs),
    CollectFundFee(CollectFeeArgs),
    Initialize,
}

#[derive(Debug, Serialize, Clone)]
#[serde(tag = "type", content = "data")]
pub enum DecodedEvent {
    Swap(SwapEvent),
    LpChange(LpChangeEvent),
}

#[derive(Debug)]
pub struct DecodedAction<'a> {
    pub instruction: DecodedInstruction,
    pub event: Option<DecodedEvent>,
    pub instruction_accounts: &'a [u8],
    pub instruction_index: u16,
}

pub struct RaydiumCpmmDecoder {
    program_id: Pubkey,
    instruction_discriminators: HashMap<[u8; 8], String>,
    event_discriminators: HashMap<[u8; 8], String>,
    pub instruction_layouts: HashMap<String, HashMap<String, usize>>,
}

impl RaydiumCpmmDecoder {
    pub fn new() -> Result<Self> {
        let idl_str = fs::read_to_string("raydium_cpmm.json")?;
        let idl: Idl = serde_json::from_str(&idl_str)?;
        let program_id = Pubkey::from_str(&idl.address)?;
        let mut instruction_discriminators = HashMap::new();
        let mut instruction_layouts = HashMap::new();

        for ix in idl.instructions {
            if ix.discriminator.len() == 8 {
                let mut disc = [0u8; 8];
                disc.copy_from_slice(&ix.discriminator);
                let account_map: HashMap<String, usize> = ix
                    .accounts
                    .into_iter()
                    .enumerate()
                    .map(|(index, acc)| (acc.name, index))
                    .collect();
                instruction_layouts.insert(ix.name.clone(), account_map);
                instruction_discriminators.insert(disc, ix.name);
            }
        }
        let event_discriminators = idl
            .events
            .into_iter()
            .map(|event| (anchor_event_discriminator(&event.name), event.name))
            .collect();
        Ok(Self {
            program_id,
            instruction_discriminators,
            event_discriminators,
            instruction_layouts,
        })
    }

    pub fn decode_transaction<'a>(
        &self,
        formatted_instructions: &'a [FormattedInstruction<'a>],
        account_keys: &[Pubkey],
    ) -> Result<Vec<DecodedAction<'a>>> {
        let mut all_actions = Vec::new();
        for (i, formatted_ix) in formatted_instructions.iter().enumerate() {
            let event = self.find_event_from_logs(&formatted_ix.logs)?;

            if account_keys.get(formatted_ix.instruction.program_id_index as usize)
                == Some(&self.program_id)
            {
                if let Some(instruction) =
                    self.decode_instruction(&formatted_ix.instruction.data)?
                {
                    all_actions.push(DecodedAction {
                        instruction,
                        event: event.clone(),
                        instruction_accounts: &formatted_ix.instruction.accounts,
                        instruction_index: i as u16,
                    });
                }
            }

            for inner_ix_cow in &formatted_ix.inner_instructions {
                let inner_ix = inner_ix_cow.as_ref();
                if account_keys.get(inner_ix.program_id_index as usize) == Some(&self.program_id) {
                    if let Some(instruction) = self.decode_instruction(&inner_ix.data)? {
                        all_actions.push(DecodedAction {
                            instruction,
                            event: event.clone(),
                            instruction_accounts: &inner_ix.accounts,
                            instruction_index: i as u16,
                        });
                    }
                }
            }
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
                "swap_base_input" => {
                    if let Ok(args) = SwapBaseInputArgs::try_from_slice(instruction_data) {
                        return Ok(Some(DecodedInstruction::SwapBaseInput(args)));
                    }
                }
                "swap_base_output" => {
                    if let Ok(args) = SwapBaseOutputArgs::try_from_slice(instruction_data) {
                        return Ok(Some(DecodedInstruction::SwapBaseOutput(args)));
                    }
                }
                "deposit" => return Ok(Some(DecodedInstruction::Deposit)),
                "withdraw" => return Ok(Some(DecodedInstruction::Withdraw)),
                "initialize" => return Ok(Some(DecodedInstruction::Initialize)),
                "collect_protocol_fee" => {
                    if let Ok(args) = CollectFeeArgs::try_from_slice(instruction_data) {
                        return Ok(Some(DecodedInstruction::CollectProtocolFee(args)));
                    }
                }
                "collect_fund_fee" => {
                    if let Ok(args) = CollectFeeArgs::try_from_slice(instruction_data) {
                        return Ok(Some(DecodedInstruction::CollectFundFee(args)));
                    }
                }
                _ => {}
            }
        }
        Ok(None)
    }

    // CORRECTED: Search logs in reverse to find the last (and most relevant) event first.
    fn find_event_from_logs<'a>(&self, logs: &[Cow<'a, str>]) -> Result<Option<DecodedEvent>> {
        for log in logs.iter().rev() {
            if let Some(data_str) = log.strip_prefix("Program data: ") {
                if let Ok(decoded_data) = base64::decode(data_str) {
                    if let Some(event) = self.decode_single_event(&decoded_data)? {
                        return Ok(Some(event));
                    }
                }
            }
        }
        Ok(None)
    }

    fn decode_single_event(&self, data: &[u8]) -> Result<Option<DecodedEvent>> {
        if data.len() < 8 {
            return Ok(None);
        }
        let mut event_disc = [0u8; 8];
        event_disc.copy_from_slice(&data[..8]);
        let event_data = &data[8..];
        if let Some(name) = self.event_discriminators.get(&event_disc) {
            match name.as_str() {
                "SwapEvent" => {
                    if let Ok(event) = SwapEvent::try_from_slice(event_data) {
                        return Ok(Some(DecodedEvent::Swap(event)));
                    }
                }
                "LpChangeEvent" => {
                    if let Ok(event) = LpChangeEvent::try_from_slice(event_data) {
                        return Ok(Some(DecodedEvent::LpChange(event)));
                    }
                }
                _ => {}
            }
        }
        Ok(None)
    }
}
pub struct RaydiumCpmmHandler {
    decoder: RaydiumCpmmDecoder,
}

impl RaydiumCpmmHandler {
    pub fn new() -> Result<Self> {
        Ok(Self {
            decoder: RaydiumCpmmDecoder::new()?,
        })
    }
}

#[async_trait]
impl TransactionHandler for RaydiumCpmmHandler {
    fn name(&self) -> &'static str {
        "Raydium CPMM"
    }
    fn is_of_interest(&self, tx_info: &TransactionInfo) -> bool {
        let mut is_invoke = false;
        for log in tx_info.logs {
            if log.contains("Program CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C invoke [") {
                is_invoke = true;
                continue;
            }
            if is_invoke {
                if let Some(name) = log.strip_prefix("Program log: Instruction: ") {
                    if matches!(
                        name,
                        "Initialize"
                            | "SwapBaseInput"
                            | "SwapBaseOutput"
                            | "Deposit"
                            | "Withdraw"
                            | "CollectFundFee"
                    ) {
                        return true;
                    }
                }
            }
        }
        false
    }

    async fn handle_transaction(
        &self,
        tx_info: &TransactionInfo,
        db_client: &Client,
        redis_conn: &MultiplexedConnection,

        native_price_usd: f64,
    ) -> Result<()> {
        let decoded_data = self
            .decoder
            .decode_transaction(tx_info.formatted_instructions, tx_info.account_keys)?;
        if decoded_data.is_empty() {
            return Ok(());
        }
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis() as u64;

        let mut pool_creation_rows = Vec::new();
        let mut trade_rows = Vec::new();
        let mut liquidity_rows = Vec::new();
        let mut fee_collection_rows = Vec::new();

        let all_tx_keys = tx_info.account_keys;

        for action in decoded_data {
            let get_acc_str = |ix_name: &str, acc_name: &str| -> String {
                self.decoder
                    .instruction_layouts
                    .get(ix_name)
                    .and_then(|l| l.get(acc_name))
                    .and_then(|&i| action.instruction_accounts.get(i))
                    .and_then(|&ki| all_tx_keys.get(ki as usize))
                    .map(|pk| pk.to_string())
                    .unwrap_or_default()
            };

            match action.instruction {
                DecodedInstruction::Initialize => {
                    println!("Raydium CPMM initialize");
                    pool_creation_rows.push(PoolCreationRow {
                        updated_at: now_ms,
                        signature: tx_info.signature.to_string(),
                        timestamp: tx_info.block_time,
                        slot: tx_info.slot,
                        success: tx_info.is_success,
                        error: if tx_info.is_success {
                            None
                        } else {
                            tx_info.error.clone()
                        },
                        priority_fee: tx_info.priority_fee.unwrap_or(0),
                        protocol: constants::PROTOCOL_RAYDIUM_CPMM,
                        creator_address: tx_info.signer.to_string(),
                        pool_address: get_acc_str("initialize", "pool_state"),
                        base_address: get_acc_str("initialize", "token_0_mint"),
                        quote_address: get_acc_str("initialize", "token_1_mint"),
                        lp_token_address: get_acc_str("initialize", "lp_mint"),
                        initial_base_liquidity: None,
                        initial_quote_liquidity: None,
                        base_decimals: None,
                        quote_decimals: None,
                    });
                }
                DecodedInstruction::SwapBaseInput(args) => {
                    if let Some(DecodedEvent::Swap(e)) = action.event {
                        println!("Raydium CPMM swap event {:#?}", e);
                        println!("Raydium CPMM swap {:#?}", args);
                        let base_mint_str = get_acc_str("swap_base_input", "token_0_mint");
                        let quote_mint_str = get_acc_str("swap_base_input", "token_1_mint");
                        let maker = Pubkey::from_str(&tx_info.signer)?;
                        let (pre_base, post_base) =
                            get_asset_balances(tx_info, &maker, &base_mint_str);
                        let (pre_quote, post_quote) =
                            get_asset_balances(tx_info, &maker, &quote_mint_str);
                        let (base_amount, quote_amount) = (e.input_amount, e.output_amount);
                        let (base_reserves, quote_reserves) =
                            (e.input_vault_before, e.output_vault_before);
                        let base_decimals = 6;
                        let quote_decimals = 9;
                        let total = quote_amount as f64 / 10f64.powi(quote_decimals);
                        let price = if base_reserves > 0 {
                            (quote_reserves as f64 / 10f64.powi(quote_decimals))
                                / (base_reserves as f64 / 10f64.powi(base_decimals))
                        } else {
                            0.0
                        };
                        let slippage = if e.output_amount > 0 {
                            (e.output_amount as f64 - args.minimum_amount_out as f64)
                                / e.output_amount as f64
                        } else {
                            0.0
                        };

                        trade_rows.push(TradeRow {
                            updated_at: now_ms,
                            signature: tx_info.signature.to_string(),
                            timestamp: tx_info.block_time,
                            slot: tx_info.slot,
                            success: tx_info.is_success,
                            error: if tx_info.is_success {
                                None
                            } else {
                                tx_info.error.clone()
                            },
                            priority_fee: tx_info.priority_fee.unwrap_or(0),
                            bribe_fee: tx_info.tip.unwrap_or(0),
                            mev_protection: tx_info.mev_protection,
                            maker: tx_info.signer.to_string(),
                            pre_base_balance: pre_base,
                            post_base_balance: post_base,
                            pre_quote_balance: pre_quote,
                            post_quote_balance: post_quote,
                            trade_type: constants::TRADE_TYPE_SELL,
                            protocol: constants::PROTOCOL_RAYDIUM_CPMM,
                            platform: constants::PLATFORM_RAYDIUM,
                            pool_address: e.pool_id.to_string(),
                            base_address: base_mint_str,
                            quote_address: quote_mint_str,
                            slippage: slippage as f32,
                            base_amount,
                            quote_amount,
                            price,
                            price_usd: price * native_price_usd,
                            total_supply: 0,
                            total,
                            total_usd: total * native_price_usd,
                        });
                    }
                }
                DecodedInstruction::SwapBaseOutput(args) => {
                    if let Some(DecodedEvent::Swap(e)) = action.event {
                        println!("Raydium CPMM swap sell {:#?}", args);
                        println!("Raydium CPMM swap sell {:#?}", e);
                        let quote_mint_str = get_acc_str("swap_base_output", "token_0_mint");
                        let base_mint_str = get_acc_str("swap_base_output", "token_1_mint");
                        let maker = Pubkey::from_str(&tx_info.signer)?;
                        let (pre_base, post_base) =
                            get_asset_balances(tx_info, &maker, &base_mint_str);
                        let (pre_quote, post_quote) =
                            get_asset_balances(tx_info, &maker, &quote_mint_str);
                        let (quote_amount, base_amount) = (e.input_amount, e.output_amount);
                        let (quote_reserves, base_reserves) =
                            (e.input_vault_before, e.output_vault_before);
                        let base_decimals = 6;
                        let quote_decimals = 9;
                        let total = quote_amount as f64 / 10f64.powi(quote_decimals);
                        let price = if base_reserves > 0 {
                            (quote_reserves as f64 / 10f64.powi(quote_decimals))
                                / (base_reserves as f64 / 10f64.powi(base_decimals))
                        } else {
                            0.0
                        };
                        let slippage = if e.input_amount > 0 {
                            (e.input_amount as f64 - args.max_amount_in as f64)
                                / e.input_amount as f64
                        } else {
                            0.0
                        };

                        trade_rows.push(TradeRow {
                            updated_at: now_ms,
                            signature: tx_info.signature.to_string(),
                            timestamp: tx_info.block_time,
                            slot: tx_info.slot,
                            success: tx_info.is_success,
                            error: if tx_info.is_success {
                                None
                            } else {
                                tx_info.error.clone()
                            },
                            priority_fee: tx_info.priority_fee.unwrap_or(0),
                            bribe_fee: tx_info.tip.unwrap_or(0),
                            mev_protection: tx_info.mev_protection,
                            maker: tx_info.signer.to_string(),
                            pre_base_balance: pre_base,
                            post_base_balance: post_base,
                            pre_quote_balance: pre_quote,
                            post_quote_balance: post_quote,
                            trade_type: constants::TRADE_TYPE_BUY,
                            protocol: constants::PROTOCOL_RAYDIUM_CPMM,
                            platform: constants::PLATFORM_RAYDIUM,
                            pool_address: e.pool_id.to_string(),
                            base_address: base_mint_str,
                            quote_address: quote_mint_str,
                            slippage: slippage as f32,
                            base_amount,
                            quote_amount,
                            price,
                            price_usd: price * native_price_usd,
                            total_supply: 0,
                            total,
                            total_usd: total * native_price_usd,
                        });
                    }
                }
                DecodedInstruction::Deposit | DecodedInstruction::Withdraw => {
                    if let Some(DecodedEvent::LpChange(e)) = action.event {
                        println!("Raydium CPMM lp change {:#?}", e);
                        liquidity_rows.push(LiquidityRow {
                            updated_at: now_ms,
                            signature: tx_info.signature.to_string(),
                            timestamp: tx_info.block_time,
                            slot: tx_info.slot,
                            success: tx_info.is_success,
                            error: if tx_info.is_success {
                                None
                            } else {
                                tx_info.error.clone()
                            },
                            priority_fee: tx_info.priority_fee.unwrap_or(0),
                            protocol: constants::PROTOCOL_RAYDIUM_CPMM,
                            change_type: e.change_type,
                            lp_provider: tx_info.signer.to_string(),
                            pool_address: e.pool_id.to_string(),
                            base_amount: e.token_0_amount,
                            quote_amount: e.token_1_amount,
                        });
                    }
                }
                DecodedInstruction::CollectFundFee(ref args) => {
                    println!("Raydium CPMM fee collection {:#?}", args);
                    fee_collection_rows.push(FeeCollectionRow {
                        updated_at: now_ms,
                        signature: tx_info.signature.to_string(),
                        timestamp: tx_info.block_time,
                        slot: tx_info.slot,
                        success: tx_info.is_success,
                        error: if tx_info.is_success {
                            None
                        } else {
                            tx_info.error.clone()
                        },
                        priority_fee: tx_info.priority_fee.unwrap_or(0),
                        protocol: constants::PROTOCOL_RAYDIUM_CPMM,
                        pool_address: get_acc_str("collect_fund_fee", "pool_state"),
                        recipient_address: get_acc_str("collect_fund_fee", "fee_owner"),
                        token_0_mint_address: get_acc_str("collect_fund_fee", "token_0_mint"),
                        token_0_amount: args.amount_0_requested,
                        token_1_mint_address: Some(get_acc_str("collect_fund_fee", "token_1_mint")),
                        token_1_amount: Some(args.amount_1_requested),
                    });
                }
                DecodedInstruction::CollectProtocolFee(_) => {}
            }
        }
        insert_rows(
            db_client,
            "pool_creations",
            pool_creation_rows,
            self.name(),
            "pool creation",
        )
        .await?;
        insert_rows(db_client, "trades", trade_rows, self.name(), "swap").await?;
        insert_rows(
            db_client,
            "liquidity",
            liquidity_rows,
            self.name(),
            "liquidity change",
        )
        .await?;
        insert_rows(
            db_client,
            "fee_collections",
            fee_collection_rows,
            self.name(),
            "fee collection",
        )
        .await?;
        Ok(())
    }
}
