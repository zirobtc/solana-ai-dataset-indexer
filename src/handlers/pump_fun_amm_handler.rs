// File: src/handlers/pump_fun_amm_handler.rs

use crate::database::insert_rows;
use crate::handlers::pump_fun_amm_idl::{BuyArgs, BuyEvent, CollectCoinCreatorFeeEvent, CreatePoolEvent, DepositArgs, DepositEvent, SellArgs, SellEvent, WithdrawArgs, WithdrawEvent};
use crate::handlers::{Idl, anchor_event_discriminator, constants};
use crate::types::{EventPayload, EventType, FeeCollectionRow, FormattedInstruction, LiquidityRow, PoolCreationRow, TradeRow, UnifiedTransaction}; 
use crate::utils::{calculate_amm_buy_metrics, calculate_amm_sell_metrics, deserialize_lax, find_account_pubkey_in_instruction, get_asset_balances, get_mev_protection, get_priority_fee, get_tip, get_trading_platform};
use anchor_lang::{AnchorDeserialize, AnchorSerialize};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use borsh::BorshDeserialize;
use clickhouse::{Client, Row};
use redis::AsyncCommands;
use redis::aio::MultiplexedConnection;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use solana_sdk::instruction::CompiledInstruction;
use solana_sdk::native_token::LAMPORTS_PER_SOL;
use solana_sdk::pubkey::Pubkey;
use spl_associated_token_account::get_associated_token_address;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fs;
use std::str::FromStr;
use yellowstone_grpc_proto::prelude::InnerInstruction;

use super::{TransactionHandler};

// --- DECODED DATA STRUCTS (NEW) ---
#[derive(Debug, Serialize, Clone)]
#[serde(tag = "type", content = "data")]
pub enum DecodedInstruction {
    Buy(BuyArgs),
    Sell(SellArgs),
    Deposit(DepositArgs),
    Withdraw(WithdrawArgs),
    CreatePool,
    CollectCoinCreatorFee,
}

#[derive(Debug, Serialize, Clone)]
#[serde(tag = "type", content = "data")]
pub enum DecodedEvent {
    Buy(BuyEvent),
    Sell(SellEvent),
    Deposit(DepositEvent),
    Withdraw(WithdrawEvent),
    CreatePool(CreatePoolEvent),
    CollectCoinCreatorFee(CollectCoinCreatorFeeEvent),
}

#[derive(Debug)]
pub struct DecodedAction<'a> {
    pub instruction: DecodedInstruction,
    pub event: Option<DecodedEvent>,
    pub instruction_accounts: &'a [u8],
    pub instruction_index: u16,
}

// --- DECODER ---
pub struct PumpFunAmmDecoder {
    instruction_discriminators: HashMap<[u8; 8], String>,
    event_discriminators: HashMap<[u8; 8], String>,
    program_id: Pubkey,
    instruction_layouts: HashMap<String, HashMap<String, usize>>,
}

impl PumpFunAmmDecoder {
    pub fn new() -> Result<Self> {
        let idl_str = fs::read_to_string("pump_fun_amm.json")?;
        let idl: Idl = serde_json::from_str(&idl_str)?;

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

        let program_id = Pubkey::from_str(&idl.address)?;
        Ok(Self {
            instruction_discriminators,
            event_discriminators,
            program_id,
            instruction_layouts,
        })
    }

    fn find_first_event<'a>(
        &self,
        inner_instructions: &[Cow<'a, InnerInstruction>],
        account_keys: &[Pubkey],
    ) -> Result<Option<DecodedEvent>> {
        for (idx, inner_ix_cow) in inner_instructions.iter().enumerate() {
            let inner_ix = inner_ix_cow.as_ref();
            if let Some(inner_program_id) = account_keys.get(inner_ix.program_id_index as usize) {
                // ANCHOR EVENTS ARE CPIs TO THE SAME PROGRAM
                if inner_program_id == &self.program_id {
                    if let Some(event) = self.decode_single_event(&inner_ix.data)? {
                        return Ok(Some(event));
                    }
                }
            }
        }
        Ok(None)
    }

    // --- FINAL DECODER WITH EXHAUSTIVE LOGGING ---
    pub fn decode_transaction<'a>(
        &'a self,
        formatted_instructions: &'a [FormattedInstruction<'a>],
        account_keys: &[Pubkey],
    ) -> Result<Vec<DecodedAction<'a>>> {
        let mut all_actions = Vec::new();

        for (i, formatted_ix) in formatted_instructions.iter().enumerate() {
            let top_level_program_id = &account_keys[formatted_ix.instruction.program_id_index as usize];

            // --- CASE 1: DIRECT PUMP.FUN CALL (TOP-LEVEL) ---
            if top_level_program_id == &self.program_id {
                if let Some(instruction) = self.decode_instruction(&formatted_ix.instruction.data)? {
                    let event = self.find_first_event(&formatted_ix.inner_instructions, account_keys)?;
                    all_actions.push(DecodedAction {
                        instruction,
                        event,
                        instruction_accounts: &formatted_ix.instruction.accounts,
                        instruction_index: i as u16,
                    });
                } else {
                }
                continue;
            }

            // --- CASE 2: ROUTED PUMP.FUN CALL (INNER-LEVEL) ---
            if !formatted_ix.inner_instructions.is_empty() {
                
                let mut instruction_info: Option<(DecodedInstruction, &'a [u8], usize)> = None;

                for (inner_idx, inner_ix_cow) in formatted_ix.inner_instructions.iter().enumerate() {
                    let inner_ix = inner_ix_cow.as_ref();
                    if let Some(inner_program_id) = account_keys.get(inner_ix.program_id_index as usize) {
                        if inner_program_id == &self.program_id {
                            if let Some(instr) = self.decode_instruction(&inner_ix.data)? {
                                instruction_info = Some((instr, &inner_ix.accounts, inner_idx));
                                break; 
                            }
                        }
                    } else {
                    }
                }

                if let Some((instruction, accounts, instruction_index)) = instruction_info {
                    let remaining_inner_instructions = &formatted_ix.inner_instructions[instruction_index + 1..];
                    let event = self.find_first_event(remaining_inner_instructions, account_keys)?;


                    all_actions.push(DecodedAction {
                        instruction,
                        event,
                        instruction_accounts: accounts,
                        instruction_index: i as u16,
                    });
                } else {
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
                "buy" => {
                    // This will succeed if the data is 16 bytes, 18 bytes, or even longer.
                    if let Ok(args) = deserialize_lax::<BuyArgs>(instruction_data) {
                        return Ok(Some(DecodedInstruction::Buy(args)));
                    }
                }
                "sell" => {
                    if let Ok(args) = deserialize_lax::<SellArgs>(instruction_data) {
                        return Ok(Some(DecodedInstruction::Sell(args)));
                    }
                }
                "deposit" => {
                    if let Ok(args) = deserialize_lax::<DepositArgs>(instruction_data) {
                        return Ok(Some(DecodedInstruction::Deposit(args)));
                    }
                }
                "withdraw" => {
                    if let Ok(args) = deserialize_lax::<WithdrawArgs>(instruction_data) {
                        return Ok(Some(DecodedInstruction::Withdraw(args)));
                    }
                }
                "create_pool" => return Ok(Some(DecodedInstruction::CreatePool)),
                "collect_coin_creator_fee" => {
                    return Ok(Some(DecodedInstruction::CollectCoinCreatorFee));
                }
                _ => {}
            }
        }
        Ok(None)
    }

    fn find_event_in_inner_instructions<'a>(
        &self,
        inner_instructions: &[Cow<'a, InnerInstruction>],
    ) -> Result<Option<DecodedEvent>> {
        for instruction_cow in inner_instructions {
            if let Some(event) = self.decode_single_event(&instruction_cow.data)? {
                return Ok(Some(event));
            }
        }
        Ok(None)
    }

    fn decode_single_event(&self, data: &[u8]) -> Result<Option<DecodedEvent>> {
        // Anchor event format: 8-byte anchor discriminator + 8-byte event discriminator + event data
        if data.len() < 16 {
            return Ok(None);
        }
        // The first 8 bytes are the anchor log discriminator: `a9-46-e9-28-a2-3f-23-e5`
        // We only need the event discriminator which is bytes 8-16.
        let mut event_disc = [0u8; 8];
        event_disc.copy_from_slice(&data[8..16]);
        let event_data = &data[16..];

        if let Some(name) = self.event_discriminators.get(&event_disc) {
            match name.as_str() {
                "BuyEvent" => {
                    if let Ok(event) = BuyEvent::try_from_slice(event_data) {
                        return Ok(Some(DecodedEvent::Buy(event)));
                    }
                }
                "SellEvent" => {
                    if let Ok(event) = SellEvent::try_from_slice(event_data) {
                        return Ok(Some(DecodedEvent::Sell(event)));
                    }
                }
                "DepositEvent" => {
                    if let Ok(event) = DepositEvent::try_from_slice(event_data) {
                        return Ok(Some(DecodedEvent::Deposit(event)));
                    }
                }
                "WithdrawEvent" => {
                    if let Ok(event) = WithdrawEvent::try_from_slice(event_data) {
                        return Ok(Some(DecodedEvent::Withdraw(event)));
                    }
                }
                "CreatePoolEvent" => {
                    if let Ok(event) = CreatePoolEvent::try_from_slice(event_data) {
                        return Ok(Some(DecodedEvent::CreatePool(event)));
                    }
                }
                "CollectCoinCreatorFeeEvent" => {
                    if let Ok(event) = CollectCoinCreatorFeeEvent::try_from_slice(event_data) {
                        return Ok(Some(DecodedEvent::CollectCoinCreatorFee(event)));
                    }
                }
                _ => {}
            }
        }
        Ok(None)
    }
}

pub struct PumpFunAmmHandler {
    decoder: PumpFunAmmDecoder,
}

impl PumpFunAmmHandler {
    pub fn new() -> Result<Self> {
        Ok(Self {
            decoder: PumpFunAmmDecoder::new()?,
        })
    }
}

#[async_trait]
impl TransactionHandler for PumpFunAmmHandler {
    fn name(&self) -> &'static str {
        "Pump.fun AMM"
    }

    fn is_of_interest(&self, tx: &UnifiedTransaction) -> bool {
        let mut is_amm_invoke = false;
        // The logs are now directly on the UnifiedTransaction
        for log in &tx.logs {
            if log.contains("Program pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA invoke [") {
                is_amm_invoke = true;
                continue;
            }
            if log.contains("Program log: Bonding curve already migrated") {
                break;
            }
            if is_amm_invoke {
                if let Some(name) = log.strip_prefix("Program log: Instruction: ") {
                    if matches!(name, "Buy" | "Sell" | "Deposit" | "Withdraw" | "CreatePool" | "CollectCoinCreatorFee") {
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
        native_price_usd: f64,
    ) -> Result<Vec<EventPayload>> {
        let decoded_data = self
            .decoder
            .decode_transaction(&tx.formatted_instructions, &tx.account_keys)?;

        if decoded_data.is_empty() {
            return Ok(Vec::new());
        }

        let mut events: Vec<EventPayload> = Vec::new();


        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis() as u64;

        // Transaction Extras
        let priority_fee: f64 = get_priority_fee(tx);
        let tip= get_tip(tx);
        let mev_protection = get_mev_protection(tx, tip);
        let platform = get_trading_platform(tx);

        // Initialize vectors for our new generic row types
        let mut pool_creation_rows: Vec<PoolCreationRow> = Vec::new();
        let mut trade_rows: Vec<TradeRow> = Vec::new();
        let mut liquidity_rows: Vec<LiquidityRow> = Vec::new();

        for action in decoded_data {

            if action.event.is_none() {
            }

            let event_type = match action.instruction {
                DecodedInstruction::CreatePool => {
                    if tx.error.is_some() {
                        let row = PoolCreationRow {
                            signature: tx.signature.to_string(), timestamp: tx.block_time, slot: tx.slot,
                            success: false, error: tx.error.clone(), priority_fee,
                            protocol: constants::PROTOCOL_PUMPFUN_AMM,
                            creator_address: tx.signers.get(0).cloned().unwrap_or_default(),
                            pool_address: "".to_string(),
                            base_address: find_account_pubkey_in_instruction(&self.decoder.instruction_layouts, "create_pool", "base_mint", action.instruction_accounts, &tx.account_keys).map(|p| p.to_string()).unwrap_or_default(),
                            quote_address: find_account_pubkey_in_instruction(&self.decoder.instruction_layouts, "create_pool", "quote_mint", action.instruction_accounts, &tx.account_keys).map(|p| p.to_string()).unwrap_or_default(),
                            lp_token_address: "".to_string(), initial_base_liquidity: None,
                            initial_quote_liquidity: None, base_decimals: None, quote_decimals: None,
                        };
                        Some(EventType::PoolCreation(row))
                    } else if let Some(DecodedEvent::CreatePool(e)) = action.event {
                        let creator_pk_str =  e.creator.to_string();
                        let quote_mint_str = e.quote_mint.to_string();
                        let row = PoolCreationRow {
                            signature: tx.signature.to_string(),
                            timestamp: tx.block_time,
                            slot: tx.slot,
                            success: true,
                            error: None,
                            priority_fee: priority_fee,
                            protocol: constants::PROTOCOL_PUMPFUN_AMM,
                            creator_address: creator_pk_str,
                            pool_address: e.pool.to_string(),
                            base_address: e.base_mint.to_string(),
                            quote_address: quote_mint_str,
                            lp_token_address: e.lp_mint.to_string(),
                            initial_base_liquidity: Some(e.base_amount_in),
                            initial_quote_liquidity: Some(e.quote_amount_in),
                            base_decimals: Some(e.base_mint_decimals),
                            quote_decimals: Some(e.quote_mint_decimals),
                        };
                        Some(EventType::PoolCreation(row))
                    } else {
                        None
                    }
                }
                DecodedInstruction::Buy(a) => {                    
                    if tx.error.is_some() {
                        let trade_row = TradeRow {
                            signature: tx.signature.to_string(), timestamp: tx.block_time, slot: tx.slot,
                            transaction_index: tx.transaction_index,
                            instruction_index: action.instruction_index,
                            success: false, error: tx.error.clone(), priority_fee, bribe_fee: tip,
                            coin_creator_fee: 0.0, mev_protection, maker: tx.signers.get(0).cloned().unwrap_or_default(),
                            base_balance: 0.0, quote_balance: 0.0,
                            trade_type: constants::TRADE_TYPE_BUY,
                            protocol: constants::PROTOCOL_PUMPFUN_AMM, platform,
                            pool_address: find_account_pubkey_in_instruction(&self.decoder.instruction_layouts, "buy", "pool", action.instruction_accounts, &tx.account_keys).map(|p| p.to_string()).unwrap_or_default(),
                            base_address: find_account_pubkey_in_instruction(&self.decoder.instruction_layouts, "buy", "base_mint", action.instruction_accounts, &tx.account_keys).map(|p| p.to_string()).unwrap_or_default(),
                            quote_address: find_account_pubkey_in_instruction(&self.decoder.instruction_layouts, "buy", "quote_mint", action.instruction_accounts, &tx.account_keys).map(|p| p.to_string()).unwrap_or_default(),
                            slippage: 0.0, price_impact: 0.0, base_amount: 0, quote_amount: 0,
                            price: 0.0, price_usd: 0.0, total: 0.0, total_usd: 0.0,
                        };
                        Some(EventType::Trade(trade_row))
                    } else if let Some(DecodedEvent::Buy(e)) = action.event {


                        // Accounts Retrieval
                        let maker = e.user;

                        let potential_base_address = find_account_pubkey_in_instruction(
                            &self.decoder.instruction_layouts,
                            "buy",
                            "base_mint",
                            action.instruction_accounts,
                            &tx.account_keys,
                        ).map(|pk| pk.to_string()).unwrap_or_default();

                        let potential_quote_address = find_account_pubkey_in_instruction(
                            &self.decoder.instruction_layouts,
                            "buy",
                            "quote_mint",
                            action.instruction_accounts,
                            &tx.account_keys,
                        ).map(|pk| pk.to_string()).unwrap_or_default();

                        if potential_base_address.is_empty() || potential_quote_address.is_empty() {
                            continue;
                        }

                        // FIX 2: Handle flipped pairs and non-whitelisted assets.
                        let whitelist = [constants::NATIVE_MINT, constants::USDC_MINT, constants::USDT_MINT, constants::USD1_MINT];
                        let is_flipped = whitelist.contains(&potential_base_address.as_str());

                        if !whitelist.contains(&potential_quote_address.as_str()) && !is_flipped {
                            println!("[pumpAmm] Skipping buy trade with non-whitelisted pair: {}/{}", potential_base_address, potential_quote_address);
                            continue;
                        }
                        
                        // Correctly assign addresses based on the is_flipped flag.
                        let (base_address, quote_address) = if is_flipped {
                            (potential_quote_address, potential_base_address)
                        } else {
                            (potential_base_address, potential_quote_address)
                        };

                        // Get token decimals using the now-correct addresses.
                        let base_decimals = tx.token_decimals.get(&base_address).cloned().unwrap_or(0);
                        let quote_decimals = tx.token_decimals.get(&quote_address).cloned().unwrap_or(0);

                        // Get pre and post balances.
                        let (_, post_base) = get_asset_balances(tx, &maker, &base_address);
                        let (_, post_quote) = get_asset_balances(tx, &maker, &quote_address);


                        
                        // Normalize amounts from the event.
                        // For a buy, the user PAYS quote_amount_in and RECEIVES base_amount_out.
                        let base_normalized = e.base_amount_out as f64 / 10f64.powi(base_decimals as i32);
                        let quote_normalized = e.quote_amount_in as f64 / 10f64.powi(quote_decimals as i32);

                        // Price = Quote / Base
                        let price = if base_normalized > 0.0 {
                            quote_normalized / base_normalized
                        } else {
                            0.0
                        };
                        let quote_price_usd = if quote_address.as_str() == constants::NATIVE_MINT {
                            native_price_usd
                        } else if [constants::USDC_MINT, constants::USDT_MINT, constants::USD1_MINT].contains(&quote_address.as_str()) {
                            1.0
                        } else {
                            0.0
                        };

                        // Slippage Calculation
                        // 1. Reconstruct PRE-TRADE reserves.
                        let actual_quote_in = e.quote_amount_in;
                        let actual_base_out = e.base_amount_out;
                        let post_trade_quote_reserves = e.pool_quote_token_reserves;
                        let post_trade_base_reserves = e.pool_base_token_reserves;
                        
                        let (pre_trade_quote_reserves, pre_trade_base_reserves) = (
                            post_trade_quote_reserves.checked_sub(actual_quote_in),
                            post_trade_base_reserves.checked_add(actual_base_out), 
                        );

                        let (pre_trade_quote_reserves, pre_trade_base_reserves) = 
                            match (pre_trade_quote_reserves, pre_trade_base_reserves) {
                                (Some(q), Some(b)) => (q, b),
                                _ => {
                                    println!("[pumpAmm] WARN: Overflow detected calculating pre-trade reserves for tx {}. Skipping trade.", tx.signature);
                                    continue;
                                }
                            };


                        // 2. Map fields to the generic function arguments.
                        let max_amount_in = a.max_quote_amount_in;
                        let desired_amount_out = a.base_amount_out;
                        
                        // --- START: THE FIX ---
                        // Get the correct baseline for the slippage calculation from the event.
                        let base_amount_for_slippage = e.user_quote_amount_in;
                        // --- END: THE FIX ---

                        // 3. Call the corrected generic "buy" function.
                        let (mut slippage_tolerance_pct, mut price_impact_pct) = (0.0, 0.0);

                        if !tx.error.is_some() {
                            (slippage_tolerance_pct, price_impact_pct) = calculate_amm_buy_metrics(
                                max_amount_in,
                                desired_amount_out,
                                pre_trade_quote_reserves,
                                pre_trade_base_reserves,
                                actual_quote_in,
                                base_amount_for_slippage, 
                            );
                        } else {
                            // If the transaction has an error, assign default values.
                            slippage_tolerance_pct = 0.0;
                            price_impact_pct = 0.0;
                        }

                        let coin_creator_fee =( e.coin_creator_fee as f64 / LAMPORTS_PER_SOL as f64) ;



                        let trade = TradeRow {
                            signature: tx.signature.to_string(),
                            timestamp: tx.block_time,
                            slot: tx.slot,
                            transaction_index: tx.transaction_index,
                            instruction_index: action.instruction_index,
                            success: tx.error.is_none(),
                            error: tx.error.clone(),
                            priority_fee: priority_fee,
                            bribe_fee: tip,
                            coin_creator_fee: coin_creator_fee,
                            mev_protection: mev_protection,
                            maker: maker.to_string(),
                            base_balance: post_base,
                            quote_balance: post_quote,
                            trade_type: constants::TRADE_TYPE_BUY,
                            protocol: constants::PROTOCOL_PUMPFUN_AMM,
                            platform: platform,
                            pool_address: e.pool.to_string(),
                            base_address,
                            quote_address,
                            slippage: slippage_tolerance_pct as f32,
                            price_impact: price_impact_pct as f32,
                            base_amount: e.base_amount_out,
                            quote_amount: e.quote_amount_in,
                            price: price,
                            price_usd: price * quote_price_usd,
                            total: quote_normalized,
                            total_usd: quote_normalized * quote_price_usd,
                        };
                        Some(EventType::Trade(trade))}
                    else {
                        None
                    }
                }
                DecodedInstruction::Sell(args) => {
                    if tx.error.is_some() {
                        let trade_row = TradeRow {
                            signature: tx.signature.to_string(), timestamp: tx.block_time, slot: tx.slot,
                            transaction_index: tx.transaction_index,
                            instruction_index: action.instruction_index,
                            success: false, error: tx.error.clone(), priority_fee, bribe_fee: tip,
                            coin_creator_fee: 0.0, mev_protection, maker: tx.signers.get(0).cloned().unwrap_or_default(),
                            base_balance: 0.0, quote_balance: 0.0,
                            trade_type: constants::TRADE_TYPE_SELL,
                            protocol: constants::PROTOCOL_PUMPFUN_AMM, 
                            platform,
                            pool_address: find_account_pubkey_in_instruction(&self.decoder.instruction_layouts, "sell", "pool", action.instruction_accounts, &tx.account_keys).map(|p| p.to_string()).unwrap_or_default(),
                            base_address: find_account_pubkey_in_instruction(&self.decoder.instruction_layouts, "sell", "base_mint", action.instruction_accounts, &tx.account_keys).map(|p| p.to_string()).unwrap_or_default(),
                            quote_address: find_account_pubkey_in_instruction(&self.decoder.instruction_layouts, "sell", "quote_mint", action.instruction_accounts, &tx.account_keys).map(|p| p.to_string()).unwrap_or_default(),
                            slippage: 0.0, price_impact: 0.0, base_amount: 0, quote_amount: 0,
                            price: 0.0, price_usd: 0.0, total: 0.0, total_usd: 0.0,
                        };
                        Some(EventType::Trade(trade_row))
                    } else if let Some(DecodedEvent::Sell(e)) = action.event {


                        // Accounts Retrieval
                        let maker = e.user;

                        // FIX: The .to_string() at the end was redundant. unwrap_or_default() already returns a String.
                        let potential_base_address = find_account_pubkey_in_instruction(
                            &self.decoder.instruction_layouts,
                            "sell",
                            "base_mint",
                            action.instruction_accounts, 
                            &tx.account_keys,        
                        ).map(|pk| pk.to_string()).unwrap_or_default();

                        // FIX: Corrected function name from 'find_account_address_in_instruction'
                        let potential_quote_address = find_account_pubkey_in_instruction(
                            &self.decoder.instruction_layouts,
                            "sell",
                            "quote_mint",
                            action.instruction_accounts,
                            &tx.account_keys,
                        ).map(|pk| pk.to_string()).unwrap_or_default();

                        // FIX: This is the correct way to skip if either address was not found (resulting in an empty string).
                        if potential_base_address.is_empty() || potential_quote_address.is_empty() {
                            continue;
                        }

                        // Pump Amm Fix Flipped
                        let whitelist = [constants::NATIVE_MINT, constants::USDC_MINT, constants::USDT_MINT, constants::USD1_MINT];
                        let is_flipped = whitelist.contains(&potential_base_address.as_str());

                        if !whitelist.contains(&potential_quote_address.as_str()) && !is_flipped {
                            println!("[pumpAmm] Skipping sell trade with non-whitelisted pair: {}/{}", potential_base_address, potential_quote_address);
                            continue;
                        }

                        // Create the final, correct variables for base and quote based on the is_flipped flag.
                        // FIX: Removed .clone() to move ownership, which is more efficient. The types remain String.
                        let (base_address, quote_address) = if is_flipped {
                            (potential_quote_address, potential_base_address)
                        } else {
                            (potential_base_address, potential_quote_address)
                        };

                        // Use the now-correct addresses to get everything else.
                        let base_decimals = tx.token_decimals.get(&base_address).cloned().unwrap_or(0);
                        let quote_decimals = tx.token_decimals.get(&quote_address).cloned().unwrap_or(0);
                        
                        let (_, post_base) = get_asset_balances(tx, &maker, &base_address);
                        let (_, post_quote) = get_asset_balances(tx, &maker, &quote_address);

                        let base_normalized = e.base_amount_in as f64 / 10f64.powi(base_decimals as i32);
                        let quote_normalized = e.quote_amount_out as f64 / 10f64.powi(quote_decimals as i32);
                        
                        // Price =  Quote / Base
                        let price = if base_normalized > 0.0 {
                            quote_normalized / base_normalized
                        } else {
                            0.0
                        };

                        let quote_price_usd = if quote_address.as_str() == constants::NATIVE_MINT {
                            native_price_usd
                        } else if [constants::USDC_MINT, constants::USDT_MINT, constants::USD1_MINT].contains(&quote_address.as_str()) {
                            1.0
                        } else {
                            0.0
                        };
                        
                        // Slippage Calculation
                        // 1. Reconstruct PRE-TRADE reserves.
                        let actual_base_in = e.base_amount_in;
                        let actual_quote_out = e.quote_amount_out;
                        let post_trade_base_reserves = e.pool_base_token_reserves;
                        let post_trade_quote_reserves = e.pool_quote_token_reserves;
                        
                        let (pre_trade_base_reserves, pre_trade_quote_reserves) = (
                            post_trade_base_reserves.checked_sub(actual_base_in),
                            post_trade_quote_reserves.checked_add(actual_quote_out),
                        );

                        // Exit this action if the subtraction failed
                        let (pre_trade_base_reserves, pre_trade_quote_reserves) =
                            match (pre_trade_base_reserves, pre_trade_quote_reserves) {
                                (Some(b), Some(q)) => (b, q),
                                _ => {
                                    println!("[pumpAmm] WARN: Overflow detected calculating pre-trade reserves for tx {}. Skipping trade.", tx.signature);
                                    continue;
                                }
                            };
                        
                        // 2. Map fields to the generic function arguments.
                        let min_amount_out = args.min_quote_amount_out;
                        let base_amount_out_for_slippage = e.user_quote_amount_out;

                        let mut slippage_tolerance_pct = 0.0;
                        let mut price_impact_pct = 0.0;

                        if !tx.error.is_some() {
                            // We can now assign the values to the variables declared outside the if block.
                            (slippage_tolerance_pct, price_impact_pct) = calculate_amm_sell_metrics(
                                min_amount_out,
                                pre_trade_base_reserves,
                                pre_trade_quote_reserves,
                                actual_base_in,
                                base_amount_out_for_slippage,
                            );
                        } else {
                            // No need to redeclare here. Just assign.
                            slippage_tolerance_pct = 0.0;
                            price_impact_pct = 0.0;
                        }

                       
                        let coin_creator_fee =( e.coin_creator_fee as f64 / LAMPORTS_PER_SOL as f64) ;


                        let trade = TradeRow {
                            signature: tx.signature.to_string(),
                            timestamp: tx.block_time,
                            slot: tx.slot,
                            transaction_index: tx.transaction_index,
                            instruction_index: action.instruction_index,
                            success: tx.error.is_none(),
                            error: tx.error.clone(),
                            priority_fee: priority_fee,
                            bribe_fee: tip,
                            coin_creator_fee: coin_creator_fee,
                            mev_protection: mev_protection,
                            maker: maker.to_string(),
                            base_balance: post_base,
                            quote_balance: post_quote,
                            trade_type: constants::TRADE_TYPE_SELL,
                            protocol: constants::PROTOCOL_PUMPFUN_AMM,
                            platform: platform,
                            pool_address: e.pool.to_string(),
                            base_address,
                            quote_address,
                            slippage: slippage_tolerance_pct as f32,
                            price_impact: price_impact_pct as f32,
                            base_amount: e.base_amount_in,
                            quote_amount: e.quote_amount_out,
                            price: price,
                            price_usd: price * quote_price_usd,
                            total: quote_normalized,
                            total_usd: quote_normalized * quote_price_usd,
                        };
                        Some(EventType::Trade(trade))
                    } else {
                        None 
                    }
                }
                DecodedInstruction::Deposit(_) => {
                    if tx.error.is_some() {
                        let lp = LiquidityRow {
                            signature: tx.signature.to_string(), timestamp: tx.block_time, slot: tx.slot, success: false, error: tx.error.clone(),
                            priority_fee, protocol: constants::PROTOCOL_PUMPFUN_AMM, change_type: 0,
                            lp_provider: tx.signers.get(0).cloned().unwrap_or_default(),
                            pool_address: find_account_pubkey_in_instruction(&self.decoder.instruction_layouts, "deposit", "pool", action.instruction_accounts, &tx.account_keys).map(|p| p.to_string()).unwrap_or_default(),
                            base_amount: 0, quote_amount: 0,
                        };
                        Some(EventType::Liquidity(lp))
                    } else if let Some(DecodedEvent::Deposit(e)) = action.event {
                        let lp = LiquidityRow {
                     
                            signature: tx.signature.to_string(),
                            timestamp: tx.block_time,
                            slot: tx.slot,
                            success: tx.error.is_none(),
                            error: tx.error.clone(),
                            priority_fee: priority_fee,
                            protocol: constants::PROTOCOL_PUMPFUN_AMM,
                            change_type: 0,
                            lp_provider: e.user.to_string(),
                            pool_address: e.pool.to_string(),
                            base_amount: e.base_amount_in,
                            quote_amount: e.quote_amount_in,
                        };

                        Some(EventType::Liquidity(lp))
                    } else {
                        None
                    }
                }
                DecodedInstruction::Withdraw(_) => {
                    if tx.error.is_some() {
                        let liquidity_row = LiquidityRow {
                            signature: tx.signature.to_string(), timestamp: tx.block_time, slot: tx.slot, success: false, error: tx.error.clone(),
                            priority_fee, protocol: constants::PROTOCOL_PUMPFUN_AMM, change_type: 1,
                            lp_provider: tx.signers.get(0).cloned().unwrap_or_default(),
                            pool_address: find_account_pubkey_in_instruction(&self.decoder.instruction_layouts, "withdraw", "pool", action.instruction_accounts, &tx.account_keys).map(|p| p.to_string()).unwrap_or_default(),
                            base_amount: 0, quote_amount: 0,
                        };
                        Some(EventType::Liquidity(liquidity_row))
                    }else if let Some(DecodedEvent::Withdraw(e)) = action.event {
                        let liquidity_row = LiquidityRow {
                      
                            signature: tx.signature.to_string(),
                            timestamp: tx.block_time,
                            slot: tx.slot,
                            success: tx.error.is_none(),
                            error: tx.error.clone(),
                            priority_fee: priority_fee,
                            protocol: constants::PROTOCOL_PUMPFUN_AMM,
                            change_type: 1,
                            lp_provider: e.user.to_string(),
                            pool_address: e.pool.to_string(),
                            base_amount: e.base_amount_out,
                            quote_amount: e.quote_amount_out,
                        };
                        Some(EventType::Liquidity(liquidity_row))
                    } else {
                        None
                    }
                }
                DecodedInstruction::CollectCoinCreatorFee => {
                    if tx.error.is_some() {
                        let fee_row = FeeCollectionRow {
                            signature: tx.signature.to_string(), timestamp: tx.block_time, slot: tx.slot,
                            success: false, error: tx.error.clone(), priority_fee,
                            protocol: constants::PROTOCOL_PUMPFUN_AMM,
                            vault_address: find_account_pubkey_in_instruction(&self.decoder.instruction_layouts, "collect_coin_creator_fee", "coin_creator_vault_ata", action.instruction_accounts, &tx.account_keys).map(|p| p.to_string()).unwrap_or_default(),
                            recipient_address: find_account_pubkey_in_instruction(&self.decoder.instruction_layouts, "collect_coin_creator_fee", "coin_creator", action.instruction_accounts, &tx.account_keys).map(|p| p.to_string()).unwrap_or_default(),
                            token_0_mint_address: find_account_pubkey_in_instruction(&self.decoder.instruction_layouts, "collect_coin_creator_fee", "quote_mint", action.instruction_accounts, &tx.account_keys).map(|p| p.to_string()).unwrap_or_default(),
                            token_0_amount: 0.0,
                            token_1_mint_address: None,
                            token_1_amount: None,
                        };
                        Some(EventType::FeeCollection(fee_row))
                    } else if let Some(DecodedEvent::CollectCoinCreatorFee(e)) = action.event {
                        let vault_address = find_account_pubkey_in_instruction(&self.decoder.instruction_layouts, "collect_coin_creator_fee", "coin_creator_vault_ata", action.instruction_accounts, &tx.account_keys).map(|p| p.to_string()).unwrap_or_default();
                        let claimed_mint = find_account_pubkey_in_instruction(&self.decoder.instruction_layouts, "collect_coin_creator_fee", "quote_mint", action.instruction_accounts, &tx.account_keys).map(|p| p.to_string()).unwrap_or_default();
                        let quote_decimals = tx.token_decimals.get(&claimed_mint).cloned().unwrap_or(6);
                        let claimed_amount = e.coin_creator_fee as f64 /  10f64.powi(quote_decimals as i32);
                        let fee_row = FeeCollectionRow {
                            signature: tx.signature.to_string(),
                            timestamp: tx.block_time,
                            slot: tx.slot,
                            success: true,
                            error: None,
                            priority_fee,
                            protocol: constants::PROTOCOL_PUMPFUN_AMM,
                            vault_address: vault_address, 
                            recipient_address: e.coin_creator.to_string(),
                            token_0_mint_address: claimed_mint,
                            token_0_amount: claimed_amount,
                            token_1_mint_address: None,
                            token_1_amount: None,
                        };
                        Some(EventType::FeeCollection(fee_row))
                    } else {

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
