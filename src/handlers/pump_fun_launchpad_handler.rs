use crate::handlers::pump_fun_launchpad_idl::{
    BuyArgs, CollectCreatorFeeEvent, CompletePumpAmmMigrationEvent, CreateArgs, CreateEvent,
    SellArgs, TradeEvent,
};
use crate::handlers::{Idl, anchor_event_discriminator, constants};
use crate::types::{
    EventPayload, EventType, FeeCollectionRow, FormattedInstruction, MigrationRow, MintRow,
    TradeRow, UnifiedTransaction,
};
use crate::utils::{
    deserialize_lax, find_account_pubkey_in_instruction, get_asset_balances, get_mev_protection,
    get_priority_fee, get_tip, get_trading_platform,
};
use anchor_lang::{AnchorDeserialize, AnchorSerialize};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use serde::Serialize; // <-- Add this line
use solana_sdk::native_token::LAMPORTS_PER_SOL;
use solana_sdk::pubkey::Pubkey;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fs;
use std::str::FromStr;
use yellowstone_grpc_proto::prelude::InnerInstruction;

use super::TransactionHandler;

//==============================================================================
// DECODER LOGIC
//==============================================================================

#[derive(Debug, Serialize, Clone)]
#[serde(tag = "type", content = "data")]
pub enum DecodedInstruction {
    Buy(BuyArgs),
    Sell(SellArgs),
    Create(CreateArgs),
    Migrate,
    CollectCreatorFee,
}

#[derive(Debug, Serialize, Clone)]
#[serde(tag = "type", content = "data")]
pub enum DecodedEvent {
    Create(CreateEvent),
    Trade(TradeEvent),
    Migration(CompletePumpAmmMigrationEvent),
    CreatorFee(CollectCreatorFeeEvent),
}

#[derive(Debug)]
pub struct DecodedAction<'a> {
    pub instruction: DecodedInstruction,
    pub event: Option<DecodedEvent>,
    pub instruction_accounts: &'a [u8],
    pub instruction_index: u16,
}

pub struct PumpFunLaunchpadDecoder {
    instruction_discriminators: HashMap<[u8; 8], String>,
    event_discriminators: HashMap<[u8; 8], String>,
    instruction_layouts: HashMap<String, HashMap<String, usize>>,
    program_id: Pubkey,
}

impl PumpFunLaunchpadDecoder {
    pub fn new() -> Result<Self> {
        let idl_str = fs::read_to_string("pump_fun_launchpad.json")?;
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
            instruction_layouts,
            program_id,
        })
    }

    pub fn decode_transaction<'a>(
        &self,
        formatted_instructions: &'a [FormattedInstruction<'a>],
        account_keys: &[Pubkey],
    ) -> Result<Vec<DecodedAction<'a>>> {
        let mut all_actions = Vec::new();

        for (i, formatted_ix) in formatted_instructions.iter().enumerate() {
            let top_level_program_id =
                &account_keys[formatted_ix.instruction.program_id_index as usize];

            // Case 1: Direct top-level call
            if top_level_program_id == &self.program_id {
                if let Some(instruction) =
                    self.decode_instruction(&formatted_ix.instruction.data)?
                {
                    let event =
                        self.find_first_event(&formatted_ix.inner_instructions, account_keys)?;
                    all_actions.push(DecodedAction {
                        instruction,
                        event,
                        instruction_accounts: &formatted_ix.instruction.accounts,
                        instruction_index: i as u16,
                    });
                }
            }

            // Case 2: Inner instruction (routed call)
            if !formatted_ix.inner_instructions.is_empty() {
                for (inner_idx, inner_ix_cow) in formatted_ix.inner_instructions.iter().enumerate()
                {
                    let inner_ix = inner_ix_cow.as_ref();
                    if let Some(inner_program_id) =
                        account_keys.get(inner_ix.program_id_index as usize)
                    {
                        if inner_program_id == &self.program_id {
                            if let Some(instruction) = self.decode_instruction(&inner_ix.data)? {
                                let remaining_inner_instructions =
                                    &formatted_ix.inner_instructions[inner_idx + 1..];
                                let event = self
                                    .find_first_event(remaining_inner_instructions, account_keys)?;
                                all_actions.push(DecodedAction {
                                    instruction,
                                    event,
                                    instruction_accounts: &inner_ix.accounts,
                                    instruction_index: i as u16,
                                });
                            }
                        }
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
                "buy" => {
                    if let Ok(args) = deserialize_lax::<BuyArgs>(instruction_data) {
                        return Ok(Some(DecodedInstruction::Buy(args)));
                    }
                }
                "sell" => {
                    if let Ok(args) = deserialize_lax::<SellArgs>(instruction_data) {
                        return Ok(Some(DecodedInstruction::Sell(args)));
                    }
                }
                "create" => {
                    if let Ok(args) = deserialize_lax::<CreateArgs>(instruction_data) {
                        return Ok(Some(DecodedInstruction::Create(args)));
                    }
                }
                "migrate" => return Ok(Some(DecodedInstruction::Migrate)),
                "collect_creator_fee" => return Ok(Some(DecodedInstruction::CollectCreatorFee)),
                _ => {}
            }
        }
        Ok(None)
    }

    fn find_first_event<'a>(
        &self,
        inner_instructions: &[Cow<'a, InnerInstruction>],
        account_keys: &[Pubkey],
    ) -> Result<Option<DecodedEvent>> {
        for inner_ix_cow in inner_instructions {
            let inner_ix = inner_ix_cow.as_ref();
            if let Some(inner_program_id) = account_keys.get(inner_ix.program_id_index as usize) {
                // Anchor events are CPIs to the same program.
                if inner_program_id == &self.program_id {
                    if let Some(event) = self.decode_single_event(&inner_ix.data)? {
                        return Ok(Some(event));
                    }
                }
            }
        }
        Ok(None)
    }

    fn decode_single_event(&self, data: &[u8]) -> Result<Option<DecodedEvent>> {
        if data.len() < 16 {
            return Ok(None);
        }
        let mut event_disc = [0u8; 8];
        event_disc.copy_from_slice(&data[8..16]);
        let event_data = &data[16..];

        if let Some(name) = self.event_discriminators.get(&event_disc) {
            match name.as_str() {
                "CreateEvent" => {
                    if let Ok(event) = CreateEvent::try_from_slice(event_data) {
                        return Ok(Some(DecodedEvent::Create(event)));
                    }
                }
                "TradeEvent" => {
                    if let Ok(event) = TradeEvent::try_from_slice(event_data) {
                        return Ok(Some(DecodedEvent::Trade(event)));
                    }
                }
                "CompletePumpAmmMigrationEvent" => {
                    if let Ok(event) = CompletePumpAmmMigrationEvent::try_from_slice(event_data) {
                        return Ok(Some(DecodedEvent::Migration(event)));
                    }
                }
                "CollectCreatorFeeEvent" => {
                    if let Ok(event) = CollectCreatorFeeEvent::try_from_slice(event_data) {
                        return Ok(Some(DecodedEvent::CreatorFee(event)));
                    }
                }
                _ => {}
            }
        }
        Ok(None)
    }
}

//==============================================================================
// PROCESSING LOGIC (The Handler)
//==============================================================================

pub struct PumpFunLaunchpadHandler {
    decoder: PumpFunLaunchpadDecoder,
}

impl PumpFunLaunchpadHandler {
    pub fn new() -> Result<Self> {
        Ok(Self {
            decoder: PumpFunLaunchpadDecoder::new()?,
        })
    }
}

#[async_trait]
impl TransactionHandler for PumpFunLaunchpadHandler {
    fn name(&self) -> &'static str {
        "Pump.fun Launchpad"
    }

    fn is_of_interest(&self, tx: &UnifiedTransaction) -> bool {
        let mut is_invoke = false;
        let mut is_interesting = false;

        for log in &tx.logs {
            if log.contains("Bonding curve already migrated") {
                return false;
            }

            if !is_invoke
                && log.contains("Program 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P invoke [")
            {
                is_invoke = true;
            }

            if is_invoke {
                if let Some(name) = log.strip_prefix("Program log: Instruction: ") {
                    if matches!(
                        name,
                        "Create" | "Buy" | "Sell" | "Migrate" | "CollectCreatorFee"
                    ) {
                        is_interesting = true;
                    }
                }
            }
        }

        is_interesting
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

        // Transaction Extras
        let priority_fee: f64 = get_priority_fee(tx);
        let tip = get_tip(tx);
        let mev_protection = get_mev_protection(tx, tip);
        let platform = get_trading_platform(tx);

        let mut events: Vec<EventPayload> = Vec::new();

        for action in decoded_data {
            let event_type = match action.instruction {
                DecodedInstruction::Buy(mut args) => {
                    if tx.error.is_some() {
                        // Transaction FAILED. Create a row with the available data.
                        let maker = tx.signers.get(0).cloned().unwrap_or_default();
                        let pool_address = find_account_pubkey_in_instruction(
                            &self.decoder.instruction_layouts,
                            "buy",
                            "bonding_curve",
                            action.instruction_accounts,
                            &tx.account_keys,
                        )
                        .map(|p| p.to_string())
                        .unwrap_or_default();
                        let base_address = find_account_pubkey_in_instruction(
                            &self.decoder.instruction_layouts,
                            "buy",
                            "mint",
                            action.instruction_accounts,
                            &tx.account_keys,
                        )
                        .map(|p| p.to_string())
                        .unwrap_or_default();

                        let trade_row = TradeRow {
                            signature: tx.signature.to_string(),
                            timestamp: tx.block_time,
                            slot: tx.slot,
                            transaction_index: tx.transaction_index,
                            instruction_index: action.instruction_index,
                            success: false, // Explicitly set to false
                            error: tx.error.clone(),
                            priority_fee,
                            bribe_fee: tip,
                            coin_creator_fee: 0.0, // Cannot be known on failure
                            mev_protection,
                            maker,
                            base_balance: 0.0,  // Balances did not change
                            quote_balance: 0.0, // Balances did not change
                            trade_type: constants::TRADE_TYPE_BUY,
                            protocol: constants::PROTOCOL_PUMPFUN_LAUNCHPAD,
                            platform,
                            pool_address,
                            base_address,
                            quote_address: constants::NATIVE_MINT.to_string(),
                            slippage: 0.0,     // Cannot be calculated on failure
                            price_impact: 0.0, // Cannot be calculated on failure
                            base_amount: 0,    // No tokens were transferred
                            quote_amount: 0,   // No SOL was transferred
                            price: 0.0,
                            price_usd: 0.0,
                            total: 0.0,
                            total_usd: 0.0,
                        };
                        Some(EventType::Trade(trade_row))
                    } else if let Some(DecodedEvent::Trade(e)) = action.event {
                        let maker_pk = e.user;
                        let base_address = e.mint.to_string();
                        let quote_address = constants::NATIVE_MINT.to_string();

                        let (_, post_base) = get_asset_balances(tx, &maker_pk, &base_address);
                        let (_, post_quote) = get_asset_balances(tx, &maker_pk, &quote_address);

                        let pool_address = find_account_pubkey_in_instruction(
                            &self.decoder.instruction_layouts,
                            "buy",
                            "bonding_curve",
                            action.instruction_accounts,
                            &tx.account_keys,
                        )
                        .map(|pk| pk.to_string())
                        .unwrap_or_default();

                        // --- START: DEFINITIVE SLIPPAGE & PRICE IMPACT FIX (BUY) ---
                        let pre_trade_virtual_sol =
                            e.virtual_sol_reserves.saturating_sub(e.sol_amount);
                        let pre_trade_virtual_token =
                            e.virtual_token_reserves.saturating_add(e.token_amount);

                        // 2. Calculate the "ideal cost" using the correct AMM formula and robust u128 math.
                        // This is what you would have paid for an infinitesimally small trade.
                        let ideal_cost_in_lamports = if pre_trade_virtual_token > e.token_amount {
                            let sol_reserves = pre_trade_virtual_sol as u128;
                            let token_reserves = pre_trade_virtual_token as u128;
                            let tokens_to_buy = e.token_amount as u128;

                            // Standard AMM formula: (reserve_in * amount_out) / (reserve_out - amount_out)
                            (sol_reserves * tokens_to_buy) / (token_reserves - tokens_to_buy)
                        } else {
                            0
                        };

                        let slippage_tolerance = if ideal_cost_in_lamports > 0 {
                            (args.max_sol_cost as f64 - ideal_cost_in_lamports as f64)
                                / ideal_cost_in_lamports as f64
                        } else {
                            0.0
                        };

                        // 3. Calculate Price Impact.
                        // Compares the actual cost paid (from the event) to the ideal cost.
                        let price_impact = if ideal_cost_in_lamports > 0 {
                            // Formula: (Actual Cost - Ideal Cost) / Ideal Cost
                            (e.sol_amount as f64 - ideal_cost_in_lamports as f64)
                                / ideal_cost_in_lamports as f64
                        } else {
                            0.0
                        };

                        let base_decimals =
                            tx.token_decimals.get(&base_address).cloned().unwrap_or(6);

                        let total = e.sol_amount as f64 / LAMPORTS_PER_SOL as f64;
                        let price = if e.token_amount > 0 {
                            total / (e.token_amount as f64 / 10f64.powi(base_decimals as i32))
                        } else {
                            0.0
                        };

                        let trade_row = TradeRow {
                            signature: tx.signature.to_string(),
                            timestamp: tx.block_time,
                            slot: tx.slot,
                            transaction_index: tx.transaction_index,
                            instruction_index: action.instruction_index,
                            success: tx.error.is_none(),
                            error: tx.error.clone(),
                            priority_fee,
                            bribe_fee: tip,
                            coin_creator_fee: (e.creator_fee as f64 / LAMPORTS_PER_SOL as f64),
                            mev_protection,
                            maker: maker_pk.to_string(),
                            base_balance: post_base,
                            quote_balance: post_quote,
                            trade_type: constants::TRADE_TYPE_BUY,
                            protocol: constants::PROTOCOL_PUMPFUN_LAUNCHPAD,
                            platform,
                            pool_address,
                            base_address,
                            quote_address,
                            slippage: slippage_tolerance as f32,
                            price_impact: 0.0 as f32,
                            base_amount: e.token_amount,
                            quote_amount: e.sol_amount,
                            price,
                            price_usd: price * native_price_usd,
                            total,
                            total_usd: total * native_price_usd,
                        };
                        Some(EventType::Trade(trade_row))
                    } else {
                        None
                    }
                }
                DecodedInstruction::Sell(args) => {
                    if tx.error.is_some() {
                        // Transaction FAILED. Create a row with the available data.
                        let maker = tx.signers.get(0).cloned().unwrap_or_default();
                        let pool_address = find_account_pubkey_in_instruction(
                            &self.decoder.instruction_layouts,
                            "buy",
                            "bonding_curve",
                            action.instruction_accounts,
                            &tx.account_keys,
                        )
                        .map(|p| p.to_string())
                        .unwrap_or_default();
                        let base_address = find_account_pubkey_in_instruction(
                            &self.decoder.instruction_layouts,
                            "buy",
                            "mint",
                            action.instruction_accounts,
                            &tx.account_keys,
                        )
                        .map(|p| p.to_string())
                        .unwrap_or_default();

                        let trade_row = TradeRow {
                            signature: tx.signature.to_string(),
                            timestamp: tx.block_time,
                            slot: tx.slot,
                            transaction_index: tx.transaction_index,
                            instruction_index: action.instruction_index,
                            success: false, // Explicitly set to false
                            error: tx.error.clone(),
                            priority_fee,
                            bribe_fee: tip,
                            coin_creator_fee: 0.0, // Cannot be known on failure
                            mev_protection,
                            maker,
                            base_balance: 0.0,  // Balances did not change
                            quote_balance: 0.0, // Balances did not change
                            trade_type: constants::TRADE_TYPE_BUY,
                            protocol: constants::PROTOCOL_PUMPFUN_LAUNCHPAD,
                            platform,
                            pool_address,
                            base_address,
                            quote_address: constants::NATIVE_MINT.to_string(),
                            slippage: 0.0,     // Cannot be calculated on failure
                            price_impact: 0.0, // Cannot be calculated on failure
                            base_amount: 0,    // No tokens were transferred
                            quote_amount: 0,   // No SOL was transferred
                            price: 0.0,
                            price_usd: 0.0,
                            total: 0.0,
                            total_usd: 0.0,
                        };
                        Some(EventType::Trade(trade_row))
                    } else if let Some(DecodedEvent::Trade(e)) = action.event {
                        let maker_pk = e.user;
                        let base_address = e.mint.to_string();
                        let quote_address = constants::NATIVE_MINT.to_string();

                        let (_, post_base) = get_asset_balances(tx, &maker_pk, &base_address);
                        let (_, post_quote) = get_asset_balances(tx, &maker_pk, &quote_address);

                        let pool_address = find_account_pubkey_in_instruction(
                            &self.decoder.instruction_layouts,
                            "sell",
                            "bonding_curve",
                            action.instruction_accounts,
                            &tx.account_keys,
                        )
                        .map(|pk| pk.to_string())
                        .unwrap_or_default();

                        let pre_trade_virtual_sol =
                            e.virtual_sol_reserves.saturating_add(e.sol_amount);
                        let pre_trade_virtual_token =
                            e.virtual_token_reserves.saturating_sub(e.token_amount);

                        // 2. Calculate the TRUE ideal SOL output using the standard AMM formula.
                        let ideal_sol_output = {
                            let sol_reserves = pre_trade_virtual_sol as u128;
                            let token_reserves = pre_trade_virtual_token as u128;
                            let tokens_to_sell = e.token_amount as u128;

                            if token_reserves > 0 {
                                // Formula: (reserve_out * amount_in) / (reserve_in + amount_in)
                                (sol_reserves * tokens_to_sell) / (token_reserves + tokens_to_sell)
                            } else {
                                0
                            }
                        };

                        // 3. Calculate Slippage Tolerance: Compares ideal output to the user's minimum.
                        let slippage_tolerance = if ideal_sol_output > 0 {
                            // Formula: (Ideal Output - Minimum Allowed) / Ideal Output
                            (ideal_sol_output as f64 - args.min_sol_output as f64)
                                / ideal_sol_output as f64
                        } else {
                            0.0
                        };

                        // 4. Calculate Price Impact: Compares ideal output to the actual amount received.
                        let price_impact = if ideal_sol_output > 0 {
                            // Formula: (Ideal Output - Actual Output) / Ideal Output
                            (ideal_sol_output as f64 - e.sol_amount as f64)
                                / ideal_sol_output as f64
                        } else {
                            0.0
                        };

                        let base_decimals =
                            tx.token_decimals.get(&base_address).cloned().unwrap_or(6);

                        let total = e.sol_amount as f64 / LAMPORTS_PER_SOL as f64;
                        let price = if e.token_amount > 0 {
                            total / (e.token_amount as f64 / 10f64.powi(base_decimals as i32))
                        } else {
                            0.0
                        };

                        let trade_row = TradeRow {
                            signature: tx.signature.to_string(),
                            timestamp: tx.block_time,
                            slot: tx.slot,
                            transaction_index: tx.transaction_index,
                            instruction_index: action.instruction_index,
                            success: tx.error.is_none(),
                            error: tx.error.clone(),
                            priority_fee,
                            bribe_fee: tip,
                            coin_creator_fee: (e.creator_fee as f64 / LAMPORTS_PER_SOL as f64),
                            mev_protection,
                            maker: maker_pk.to_string(),
                            base_balance: post_base,
                            quote_balance: post_quote,
                            trade_type: constants::TRADE_TYPE_SELL,
                            protocol: constants::PROTOCOL_PUMPFUN_LAUNCHPAD,
                            platform,
                            pool_address,
                            base_address,
                            quote_address,
                            slippage: slippage_tolerance as f32,
                            price_impact: price_impact as f32,
                            base_amount: e.token_amount,
                            quote_amount: e.sol_amount,
                            price,
                            price_usd: price * native_price_usd,
                            total,
                            total_usd: total * native_price_usd,
                        };
                        Some(EventType::Trade(trade_row))
                    } else {
                        None
                    }
                }
                DecodedInstruction::Create(args) => {
                    if tx.error.is_some() {
                        let mint_row = MintRow {
                            signature: tx.signature.to_string(),
                            timestamp: tx.block_time,
                            slot: tx.slot,
                            success: false,
                            error: tx.error.clone(),
                            priority_fee,
                            protocol: constants::PROTOCOL_PUMPFUN_LAUNCHPAD,
                            mint_address: find_account_pubkey_in_instruction(
                                &self.decoder.instruction_layouts,
                                "create",
                                "mint",
                                action.instruction_accounts,
                                &tx.account_keys,
                            )
                            .map(|p| p.to_string())
                            .unwrap_or_default(),
                            creator_address: tx.signers.get(0).cloned().unwrap_or_default(),
                            pool_address: "".to_string(),
                            initial_base_liquidity: 0,
                            initial_quote_liquidity: 0,
                            token_name: Some(args.name),
                            token_symbol: Some(args.symbol),
                            token_uri: Some(args.uri),
                            token_decimals: 6,
                            total_supply: 0,
                            is_mutable: false,
                            update_authority: None,
                            mint_authority: None,
                            freeze_authority: None,
                        };
                        Some(EventType::Mint(mint_row))
                    } else if let Some(DecodedEvent::Create(e)) = action.event {
                        let mint_authority = find_account_pubkey_in_instruction(
                            &self.decoder.instruction_layouts,
                            "create",
                            "mint_authority",
                            action.instruction_accounts,
                            &tx.account_keys,
                        )
                        .map(|pk| pk.to_string());

                        let mint_pubkey_str = e.mint.to_string();

                        let token_decimals = tx
                            .token_decimals
                            .get(&mint_pubkey_str)
                            .cloned()
                            .unwrap_or(6);

                        let mint_row = MintRow {
                            signature: tx.signature.to_string(),
                            timestamp: tx.block_time,
                            slot: tx.slot,
                            success: tx.error.is_none(),
                            error: tx.error.clone(),
                            priority_fee: priority_fee,
                            protocol: constants::PROTOCOL_PUMPFUN_LAUNCHPAD,
                            mint_address: mint_pubkey_str,
                            creator_address: e.creator.to_string(),
                            pool_address: e.bonding_curve.to_string(),
                            initial_base_liquidity: e.virtual_token_reserves,
                            initial_quote_liquidity: e.virtual_sol_reserves,
                            token_name: Some(e.name),
                            token_symbol: Some(e.symbol),
                            token_uri: Some(e.uri),
                            token_decimals: token_decimals,
                            total_supply: e.token_total_supply,
                            is_mutable: false,
                            update_authority: mint_authority,
                            mint_authority: None,
                            freeze_authority: None,
                        };
                        Some(EventType::Mint(mint_row))
                    } else {
                        None
                    }
                }
                DecodedInstruction::Migrate => {
                    if let Some(DecodedEvent::Migration(e)) = action.event {
                        let migration_row = MigrationRow {
                            signature: tx.signature.to_string(),
                            timestamp: tx.block_time,
                            slot: tx.slot,
                            success: tx.error.is_none(),
                            error: tx.error.clone(),
                            priority_fee: priority_fee,
                            protocol: constants::PROTOCOL_PUMPFUN_LAUNCHPAD, // Source protocol
                            mint_address: e.mint.to_string(),
                            virtual_pool_address: e.bonding_curve.to_string(),
                            pool_address: e.pool.to_string(),
                            migrated_base_liquidity: Some(e.mint_amount),
                            migrated_quote_liquidity: Some(e.sol_amount),
                        };
                        Some(EventType::Migration(migration_row))
                    } else {
                        None
                    }
                }
                DecodedInstruction::CollectCreatorFee => {
                    if tx.error.is_some() {
                        let fee_row = FeeCollectionRow {
                            signature: tx.signature.to_string(),
                            timestamp: tx.block_time,
                            slot: tx.slot,
                            success: false,
                            error: tx.error.clone(),
                            priority_fee,
                            protocol: constants::PROTOCOL_PUMPFUN_LAUNCHPAD,
                            vault_address: find_account_pubkey_in_instruction(
                                &self.decoder.instruction_layouts,
                                "collect_creator_fee",
                                "creator_vault",
                                action.instruction_accounts,
                                &tx.account_keys,
                            )
                            .map(|p| p.to_string())
                            .unwrap_or_default(),
                            recipient_address: find_account_pubkey_in_instruction(
                                &self.decoder.instruction_layouts,
                                "collect_creator_fee",
                                "creator",
                                action.instruction_accounts,
                                &tx.account_keys,
                            )
                            .map(|p| p.to_string())
                            .unwrap_or_default(),
                            token_0_mint_address: constants::NATIVE_MINT.to_string(),
                            token_0_amount: 0.0,
                            token_1_mint_address: None,
                            token_1_amount: None,
                        };
                        Some(EventType::FeeCollection(fee_row))
                    } else if let Some(DecodedEvent::CreatorFee(e)) = action.event {
                        let pool_address = find_account_pubkey_in_instruction(
                            &self.decoder.instruction_layouts,
                            "collect_creator_fee",
                            "creator_vault",
                            action.instruction_accounts,
                            &tx.account_keys,
                        )
                        .map(|pk| pk.to_string())
                        .unwrap_or_default();

                        let claimed_amount = e.creator_fee as f64 / LAMPORTS_PER_SOL as f64;

                        let fee_row = FeeCollectionRow {
                            signature: tx.signature.to_string(),
                            timestamp: tx.block_time,
                            slot: tx.slot,
                            success: tx.error.is_none(),
                            error: tx.error.clone(),
                            priority_fee: priority_fee,
                            protocol: constants::PROTOCOL_PUMPFUN_LAUNCHPAD,
                            vault_address: pool_address,
                            recipient_address: e.creator.to_string(),
                            token_0_mint_address: constants::NATIVE_MINT.to_string(),
                            token_0_amount: claimed_amount,
                            token_1_mint_address: None,
                            token_1_amount: None,
                        };
                        Some(EventType::FeeCollection(fee_row))
                    } else {
                        None
                    }
                }
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
