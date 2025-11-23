use anchor_lang::AnchorDeserialize;
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use borsh::BorshDeserialize;
use clickhouse::{Client, Row};
use redis::aio::MultiplexedConnection;
use serde::{Deserialize, Serialize};

use solana_sdk::native_token::LAMPORTS_PER_SOL;
use solana_sdk::{instruction::CompiledInstruction, pubkey::Pubkey};
use std::borrow::Cow;
use std::collections::HashMap;
use std::fs;
use std::str::FromStr;
use yellowstone_grpc_proto::prelude::InnerInstruction;

use super::{TransactionHandler, TransactionInfo};
use crate::database::insert_rows;
use crate::handlers::{Idl, anchor_event_discriminator, constants};
use crate::spl_system_decoder;
use crate::types::{FeeCollectionRow, MigrationRow, MintRow, TradeRow};
use crate::utils::{FormattedInstruction, get_asset_balances};

//==============================================================================
// 1. DECODING LOGIC (The original Decoder)
//==============================================================================

#[derive(Debug, BorshDeserialize, Serialize, Clone)]
pub struct BuyExactInArgs {
    pub amount_in: u64,
    pub minimum_amount_out: u64,
    pub share_fee_rate: u64,
}

#[derive(Debug, BorshDeserialize, Serialize, Clone)]
pub struct SellExactInArgs {
    pub amount_in: u64,
    pub minimum_amount_out: u64,
    pub share_fee_rate: u64,
}

#[derive(Debug, BorshDeserialize, Serialize, Clone)]
pub struct MintParams {
    pub decimals: u8,
    pub name: String,
    pub symbol: String,
    pub uri: String,
}

#[derive(Debug, BorshDeserialize, Serialize, Clone)]
pub enum CurveParams {
    Constant(ConstantCurve),
    Fixed(FixedCurve),
    Linear(LinearCurve),
}

#[derive(Debug, BorshDeserialize, Serialize, Clone)]
pub struct ConstantCurve {
    pub supply: u64,
    pub total_base_sell: u64,
    pub total_quote_fund_raising: u64,
    pub migrate_type: u8,
}

#[derive(Debug, BorshDeserialize, Serialize, Clone)]
pub struct FixedCurve {
    pub supply: u64,
    pub total_quote_fund_raising: u64,
    pub migrate_type: u8,
}

#[derive(Debug, BorshDeserialize, Serialize, Clone)]
pub struct LinearCurve {
    pub supply: u64,
    pub total_quote_fund_raising: u64,
    pub migrate_type: u8,
}

#[derive(Debug, BorshDeserialize, Serialize, Clone)]
pub struct VestingParams {
    pub total_locked_amount: u64,
    pub cliff_period: u64,
    pub unlock_period: u64,
}

#[derive(Debug, BorshDeserialize, Serialize, Clone)]
pub struct InitializeArgs {
    pub base_mint_param: MintParams,
    pub curve_param: CurveParams,
    pub vesting_param: VestingParams,
}

// --- EVENT STRUCTS (Originals, now with Serialize) ---
#[derive(Debug, BorshDeserialize, Serialize, Clone)]
pub enum TradeDirection {
    Buy,
    Sell,
}

#[derive(Debug, BorshDeserialize, Serialize, Clone)]
pub enum PoolStatus {
    Fund,
    Migrate,
    Trade,
}

#[derive(Debug, BorshDeserialize, Serialize, Clone)]
pub struct TradeEvent {
    pub pool_state: Pubkey,
    pub total_base_sell: u64,
    pub virtual_base: u64,
    pub virtual_quote: u64,
    pub real_base_before: u64,
    pub real_quote_before: u64,
    pub real_base_after: u64,
    pub real_quote_after: u64,
    pub amount_in: u64,
    pub amount_out: u64,
    pub protocol_fee: u64,
    pub platform_fee: u64,
    pub creator_fee: u64,
    pub share_fee: u64,
    pub trade_direction: TradeDirection,
    pub pool_status: PoolStatus,
    pub exact_in: bool,
}

#[derive(Debug, BorshDeserialize, Serialize, Clone)]
pub struct PoolCreateEvent {
    pub pool_state: Pubkey,
    pub creator: Pubkey,
    pub config: Pubkey,
    pub base_mint_param: MintParams,
    pub curve_param: CurveParams,
    pub vesting_param: VestingParams,
}

// Struct for holding data from a fee claim, decoded from an inner SPL Transfer instruction
#[derive(Debug, Serialize, Clone)]
pub struct PlatformFeeClaimed {
    pub amount: u64,
    pub mint: Pubkey,
    pub destination: Pubkey,
}

// --- DECODED DATA STRUCTS (NEW) ---
#[derive(Debug, Serialize, Clone)]
#[serde(tag = "type", content = "data")]
pub enum DecodedInstruction {
    BuyExactIn(BuyExactInArgs),
    SellExactIn(SellExactInArgs),
    Initialize(InitializeArgs),
    MigrateToCpSwap { new_cpswap_pool: Pubkey },
    ClaimPlatformFee,
}

#[derive(Debug, Serialize, Clone)]
#[serde(tag = "type", content = "data")]
pub enum DecodedEvent {
    Trade(TradeEvent),
    FeeClaim(PlatformFeeClaimed),
    PoolCreate(PoolCreateEvent),
}

#[derive(Debug)]
pub struct DecodedAction<'a> {
    pub instruction: DecodedInstruction,
    pub event: Option<DecodedEvent>,
    pub instruction_accounts: &'a [u8],
    pub instruction_index: u16,
}
pub struct RaydiumLaunchpadDecoder {
    instruction_discriminators: HashMap<[u8; 8], String>,
    event_discriminators: HashMap<[u8; 8], String>,
    program_id: Pubkey,
    pub instruction_layouts: HashMap<String, HashMap<String, usize>>,
}

impl RaydiumLaunchpadDecoder {
    pub fn new() -> Result<Self> {
        let idl_str = fs::read_to_string("raydium_launchpad.json")?;
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
            instruction_layouts, // Store the layouts
        })
    }

    pub fn decode_transaction<'a>(
        &self,
        formatted_instructions: &'a [FormattedInstruction<'a>],
        account_keys: &[Pubkey],
    ) -> Result<Vec<DecodedAction<'a>>> {
        let mut all_actions = Vec::new(); // CORRECTED THIS LINE

        for (i, formatted_ix) in formatted_instructions.iter().enumerate() {
            let event = self.find_event_from_logs(&formatted_ix.logs)?;

            if account_keys.get(formatted_ix.instruction.program_id_index as usize)
                == Some(&self.program_id)
            {
                if let Some(instruction) =
                    self.decode_instruction(&formatted_ix.instruction, account_keys)?
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
                    let compiled_ix = CompiledInstruction {
                        program_id_index: inner_ix.program_id_index as u8,
                        accounts: inner_ix.accounts.clone(),
                        data: inner_ix.data.clone(),
                    };
                    if let Some(instruction) =
                        self.decode_instruction(&compiled_ix, account_keys)?
                    {
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

    fn find_event_from_logs<'a>(&self, logs: &[Cow<'a, str>]) -> Result<Option<DecodedEvent>> {
        // Search in reverse to find the last, most relevant event first
        for log in logs.iter().rev() {
            if let Some(data_str) = log.strip_prefix("Program data: ") {
                if let Ok(decoded_data) = base64::decode(data_str) {
                    // Try to decode. If it's not a Raydium event, it will just continue.
                    if let Ok(Some(event)) = self.try_decode_single_event(&decoded_data) {
                        return Ok(Some(event));
                    }
                }
            }
        }
        Ok(None)
    }

    fn decode_instruction(
        &self,
        ix: &CompiledInstruction,
        account_keys: &[Pubkey],
    ) -> Result<Option<DecodedInstruction>> {
        if ix.data.len() < 8 {
            return Ok(None);
        }
        let mut disc = [0u8; 8];
        disc.copy_from_slice(&ix.data[..8]);
        let instruction_data = &ix.data[8..];

        if let Some(name) = self.instruction_discriminators.get(&disc) {
            match name.as_str() {
                "buy_exact_in" => {
                    if let Ok(args) = BuyExactInArgs::try_from_slice(instruction_data) {
                        return Ok(Some(DecodedInstruction::BuyExactIn(args)));
                    }
                }
                "sell_exact_in" => {
                    if let Ok(args) = SellExactInArgs::try_from_slice(instruction_data) {
                        return Ok(Some(DecodedInstruction::SellExactIn(args)));
                    }
                }
                "initialize" => {
                    if let Ok(args) = InitializeArgs::try_from_slice(instruction_data) {
                        return Ok(Some(DecodedInstruction::Initialize(args)));
                    }
                }
                "migrate_to_cpswap" => {
                    let pool_acc_idx = ix
                        .accounts
                        .get(5)
                        .ok_or_else(|| anyhow!("Migrate ix missing pool account index"))?;
                    let cswap_pool = account_keys
                        .get(*pool_acc_idx as usize)
                        .ok_or_else(|| anyhow!("Cpswap Pool account not found"))?;
                    return Ok(Some(DecodedInstruction::MigrateToCpSwap {
                        new_cpswap_pool: *cswap_pool,
                    }));
                }
                "claim_platform_fee" => {
                    return Ok(Some(DecodedInstruction::ClaimPlatformFee));
                }
                _ => {}
            }
        }
        Ok(None)
    }

    fn try_decode_single_event(&self, data: &[u8]) -> Result<Option<DecodedEvent>> {
        // Events in `Program data:` logs start directly with the 8-byte event discriminator.
        // There is no 8-byte instruction prefix to skip. This was the bug.
        if data.len() < 8 {
            return Ok(None);
        }
        let mut event_disc = [0u8; 8];
        event_disc.copy_from_slice(&data[..8]);
        let event_data = &data[8..];

        if let Some(name) = self.event_discriminators.get(&event_disc) {
            match name.as_str() {
                "TradeEvent" => {
                    if let Ok(event) = TradeEvent::try_from_slice(event_data) {
                        return Ok(Some(DecodedEvent::Trade(event)));
                    }
                }
                "PoolCreateEvent" => {
                    if let Ok(event) = PoolCreateEvent::try_from_slice(event_data) {
                        return Ok(Some(DecodedEvent::PoolCreate(event)));
                    }
                }
                _ => {}
            }
        }
        Ok(None)
    }
}

pub struct RaydiumLaunchpadHandler {
    decoder: RaydiumLaunchpadDecoder,
}

impl RaydiumLaunchpadHandler {
    pub fn new() -> Result<Self> {
        Ok(Self {
            decoder: RaydiumLaunchpadDecoder::new()?,
        })
    }
}

#[async_trait]
impl TransactionHandler for RaydiumLaunchpadHandler {
    fn name(&self) -> &'static str {
        "Raydium Launchpad"
    }

    fn is_of_interest(&self, tx_info: &TransactionInfo) -> bool {
        let program_id = "LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj";
        tx_info
            .logs
            .iter()
            .any(|log| log.contains(&format!("Program {} invoke", program_id)))
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

        let mut mint_rows = Vec::new();
        let mut trade_rows = Vec::new();
        let mut migration_rows = Vec::new();
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
                DecodedInstruction::Initialize(args) => {
                    let (initial_base_liquidity, initial_quote_liquidity) = match args.curve_param {
                        CurveParams::Constant(c) => (c.supply, c.total_quote_fund_raising),
                        CurveParams::Fixed(f) => (f.supply, f.total_quote_fund_raising),
                        CurveParams::Linear(l) => (l.supply, l.total_quote_fund_raising),
                    };

                    mint_rows.push(MintRow {
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
                        protocol: constants::PROTOCOL_RAYDIUM_LAUNCHPAD,
                        mint_address: get_acc_str("initialize", "base_mint"),
                        creator_address: get_acc_str("initialize", "creator"),
                        pool_address: get_acc_str("initialize", "pool_state"),
                        initial_base_liquidity,
                        initial_quote_liquidity,
                        token_name: Some(args.base_mint_param.name),
                        token_symbol: Some(args.base_mint_param.symbol),
                        token_uri: Some(args.base_mint_param.uri),
                        token_decimals: Some(args.base_mint_param.decimals),
                    });
                }
                DecodedInstruction::BuyExactIn(args) => {
                    if let Some(event) = action.event.clone().and_then(|e| match e {
                        DecodedEvent::Trade(t) => Some(t),
                        _ => None,
                    }) {
                        println!("raydium launchpad buy event decoded: {:#?}", event);
                        let base_address = get_acc_str("buy_exact_in", "base_mint");
                        let quote_address = get_acc_str("buy_exact_in", "quote_mint");
                        let maker_str = get_acc_str("buy_exact_in", "payer");
                        let maker = Pubkey::from_str(&maker_str).unwrap_or_default();
                        let (pre_base, post_base) =
                            get_asset_balances(tx_info, &maker, &base_address);
                        let (pre_quote, post_quote) =
                            get_asset_balances(tx_info, &maker, &quote_address);
                        let (base_amount, quote_amount) = (event.amount_out, event.amount_in);
                        let total = quote_amount as f64 / LAMPORTS_PER_SOL as f64;
                        let slippage = if event.amount_out > 0 {
                            (event.amount_out as f64 - args.minimum_amount_out as f64)
                                / event.amount_out as f64
                        } else {
                            0.0
                        };
                        let price = if base_amount > 0 {
                            (quote_amount as f64) / (base_amount as f64)
                        } else {
                            0.0
                        };
                        let (price_usd, total_usd) = if native_price_usd > 0.0 {
                            (price * native_price_usd, total * native_price_usd)
                        } else {
                            (0.0, 0.0)
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
                            maker: maker.to_string(),
                            pre_base_balance: pre_base,
                            post_base_balance: post_base,
                            pre_quote_balance: pre_quote,
                            post_quote_balance: post_quote,
                            trade_type: constants::TRADE_TYPE_BUY,
                            protocol: constants::PROTOCOL_RAYDIUM_LAUNCHPAD,
                            platform: 0,
                            pool_address: event.pool_state.to_string(),
                            base_address,
                            quote_address,
                            slippage: slippage as f32,
                            base_amount,
                            quote_amount,
                            price,
                            price_usd,
                            total_supply: constants::RAYDIUM_LAUNCHPAD_TOTAL_SUPPLY,
                            total,
                            total_usd,
                        });
                    }
                }
                DecodedInstruction::SellExactIn(args) => {
                    if let Some(event) = action.event.clone().and_then(|e| match e {
                        DecodedEvent::Trade(t) => Some(t),
                        _ => None,
                    }) {
                        let base_address = get_acc_str("sell_exact_in", "base_mint");
                        let quote_address = get_acc_str("sell_exact_in", "quote_mint");
                        let maker_str = get_acc_str("sell_exact_in", "payer");
                        let maker = Pubkey::from_str(&maker_str).unwrap_or_default();
                        let (pre_base, post_base) =
                            get_asset_balances(tx_info, &maker, &base_address);
                        let (pre_quote, post_quote) =
                            get_asset_balances(tx_info, &maker, &quote_address);
                        let (base_amount, quote_amount) = (event.amount_in, event.amount_out);
                        let total = quote_amount as f64 / LAMPORTS_PER_SOL as f64;
                        let slippage = if event.amount_out > 0 {
                            (event.amount_out as f64 - args.minimum_amount_out as f64)
                                / event.amount_out as f64
                        } else {
                            0.0
                        };
                        let price = if base_amount > 0 {
                            (quote_amount as f64) / (base_amount as f64)
                        } else {
                            0.0
                        };
                        let (price_usd, total_usd) = if native_price_usd > 0.0 {
                            (price * native_price_usd, total * native_price_usd)
                        } else {
                            (0.0, 0.0)
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
                            maker: maker.to_string(),
                            pre_base_balance: pre_base,
                            post_base_balance: post_base,
                            pre_quote_balance: pre_quote,
                            post_quote_balance: post_quote,
                            trade_type: constants::TRADE_TYPE_SELL,
                            protocol: constants::PROTOCOL_RAYDIUM_LAUNCHPAD,
                            platform: 0,
                            pool_address: event.pool_state.to_string(),
                            base_address,
                            quote_address,
                            slippage: slippage as f32,
                            base_amount,
                            quote_amount,
                            price,
                            price_usd,
                            total_supply: constants::RAYDIUM_LAUNCHPAD_TOTAL_SUPPLY,
                            total,
                            total_usd,
                        });
                    }
                }
                DecodedInstruction::MigrateToCpSwap { new_cpswap_pool } => {
                    migration_rows.push(MigrationRow {
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
                        protocol: constants::PROTOCOL_RAYDIUM_LAUNCHPAD,
                        mint_address: get_acc_str("migrate_to_cpswap", "base_mint"),
                        virtual_pool_address: get_acc_str("migrate_to_cpswap", "pool_state"),
                        pool_address: new_cpswap_pool.to_string(),
                        migrated_base_liquidity: None,
                        migrated_quote_liquidity: None,
                    });
                }
                DecodedInstruction::ClaimPlatformFee => {
                    if let Some(DecodedEvent::FeeClaim(e)) = action.event {
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
                            protocol: constants::PROTOCOL_RAYDIUM_LAUNCHPAD,
                            pool_address: get_acc_str("claim_platform_fee", "pool_state"),
                            recipient_address: e.destination.to_string(),
                            token_0_mint_address: e.mint.to_string(),
                            token_0_amount: e.amount,
                            token_1_mint_address: None,
                            token_1_amount: None,
                        });
                    }
                }
            }
        }

        insert_rows(db_client, "mints", mint_rows, self.name(), "mint").await?;
        insert_rows(db_client, "trades", trade_rows, self.name(), "trade").await?;
        insert_rows(
            db_client,
            "migrations",
            migration_rows,
            self.name(),
            "migration",
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
