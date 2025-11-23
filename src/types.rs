use std::{borrow::Cow, collections::HashMap, time::{SystemTime, UNIX_EPOCH}};

use clickhouse::Row;
use serde::{Deserialize, Serialize};
use solana_sdk::{ pubkey::Pubkey};
use yellowstone_grpc_proto::prelude::InnerInstruction;



pub type BalanceMap = HashMap<String, u64>; // Wallet Address -> Lamport Balance
pub type TokenBalanceMap = HashMap<String, BalanceMap>; // Mint Address -> BalanceMap

#[derive(Debug, Clone)]
pub struct FormattedInstruction<'a> {
    pub instruction: Cow<'a, solana_sdk::instruction::CompiledInstruction>,
    pub inner_instructions: Vec<Cow<'a, InnerInstruction>>, 
    pub logs: Vec<Cow<'a, str>>,
}


/// The simple, universal transaction structure based on your example.
#[derive(Debug, Clone)]
pub struct UnifiedTransaction<'a> {
    pub signature: String,
    pub slot: u64,
    pub transaction_index: u32,
    pub block_time: u32,
    pub signers: Vec<String>,
    pub error: Option<String>,
    
    pub account_keys: Vec<Pubkey>,
    pub formatted_instructions: Vec<FormattedInstruction<'a>>,
    pub logs: Vec<String>,

    pub pre_balances: TokenBalanceMap,
    pub post_balances: TokenBalanceMap,
    pub token_decimals: HashMap<String, u8>, 
}



#[derive(Row, Serialize, Deserialize, Debug, Clone)]
pub struct TradeRow {
    pub timestamp: u32,
    pub signature: String,

    pub slot: u64,
    pub transaction_index: u32,
    pub instruction_index: u16,
    pub success: bool,
    pub error: Option<String>,

    pub priority_fee: f64,
    pub bribe_fee: f64,
    pub coin_creator_fee: f64,
    pub mev_protection: u8,

    pub maker: String,

    pub base_balance: f64,
    pub quote_balance: f64,

    pub trade_type: u8,

    pub protocol: u8,
    pub platform: u8,

    pub pool_address: String,
    pub base_address: String,
    pub quote_address: String,

    pub slippage: f32,
    
    pub price_impact: f32,

    pub base_amount: u64,
    pub quote_amount: u64,

    pub price: f64,
    pub price_usd: f64,

    pub total: f64,
    pub total_usd: f64,
}

#[derive(Row, Serialize, Deserialize, Debug, Clone)]
pub struct MintRow {
    // === Transaction Details ===

    pub signature: String,
    pub timestamp: u32,
    pub slot: u64,
    pub success: bool,
    pub error: Option<String>,
    pub priority_fee: f64,

    // === Protocol & Platform ===
    /// Protocol codes: 0=Unknown, 1=PumpFunLaunchpad, 2=RaydiumLaunchpad,
    /// 3=PumpFunAMM, 4=RaydiumCPMM, 5=MeteoraBonding
    pub protocol: u8,

    // === Mint & Pool Details ===
    pub mint_address: String,
    pub creator_address: String,
    pub pool_address: String,

    // === Liquidity Details ===
    pub initial_base_liquidity: u64,
    pub initial_quote_liquidity: u64,

    // === Token Metadata ===
    pub token_name: Option<String>,
    pub token_symbol: Option<String>,
    pub token_uri: Option<String>,
    pub token_decimals: u8,
    pub total_supply: u64,

    pub is_mutable: bool,
    pub update_authority: Option<String>,
    pub mint_authority: Option<String>,
    pub freeze_authority: Option<String>,
}

#[derive(Row, Serialize, Deserialize, Debug, Clone)]
pub struct MigrationRow {
    // === Transaction Details ===

    pub timestamp: u32,
    pub signature: String,
    pub slot: u64,
    pub success: bool,
    pub error: Option<String>,
    pub priority_fee: f64,

    // === Protocol & Platform ===
    /// Protocol codes: 0=Unknown, 1=PumpFunLaunchpad, 2=RaydiumLaunchpad,
    /// 3=PumpFunAMM, 4=RaydiumCPMM, 5=MeteoraBonding
    pub protocol: u8,

    // === Migration Details ===
    /// The address of the token mint being migrated.
    pub mint_address: String,

    pub virtual_pool_address: String,
    pub pool_address: String,

    // === Liquidity & Fee Details ===
    pub migrated_base_liquidity: Option<u64>,
    pub migrated_quote_liquidity: Option<u64>,
}

#[derive(Row, Serialize, Deserialize, Debug, Clone)]
pub struct FeeCollectionRow {
    // === Transaction Details ===

    pub timestamp: u32,
    pub signature: String,
    pub slot: u64,
    pub success: bool,
    pub error: Option<String>,
    pub priority_fee: f64,

    // === Protocol & Platform ===
    /// Protocol codes: 0=Unknown, 1=PumpFunLaunchpad, 2=RaydiumLaunchpad,
    /// 3=PumpFunAMM, 4=RaydiumCPMM, 5=MeteoraBonding
    pub protocol: u8,

    // === Fee Details ===
    /// The address of the pool or bonding curve from which fees are collected.
    pub vault_address: String,
    /// The address of the wallet receiving the fees.
    pub recipient_address: String,

    // === Collected Amounts ===
    /// The mint address of the first token collected as a fee.
    pub token_0_mint_address: String,
    /// The amount of the first token collected.
    pub token_0_amount: f64,

    /// The mint address of the second token, if applicable (e.g., for Raydium CPMM).
    pub token_1_mint_address: Option<String>,
    /// The amount of the second token collected, if applicable.
    pub token_1_amount: Option<f64>,
}

#[derive(Row, Serialize, Deserialize, Debug, Clone)]
pub struct LiquidityRow {
    // === Transaction Details ===

    pub signature: String,
    pub timestamp: u32,
    pub slot: u64,
    pub success: bool,
    pub error: Option<String>,
    pub priority_fee: f64,

    // === Protocol Info ===
    /// Protocol codes: 0=Unknown, 1=PumpFunLaunchpad, 2=RaydiumLaunchpad,
    /// 3=PumpFunAMM, 4=RaydiumCPMM, 5=MeteoraBonding
    pub protocol: u8,

    // === LP Action Details ===
    /// 0: Add Liquidity, 1: Remove Liquidity
    pub change_type: u8,
    pub lp_provider: String,
    pub pool_address: String,

    // === Token Amounts ===
    pub base_amount: u64,  // Corrected
    pub quote_amount: u64, // Corrected
}

#[derive(Row, Serialize, Deserialize, Debug, Clone)]
pub struct PoolCreationRow {
    // === Transaction Details ===

    pub signature: String,
    pub timestamp: u32,
    pub slot: u64,
    pub success: bool,
    pub error: Option<String>,
    pub priority_fee: f64,

    // === Protocol Info ===
    pub protocol: u8,

    // === Pool & Token Details ===
    pub creator_address: String,
    pub pool_address: String,
    pub base_address: String,  // Corrected
    pub quote_address: String, // Corrected
    pub lp_token_address: String,

    // === Optional Initial State ===
    pub initial_base_liquidity: Option<u64>,  // Corrected
    pub initial_quote_liquidity: Option<u64>, // Corrected
    pub base_decimals: Option<u8>,            // Corrected
    pub quote_decimals: Option<u8>,           // Corrected
}

#[derive(Row, Serialize, Deserialize, Debug, Clone)]
pub struct TransferRow {
    // === Transaction Details ===

    pub timestamp: u32,
    pub signature: String,
    pub slot: u64,
    pub success: bool,
    pub error: Option<String>,
    pub priority_fee: f64,

    // === Transfer Details ===
    /// The wallet or token account address the transfer is from.
    pub source: String,
    /// The wallet or token account address the transfer is to.
    pub destination: String,

    // === Amount & Mint Details ===
    /// The mint address of the token being transferred.
    pub mint_address: String,
    /// The raw token amount (lamports for SOL).
    pub amount: u64,
    /// The human-readable amount, adjusted for decimals.
    pub amount_decimal: f64,

    // Balance Context ===
    pub source_balance: f64,
    
    pub destination_balance: f64
}

#[derive(Row, Serialize, Deserialize, Debug, Clone)]
pub struct SupplyLockRow {
    // === Transaction Details ===

    pub timestamp: u32,
    pub signature: String,
    pub slot: u64,
    pub success: bool,
    pub error: Option<String>,
    pub priority_fee: f64,

    // === Protocol Info ===
    pub protocol: u8,

    // === Vesting Details ===
    pub contract_address: String,
    pub sender: String,
    pub recipient: String,
    pub mint_address: String,
    pub total_locked_amount: f64,
    pub final_unlock_timestamp: u64,
}

#[derive(Row, Serialize, Deserialize, Debug, Clone)]
pub struct SupplyLockActionRow {
    // === Transaction Details ===

    pub signature: String,
    pub timestamp: u32,
    pub slot: u64,
    pub success: bool,
    pub error: Option<String>,
    pub priority_fee: f64,

    // === Protocol Info ===
    pub protocol: u8,

    // === Action Details ===
    /// 0: Withdraw, 1: Topup
    pub action_type: u8,
    /// The address of the vesting contract being acted upon.
    pub contract_address: String,
    /// The user performing the action.
    pub user: String,
    /// The mint address of the token being withdrawn or topped up.
    pub mint_address: String,
    /// The amount of tokens involved in the action. Decimals
    pub amount: f64,
}

#[derive(Row, Serialize, Deserialize, Debug, Clone)]
pub struct BurnRow {
    // === Transaction Details ===

    pub timestamp: u32,
    pub signature: String,
    pub slot: u64,
    pub success: bool,
    pub error: Option<String>,
    pub priority_fee: f64,

    // === Burn Details ===
    /// The mint address of the token being burned.
    pub mint_address: String,
    /// The token account address the burn is from.
    pub source: String,
    /// The raw token amount burned.
    pub amount: u64,
    /// The human-readable amount, adjusted for decimals.
    pub amount_decimal: f64,

    pub source_balance: f64,
}

#[derive(Row, Serialize, Deserialize, Debug, Clone)]
pub struct WalletProfileRow {
    pub updated_at: u32,
    pub first_seen_ts: u32,      
    pub last_seen_ts: u32,   
    
    pub wallet_address: String,  
    pub tags: Vec<String>,
    pub deployed_tokens: Vec<String>,
    pub funded_from: String,
    pub funded_timestamp: u32,
    pub funded_signature: String,
    pub funded_amount: f64,
}

#[derive(Row, Serialize, Deserialize, Debug, Clone)]
pub struct WalletProfileMetricsRow {
    pub updated_at: u32,
    pub wallet_address: String,
    pub balance: f64,

    pub transfers_in_count: u32,
    pub transfers_out_count: u32,
    pub spl_transfers_in_count: u32,
    pub spl_transfers_out_count: u32,

    pub total_buys_count: u32,
    pub total_sells_count: u32,
    pub total_winrate: f32,

    pub stats_1d_realized_profit_sol: f64,
    pub stats_1d_realized_profit_usd: f64, 
    pub stats_1d_realized_profit_pnl: f32,
    pub stats_1d_buy_count: u32,
    pub stats_1d_sell_count: u32,
    pub stats_1d_transfer_in_count: u32,
    pub stats_1d_transfer_out_count: u32,
    pub stats_1d_avg_holding_period: f32,
    pub stats_1d_total_bought_cost_sol: f64,
    pub stats_1d_total_bought_cost_usd: f64,
    pub stats_1d_total_sold_income_sol: f64,
    pub stats_1d_total_sold_income_usd: f64,
    pub stats_1d_total_fee: f64,
    pub stats_1d_winrate: f32,
    pub stats_1d_tokens_traded: u32,

    pub stats_7d_realized_profit_sol: f64,
    pub stats_7d_realized_profit_usd: f64, 
    pub stats_7d_realized_profit_pnl: f32,
    pub stats_7d_buy_count: u32,
    pub stats_7d_sell_count: u32,
    pub stats_7d_transfer_in_count: u32,
    pub stats_7d_transfer_out_count: u32,
    pub stats_7d_avg_holding_period: f32,
    pub stats_7d_total_bought_cost_sol: f64,
    pub stats_7d_total_bought_cost_usd: f64,
    pub stats_7d_total_sold_income_sol: f64,
    pub stats_7d_total_sold_income_usd: f64, 
    pub stats_7d_total_fee: f64,
    pub stats_7d_winrate: f32,
    pub stats_7d_tokens_traded: u32,

    pub stats_30d_realized_profit_sol: f64,
    pub stats_30d_realized_profit_usd: f64, 
    pub stats_30d_realized_profit_pnl: f32,
    pub stats_30d_buy_count: u32,
    pub stats_30d_sell_count: u32,
    pub stats_30d_transfer_in_count: u32,
    pub stats_30d_transfer_out_count: u32,
    pub stats_30d_avg_holding_period: f32,
    pub stats_30d_total_bought_cost_sol: f64,
    pub stats_30d_total_bought_cost_usd: f64, 
    pub stats_30d_total_sold_income_sol: f64,
    pub stats_30d_total_sold_income_usd: f64, 
    pub stats_30d_total_fee: f64,
    pub stats_30d_winrate: f32,
    pub stats_30d_tokens_traded: u32,
}

#[derive(Row, Serialize, Deserialize, Debug, Clone)]
pub struct WalletHoldingRow {
    pub updated_at: u32,
    pub start_holding_at: u32,
    pub wallet_address: String,
    pub mint_address: String,
    pub current_balance: f64,

    pub realized_profit_pnl: f32,
    pub realized_profit_sol: f64,
    pub realized_profit_usd: f64,

    pub history_transfer_in: u32,
    pub history_transfer_out: u32,

    pub history_bought_amount: f64,
    pub history_bought_cost_sol: f64,
    pub history_sold_amount: f64,
    pub history_sold_income_sol: f64,
}


#[derive(Row, Serialize, Deserialize, Debug, Clone)]
pub struct TokenStaticRow {
    pub updated_at: u32,
    pub created_at: u32,
    
    pub token_address: String,

    pub name: String,
    pub symbol: String,
    pub token_uri: String,

    pub decimals: u8,
    pub creator_address: String,
    pub pool_addresses: Vec<String>,
        
    pub launchpad: u8,
    pub protocol: u8,
    pub total_supply: u64,

    pub is_mutable: bool,
    pub update_authority: Option<String>,
    pub mint_authority: Option<String>,
    pub freeze_authority: Option<String>,
}

#[derive(Row, Serialize, Deserialize, Debug, Clone)]
pub struct TokenMetricsRow {
    pub updated_at: u32,
    pub token_address: String,
    pub total_volume_usd: f64,
    pub total_buys: u32,
    pub total_sells: u32,
    pub unique_holders: u32,
    pub ath_price_usd: f64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum EventType {
    Trade(TradeRow),
    Transfer(TransferRow),
    Mint(MintRow),
    SupplyLock(SupplyLockRow),
    Burn(BurnRow),
    PoolCreation(PoolCreationRow),
    Liquidity(LiquidityRow),
    Migration(MigrationRow),
    FeeCollection(FeeCollectionRow),
    SupplyLockAction(SupplyLockActionRow),
}

// EventPayload struct remains the same
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EventPayload {
    pub timestamp: u32,
    pub event: EventType,
    pub balances: TokenBalanceMap,
    pub token_decimals: HashMap<String, u8>,
}

// MODIFIED: Constructor now uses u32 timestamps
impl WalletHoldingRow {
    pub fn new(wallet_address: String, mint_address: String, timestamp_secs: u32) -> Self {
        Self {
            wallet_address,
            mint_address,
            current_balance: 0.0,
            history_bought_amount: 0.0,
            history_bought_cost_sol: 0.0,
            history_sold_amount: 0.0,
            history_sold_income_sol: 0.0,
            realized_profit_pnl: 0.0,
            realized_profit_sol: 0.0,
            realized_profit_usd: 0.0,
            start_holding_at: timestamp_secs,
            updated_at: timestamp_secs,
            history_transfer_in: 0,
            history_transfer_out: 0,
        }
    }
}

impl WalletProfileRow {
    pub fn new(wallet_address: String, timestamp: u32) -> Self {
        Self {
            updated_at: timestamp,
            first_seen_ts: timestamp,
            last_seen_ts: timestamp,

            wallet_address,
            tags: Vec::new(),
            deployed_tokens: Vec::new(),
            funded_from: String::new(),
            funded_timestamp: 0,
            funded_signature: String::new(),
            funded_amount: 0.0,
        }
    }
}

impl WalletProfileMetricsRow {
    pub fn new(wallet_address: String, timestamp: u32) -> Self {
        Self {
            updated_at: timestamp,
            wallet_address,
            balance: 0.0,
            transfers_in_count: 0,
            transfers_out_count: 0,
            spl_transfers_in_count: 0,
            spl_transfers_out_count: 0,
            total_buys_count: 0,
            total_sells_count: 0,
            total_winrate: 0.0,
            stats_1d_realized_profit_sol: 0.0,
            stats_1d_realized_profit_usd: 0.0,
            stats_1d_realized_profit_pnl: 0.0,
            stats_1d_buy_count: 0,
            stats_1d_sell_count: 0,
            stats_1d_transfer_in_count: 0,
            stats_1d_transfer_out_count: 0,
            stats_1d_avg_holding_period: 0.0,
            stats_1d_total_bought_cost_sol: 0.0,
            stats_1d_total_bought_cost_usd: 0.0,
            stats_1d_total_sold_income_sol: 0.0,
            stats_1d_total_sold_income_usd: 0.0,
            stats_1d_total_fee: 0.0,
            stats_1d_winrate: 0.0,
            stats_1d_tokens_traded: 0,
            stats_7d_realized_profit_sol: 0.0,
            stats_7d_realized_profit_usd: 0.0,
            stats_7d_realized_profit_pnl: 0.0,
            stats_7d_buy_count: 0,
            stats_7d_sell_count: 0,
            stats_7d_transfer_in_count: 0,
            stats_7d_transfer_out_count: 0,
            stats_7d_avg_holding_period: 0.0,
            stats_7d_total_bought_cost_sol: 0.0,
            stats_7d_total_bought_cost_usd: 0.0,
            stats_7d_total_sold_income_sol: 0.0,
            stats_7d_total_sold_income_usd: 0.0,
            stats_7d_total_fee: 0.0,
            stats_7d_winrate: 0.0,
            stats_7d_tokens_traded: 0,
            stats_30d_realized_profit_sol: 0.0,
            stats_30d_realized_profit_usd: 0.0,
            stats_30d_realized_profit_pnl: 0.0,
            stats_30d_buy_count: 0,
            stats_30d_sell_count: 0,
            stats_30d_transfer_in_count: 0,
            stats_30d_transfer_out_count: 0,
            stats_30d_avg_holding_period: 0.0,
            stats_30d_total_bought_cost_sol: 0.0,
            stats_30d_total_bought_cost_usd: 0.0,
            stats_30d_total_sold_income_sol: 0.0,
            stats_30d_total_sold_income_usd: 0.0,
            stats_30d_total_fee: 0.0,
            stats_30d_winrate: 0.0,
            stats_30d_tokens_traded: 0,
        }
    }
}

impl TokenStaticRow {
    pub fn new_from_mint(mint: &MintRow) -> Self {
        Self {
            updated_at: mint.timestamp,
            created_at: mint.timestamp,
            token_address: mint.mint_address.clone(),
            name: mint.token_name.clone().unwrap_or_default(),
            symbol: mint.token_symbol.clone().unwrap_or_default(),
            token_uri: mint.token_uri.clone().unwrap_or_default(),
            decimals: mint.token_decimals,
            creator_address: mint.creator_address.clone(),
            pool_addresses: if mint.pool_address.is_empty() { vec![] } else { vec![mint.pool_address.clone()] },
            launchpad: mint.protocol,
            protocol: mint.protocol,
            total_supply: mint.initial_base_liquidity,
            is_mutable: mint.is_mutable,
            update_authority: mint.update_authority.clone(),
            mint_authority: mint.mint_authority.clone(),
            freeze_authority: mint.freeze_authority.clone(),
        }
    }
    
    pub fn new(
        mint_address: String, timestamp: u32, name: String, symbol: String, uri: String, 
        decimals: u8, creator_address: String, pool_addresses: Vec<String>,  protocol: u8, 
        total_supply: u64, is_mutable: bool, update_authority: Option<String>, 
        mint_authority: Option<String>, freeze_authority: Option<String>
    ) -> Self {
        Self {
            updated_at: timestamp,
            created_at: timestamp,
            token_address: mint_address,
            name,
            symbol,
            token_uri: uri,
            decimals,
            creator_address,
            pool_addresses,
            launchpad: 0,
            protocol,
            total_supply,
            is_mutable,
            update_authority,
            mint_authority,
            freeze_authority,
        }
    }
}

impl TokenMetricsRow {
    pub fn new(token_address: String, timestamp: u32) -> Self {
        Self {
            updated_at: timestamp,
            token_address,
            total_volume_usd: 0.0,
            total_buys: 0,
            total_sells: 0,
            unique_holders: 0,
            ath_price_usd: 0.0,
        }
    }
}
