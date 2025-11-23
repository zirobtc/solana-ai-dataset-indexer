use anchor_lang::{AnchorDeserialize, AnchorSerialize};
use borsh::{BorshDeserialize, BorshSerialize};
use serde::Serialize;
use solana_sdk::pubkey::Pubkey;

#[derive(AnchorSerialize, AnchorDeserialize, Debug, Serialize, Clone)]
pub struct CreateArgs {
    pub name: String,
    pub symbol: String,
    pub uri: String,
    pub creator: Pubkey,
}

#[derive(AnchorSerialize, AnchorDeserialize, Debug, Serialize, Clone)]
pub struct BuyArgs {
    pub amount: u64,
    pub max_sol_cost: u64,
}

#[derive(AnchorSerialize, AnchorDeserialize, Debug, Serialize, Clone)]
pub struct SellArgs {
    pub amount: u64,
    pub min_sol_output: u64,
}

#[derive(AnchorSerialize, AnchorDeserialize, Clone, Debug, Serialize)]
pub struct CreateEvent {
    pub name: String,
    pub symbol: String,
    pub uri: String,
    pub mint: Pubkey,
    pub bonding_curve: Pubkey,
    pub user: Pubkey,
    pub creator: Pubkey,
    pub timestamp: i64,
    pub virtual_token_reserves: u64,
    pub virtual_sol_reserves: u64,
    pub real_token_reserves: u64,
    pub token_total_supply: u64,
}

#[derive(AnchorSerialize, AnchorDeserialize, Clone, Debug, Serialize)]
pub struct TradeEvent {
    pub mint: Pubkey,
    pub sol_amount: u64,
    pub token_amount: u64,
    pub is_buy: bool,
    pub user: Pubkey,
    pub timestamp: i64,
    pub virtual_sol_reserves: u64,
    pub virtual_token_reserves: u64,
    pub real_sol_reserves: u64,
    pub real_token_reserves: u64,
    pub fee_recipient: Pubkey,
    pub fee_basis_points: u64,
    pub fee: u64,
    pub creator: Pubkey,
    pub creator_fee_basis_points: u64,
    pub creator_fee: u64,
    pub track_volume: bool,
    pub total_unclaimed_tokens: u64,
    pub total_claimed_tokens: u64,
    pub current_sol_volume: u64,
    pub last_update_timestamp: i64,
}

#[derive(AnchorSerialize, AnchorDeserialize, Clone, Debug, Serialize)]
pub struct CompletePumpAmmMigrationEvent {
    pub user: Pubkey,
    pub mint: Pubkey,
    pub mint_amount: u64,
    pub sol_amount: u64,
    pub pool_migration_fee: u64,
    pub bonding_curve: Pubkey,
    pub timestamp: i64,
    pub pool: Pubkey,
}

#[derive(AnchorSerialize, AnchorDeserialize, Clone, Debug, Serialize)]
pub struct CollectCreatorFeeEvent {
    pub timestamp: i64,
    pub creator: Pubkey,
    pub creator_fee: u64,
}
