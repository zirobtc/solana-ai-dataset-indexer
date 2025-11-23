use anyhow::Result;
use async_trait::async_trait;
use clickhouse::Client;
use redis::aio::MultiplexedConnection;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use solana_sdk::{pubkey::Pubkey, transaction::VersionedTransaction};
use std::io::{Cursor, Read};
use yellowstone_grpc_proto::prelude::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterTransactions, TransactionStatusMeta,
    subscribe_update::UpdateOneof,
};

use crate::types::{EventPayload, FormattedInstruction, UnifiedTransaction};

// pub mod meteora_bonding_curve_handler;
pub mod pump_fun_amm_handler;
pub mod pump_fun_amm_idl;
pub mod pump_fun_launchpad_handler;
pub mod pump_fun_launchpad_idl;
// pub mod raydium_cpmm_handler;
// pub mod raydium_launchpad_handler;
pub mod system_handler;
pub mod spl_token_handler;
pub mod streamflow_handler;
pub mod streamflow_idl;

// A struct to hold all the common data about a transaction
// This avoids passing 10+ arguments to every function.
pub struct TransactionInfo<'a> {
    pub signature: &'a str,
    pub slot: u64,
    pub block_time: i64,
    pub signer: &'a str,
    pub is_success: bool,
    pub error: &'a Option<String>,
    pub priority_fee: Option<u64>,
    pub tx: &'a VersionedTransaction,
    pub meta: &'a TransactionStatusMeta,
    pub account_keys: &'a [Pubkey],
    pub formatted_instructions: &'a [FormattedInstruction<'a>],
    pub logs: &'a [String],
    pub tip: Option<u64>,
    pub mev_protection: u8,
}

// The core trait for any protocol-specific handler
#[async_trait]
pub trait TransactionHandler: Send + Sync {
    // A unique name for logging purposes
    fn name(&self) -> &'static str;

    // Check if the transaction is relevant to this handler
        fn is_of_interest(&self, tx: &UnifiedTransaction) -> bool;

    // Process the transaction and store its data in the database
    async fn process_transaction(
        &self,
        tx: &UnifiedTransaction,
        native_price_usd: f64,
    ) -> Result<Vec<EventPayload>>;
}

pub fn anchor_event_discriminator(name: &str) -> [u8; 8] {
    let preimage = format!("event:{}", name);
    let mut hasher = Sha256::new();
    hasher.update(preimage.as_bytes());
    let hash = hasher.finalize();
    let mut disc = [0u8; 8];
    disc.copy_from_slice(&hash[..8]);
    disc
}

pub fn read_pubkey(cursor: &mut Cursor<&[u8]>) -> Result<Pubkey, std::io::Error> {
    let mut buf = [0u8; 32];
    cursor.read_exact(&mut buf)?;
    Ok(Pubkey::new_from_array(buf))
}

pub fn read_u64(cursor: &mut Cursor<&[u8]>) -> Result<u64, std::io::Error> {
    let mut buf = [0u8; 8];
    cursor.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}

pub fn read_i64(cursor: &mut Cursor<&[u8]>) -> Result<i64, std::io::Error> {
    let mut buf = [0u8; 8];
    cursor.read_exact(&mut buf)?;
    Ok(i64::from_le_bytes(buf))
}

pub fn read_bool(cursor: &mut Cursor<&[u8]>) -> Result<bool, std::io::Error> {
    let mut buf = [0u8; 1];
    cursor.read_exact(&mut buf)?;
    Ok(buf[0] != 0)
}

#[derive(Deserialize)]
pub struct IdlAccount {
    // Add this struct
    name: String,
}

#[derive(Deserialize)]
pub struct IdlEvent {
    name: String,
}
#[derive(Deserialize)]
pub struct IdlInstruction {
    name: String,
    discriminator: Vec<u8>,
    accounts: Vec<IdlAccount>,
}

#[derive(Deserialize)]
pub struct Idl {
    address: String,
    instructions: Vec<IdlInstruction>,
    events: Vec<IdlEvent>,
}

pub mod constants {
    pub const PROTOCOL_PUMPFUN_LAUNCHPAD: u8 = 1;
    pub const NATIVE_MINT: &str = "So11111111111111111111111111111111111111112";
    pub const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
    pub const USDT_MINT: &str = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB";
    pub const USD1_MINT: &str = "USD1ttGY1N17NEEHLmELoaybftRBUSErhqYiQzvEmuB";
    pub const TRADE_TYPE_BUY: u8 = 0;
    pub const TRADE_TYPE_SELL: u8 = 1;
    pub const PUMP_TOTAL_SUPPLY: u64 = 1_000_000_000_000_000;
    pub const PROTOCOL_RAYDIUM_LAUNCHPAD: u8 = 2;
    pub const RAYDIUM_LAUNCHPAD_TOTAL_SUPPLY: u64 = 1_000_000_000_000_000;
    pub const PROTOCOL_PUMPFUN_AMM: u8 = 3;
    pub const PROTOCOL_RAYDIUM_CPMM: u8 = 4;


    pub const VESTING_ACTION_WITHDRAW: u8 = 1;
    pub const VESTING_ACTION_TOPUP: u8 = 0;
    pub const VESTING_ACTION_CANCEL: u8 = 2;
    pub const PROTOCOL_METEORA_BONDING: u8 = 5;

    // Platforms

    // Base ones
    pub const UNKNOWN: u8 = 0;
    pub const PLATFORM_PUMPFUN: u8 = 1;
    pub const PLATFORM_RAYDIUM: u8 = 2;

    // Trading Platforms
    pub const PLATFORM_AXIOM: u8 = 3;
    pub const AXIOM_FEE_ADDRESSES: [&str; 2] = [
        "4FobGn5ZWYquoJkxMzh2VUAWvV36xMgxQ3M7uG1pGGhd",
        "DYVeNgXGLAhZdeLMMYnCw1nPnMxkBN7fJnNpHmizTrrF"
    ];
    pub const AXIOM_ROUTER_PROGRAM_ID: &str = "6HB1VBBS8LrdQiR9MZcXV5VdpKFb7vjTMZuQQEQEPioC";

    // Trojan
    pub const PLATFORM_TROJAN: u8 = 4; 
    pub const TROJAN_ROUTER_PROGRAM_ID: &str = "troY36YiPGqMyAYCNbEqYCdN2tb91Zf7bHcQt7KUi61";

    // BULLX
    pub const PLATFORM_BULLX: u8 = 5; 
    pub const BULLX_ROUTER_PROGRAM_ID: &str = "3soiUx2xiUB4wvooFKsQC3UqDR9tDKBgqEBPtpdT93Gu";
    pub const BULLX_FEE_ADDRESSES: [&str; 1] = [
        "9RYJ3qr5eU5xAooqVcbmdeusjcViL5Nkiq7Gske3tiKq",
        // Add more BullX fee addresses here
    ];

    // Locking Protocols
    pub const PROTOCOL_STREAMFLOW: u8 = 1;

}
