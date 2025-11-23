use borsh::{BorshDeserialize, BorshSerialize};
use serde::Serialize;

#[derive(BorshDeserialize, Debug, Serialize, Clone)]
pub struct CreateArgs {
    pub start_time: u64,
    pub net_amount_deposited: u64,
    pub period: u64,
    pub amount_per_period: u64,
    pub cliff: u64,
    pub cliff_amount: u64,
    pub cancelable_by_sender: bool,
    pub cancelable_by_recipient: bool,
    pub automatic_withdrawal: bool,
    pub transferable_by_sender: bool,
    pub transferable_by_recipient: bool,
    pub can_topup: bool,
    #[serde(with = "serde_bytes")]
    pub stream_name: [u8; 64],
    pub withdraw_frequency: u64,
    pub pausable: Option<bool>,
    pub can_update_rate: Option<bool>,
}

#[derive(BorshDeserialize, Debug, Serialize, Clone)]
pub struct WithdrawArgs {
    pub amount: u64,
}

#[derive(BorshDeserialize, Debug, Serialize, Clone)]
pub struct TopupArgs {
    pub amount: u64,
}

#[derive(BorshDeserialize, Debug, Serialize, Clone)]
pub struct UpdateArgs {
    pub enable_automatic_withdrawal: Option<bool>,
    pub withdraw_frequency: Option<u64>,
    pub amount_per_period: Option<u64>,
}
