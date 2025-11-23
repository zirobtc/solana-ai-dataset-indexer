/// Tracks direct capital flow and identifies funding chains.
pub struct TransferLink {
    pub signature: String,
    pub source: String,
    pub destination: String,
    pub mint: String,
    pub timestamp: i64,
    pub amount: f64,
}

/// Identifies wallets trading the same token in the same slot.
pub struct BundleTradeLink {
    pub signatures: Vec<String>,
    pub wallet_a: String,
    pub wallet_b: String,
    pub mint: String,
    pub slot: i64,
    pub timestamp: i64,
}

/// Reveals a behavioral pattern of one wallet mirroring another's successful trade.
pub struct CopiedTradeLink {
    pub timestamp: i64,
    pub leader_buy_sig: String,
    pub leader_sell_sig: String,
    pub follower_buy_sig: String,
    pub follower_sell_sig: String,
    pub follower: String,
    pub leader: String,
    pub mint: String,
    pub time_gap_on_buy_sec: i64,
    pub time_gap_on_sell_sec: i64,
    pub leader_pnl: f64,
    pub follower_pnl: f64,

    pub leader_buy_total: f64,
    pub leader_sell_total: f64,

    pub follower_buy_total: f64,
    pub follower_sell_total: f64,
    pub follower_buy_slippage: f32,
    pub follower_sell_slippage: f32,
}

/// Represents a link where a group of wallets re-engage with a token in a coordinated manner.
pub struct CoordinatedActivityLink {
    pub timestamp: i64,
    pub leader_first_sig: String,
    pub leader_second_sig: String,
    pub follower_first_sig: String,
    pub follower_second_sig: String,
    pub follower: String,
    pub leader: String,
    pub mint: String,
    pub time_gap_on_first_sec: i64,
    pub time_gap_on_second_sec: i64,
}

/// Links a token to its original creator.
pub struct MintedLink {
    pub signature: String,
    pub timestamp: i64,
    pub buy_amount: f64,
}

/// Connects a token to its successful first-movers.
pub struct SnipedLink {
    pub timestamp: i64,
    pub signature: String,
    pub rank: i64,
    pub sniped_amount: f64,
}

/// Represents connection between wallet that locked supply.
pub struct LockedSupplyLink {
    pub timestamp: i64,
    pub signature: String,
    pub amount: f64,
    pub unlock_timestamp: u64,
}

/// link of the   wallet that burned tokens.
pub struct BurnedLink {
    pub signature: String,
    pub amount: f64,
    pub timestamp: i64,
}

/// Identifies wallets that provided liquidity, signaling high conviction.
pub struct ProvidedLiquidityLink {
    pub signature: String,
    pub wallet: String,
    pub token: String,
    pub pool_address: String,
    pub amount_base: f64,
    pub amount_quote: f64,
    pub timestamp: i64,
}

/// A derived link connecting a token to its largest holders.
pub struct WhaleOfLink {
    pub timestamp: i64,
    pub wallet: String,
    pub token: String,
    pub holding_pct_at_creation: f32, // Holding % when the link was made
    pub ath_usd_at_creation: f64,     // Token's ATH when the link was made
}

/// A derived link connecting a token to its most profitable traders.
pub struct TopTraderOfLink {
    pub timestamp: i64,
    pub wallet: String,
    pub token: String,
    pub pnl_at_creation: f64,     // The PNL that first triggered the link
    pub ath_usd_at_creation: f64, // Token's ATH when the link was made
}
