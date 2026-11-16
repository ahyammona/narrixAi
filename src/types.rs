use serde::{Deserialize, Serialize};

use crate::blockchain::event;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    Onchain,
    X,
}

#[derive(Debug, Clone)]
pub struct Signal {
    pub source: Source,
    pub text: String,
    pub weight: f64,
    pub source_id: Option<String>, // NEW: mint or tweet id, for dedupe/trace
}
#[derive(Debug)]
pub struct Trade {
    pub pair: Option<Vec<Pair>>,
    pub decoded: event::TradeEvent,
    pub token_name: Option<String>,
    pub token_symbol: Option<String>,
    pub metadata_uri: Option<String>,
    pub pool: PoolMetrics,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PoolMetrics {
    pub trade_count: u64,
    pub observed_fees_lamports: u64,
    pub observed_creator_fees_lamports: u64,
    pub latest_trade_sol_amount: u64,
    pub latest_creator_fee_lamports: u64,
    pub latest_fee_lamports: u64,
    pub current_sol_volume_lamports: u64,
    pub virtual_sol_reserves: u64,
    pub real_sol_reserves: u64,
    pub virtual_token_reserves: u64,
    pub real_token_reserves: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Pair {
    pub chain_id: String,
    pub dex_id: String,
    pub url: String,
    pub pair_address: String,
    pub base_token: Token,
    pub quote_token: Token,
    pub price_native: String,
    #[serde(default)]
    pub price_usd: Option<String>,
    #[serde(default)]
    pub txns: Option<Txns>,
    #[serde(default)]
    pub volume: Option<Volume>,
    #[serde(default)]
    pub price_change: Option<PriceChange>,
    #[serde(default)]
    pub liquidity: Option<Liquidity>,
    #[serde(default)]
    pub fdv: Option<f64>,
    #[serde(default)]
    pub market_cap: Option<f64>,
    #[serde(default)]
    pub pair_created_at: Option<u64>,
    #[serde(default)]
    pub labels: Option<Vec<String>>,
    #[serde(default)]
    pub info: Option<PairInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PairInfo {
    #[serde(default)]
    pub image_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Token {
    pub address: String,
    pub name: String,
    pub symbol: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxnCount {
    pub buys: u64,
    pub sells: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Txns {
    #[serde(default)]
    pub m5: Option<TxnCount>,
    #[serde(default)]
    pub h1: Option<TxnCount>,
    #[serde(default)]
    pub h6: Option<TxnCount>,
    #[serde(default)]
    pub h24: Option<TxnCount>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Volume {
    #[serde(default)]
    pub m5: Option<f64>,
    #[serde(default)]
    pub h1: Option<f64>,
    #[serde(default)]
    pub h6: Option<f64>,
    #[serde(default)]
    pub h24: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceChange {
    #[serde(default)]
    pub m5: Option<f64>,
    #[serde(default)]
    pub h1: Option<f64>,
    #[serde(default)]
    pub h6: Option<f64>,
    #[serde(default)]
    pub h24: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Liquidity {
    #[serde(default)]
    pub usd: Option<f64>,
    #[serde(default)]
    pub base: Option<f64>,
    #[serde(default)]
    pub quote: Option<f64>,
}
