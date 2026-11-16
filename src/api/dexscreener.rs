use crate::error::PumpFunError;
use crate::types::*;
use anyhow::Context;

use serde::Deserialize;

const BASE_URL: &str = "https://api.dexscreener.com";

pub async fn fetch_token_data(mint: &str) -> std::result::Result<Vec<Pair>, PumpFunError> {
    let request = reqwest::Client::builder()
        .user_agent("trading-terminal-backend/0.1")
        .build()
        .expect("building reqwest client");

    let uri = format!("{BASE_URL}/tokens/v1/solana/{mint}");

    let resp = request
        .get(&uri)
        .send()
        .await
        .with_context(|| format!("GET {uri}"))?
        .error_for_status()
        .with_context(|| format!("non-2xx from {uri}"))?;

    let pairs: Vec<Pair> = resp
        .json()
        .await
        .with_context(|| format!("parsing JSON from {uri}"))?;

    Ok(pairs)
}
