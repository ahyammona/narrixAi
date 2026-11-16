use anyhow::Result;
use futures_util::StreamExt;
use solana_client::{
    nonblocking::pubsub_client::PubsubClient,
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcTransactionLogsConfig, RpcTransactionLogsFilter},
};
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey};
use std::collections::HashMap;
use std::str::FromStr;
use tokio::sync::mpsc;

use crate::{
    api::fetch_token_data,
    blockchain::decode_data::{self, DecodedPumpFunEvent},
    types::{PoolMetrics, Trade},
};

const PUMPFUN_PROGRAM_ID: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const METADATA_PROGRAM_ID: &str = "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s";

#[derive(Clone, Default)]
struct TokenMetadata {
    name: Option<String>,
    symbol: Option<String>,
    uri: Option<String>,
}

pub async fn stream_data(rpc_url: &str, wss: &str) -> Result<Trade> {
    let (sender, mut receiver) = mpsc::channel(16);
    let stream = stream_data_to_channel(rpc_url, wss, sender);
    tokio::pin!(stream);

    let mut last_trade = None;
    loop {
        tokio::select! {
            result = &mut stream => {
                result?;
                break;
            }
            trade = receiver.recv() => {
                if let Some(trade) = trade {
                    last_trade = Some(trade);
                }
            }
        }
    }

    last_trade.ok_or_else(|| anyhow::anyhow!("stream ended before receiving a token event"))
}

pub async fn stream_data_to_channel(
    _rpc_url: &str,
    wss: &str,
    sender: mpsc::Sender<Trade>,
) -> Result<()> {
    let pubsub_client = PubsubClient::new(&wss.to_string()).await?;

    let filter = RpcTransactionLogsFilter::Mentions(vec![PUMPFUN_PROGRAM_ID.to_string()]);

    let config = RpcTransactionLogsConfig {
        commitment: Some(CommitmentConfig::processed()),
    };

    let (mut stream, _unsubscribe) = pubsub_client.logs_subscribe(filter, config).await?;
    let rpc_client = RpcClient::new(_rpc_url.to_string());
    let mut token_metadata = HashMap::<String, TokenMetadata>::new();
    let mut observed_fees = HashMap::<String, ObservedFees>::new();

    println!("🚀 Listening for new Pump.fun token mints...");
    while let Some(response) = stream.next().await {
        let log_info = response.value;

        if log_info.err.is_some() {
            continue;
        }
        for event in extract_decoded_events(&log_info.logs) {
            match event {
                DecodedPumpFunEvent::Create(event) => {
                    token_metadata.insert(
                        event.mint.to_string(),
                        TokenMetadata {
                            name: Some(event.name),
                            symbol: Some(event.symbol),
                            uri: Some(event.uri),
                        },
                    );
                }
                DecodedPumpFunEvent::Trade(decoded) => {
                    let mint = decoded.mint.to_string();
                    if !token_metadata.contains_key(&mint) {
                        match fetch_token_metadata(&rpc_client, &decoded.mint).await {
                            Ok(metadata) => {
                                token_metadata.insert(mint.clone(), metadata);
                            }
                            Err(error) => {
                                log::debug!("Could not load on-chain metadata for {mint}: {error}");
                            }
                        }
                    }
                    let totals = observed_fees.entry(mint.clone()).or_default();
                    totals.trade_count = totals
                        .trade_count
                        .checked_add(1)
                        .ok_or_else(|| anyhow::anyhow!("trade count overflow for {mint}"))?;
                    totals.fees = totals
                        .fees
                        .checked_add(decoded.fee)
                        .ok_or_else(|| anyhow::anyhow!("fee total overflow for {mint}"))?;
                    totals.creator_fees = totals
                        .creator_fees
                        .checked_add(decoded.creator_fee)
                        .ok_or_else(|| anyhow::anyhow!("creator fee total overflow for {mint}"))?;

                    let pair = match fetch_token_data(&mint).await {
                        Ok(pairs) if !pairs.is_empty() => Some(pairs),
                        Ok(_) => None,
                        Err(err) => {
                            log::debug!("DexScreener lookup failed for {mint}: {err}");
                            None
                        }
                    };
                    let dex_token = pair.as_ref().and_then(|pairs| {
                        pairs
                            .iter()
                            .find(|pair| pair.base_token.address == mint)
                            .or_else(|| pairs.first())
                    });
                    let metadata = token_metadata.get(&mint).cloned().unwrap_or_default();
                    let token_name = dex_token
                        .map(|pair| pair.base_token.name.clone())
                        .filter(|name| !name.trim().is_empty())
                        .or(metadata.name);
                    let token_symbol = dex_token
                        .map(|pair| pair.base_token.symbol.clone())
                        .filter(|symbol| !symbol.trim().is_empty())
                        .or(metadata.symbol);

                    let trade = Trade {
                        pair,
                        token_name,
                        token_symbol,
                        metadata_uri: metadata.uri,
                        pool: PoolMetrics {
                            trade_count: totals.trade_count,
                            observed_fees_lamports: totals.fees,
                            observed_creator_fees_lamports: totals.creator_fees,
                            latest_trade_sol_amount: decoded.sol_amount,
                            latest_creator_fee_lamports: decoded.creator_fee,
                            latest_fee_lamports: decoded.fee,
                            current_sol_volume_lamports: decoded.current_sol_volume,
                            virtual_sol_reserves: decoded.virtual_sol_reserves,
                            real_sol_reserves: decoded.real_sol_reserves,
                            virtual_token_reserves: decoded.virtual_token_reserves,
                            real_token_reserves: decoded.real_token_reserves,
                        },
                        decoded,
                    };
                    sender
                        .send(trade)
                        .await
                        .map_err(|_| anyhow::anyhow!("token event receiver was closed"))?;
                }
            }
        }
    }

    Ok(())
}

async fn fetch_token_metadata(rpc_client: &RpcClient, mint: &Pubkey) -> Result<TokenMetadata> {
    let metadata_program = Pubkey::from_str(METADATA_PROGRAM_ID)?;
    let (metadata_address, _) = Pubkey::find_program_address(
        &[b"metadata", metadata_program.as_ref(), mint.as_ref()],
        &metadata_program,
    );
    let account = rpc_client.get_account(&metadata_address).await?;
    parse_metadata(&account.data)
}

fn parse_metadata(data: &[u8]) -> Result<TokenMetadata> {
    const METADATA_HEADER_LEN: usize = 1 + 32 + 32;
    if data.len() < METADATA_HEADER_LEN {
        anyhow::bail!("metadata account is shorter than its fixed header");
    }

    let mut offset = METADATA_HEADER_LEN;
    let name = clean_metadata_string(read_borsh_string(data, &mut offset)?);
    let symbol = clean_metadata_string(read_borsh_string(data, &mut offset)?);
    let uri = read_borsh_string(data, &mut offset)?;
    Ok(TokenMetadata {
        name: (!name.is_empty()).then_some(name),
        symbol: (!symbol.is_empty()).then_some(symbol),
        uri: {
            let uri = clean_metadata_string(uri);
            (!uri.is_empty()).then_some(uri)
        },
    })
}

fn clean_metadata_string(value: String) -> String {
    value.trim_matches(char::from(0)).trim().to_string()
}

fn read_borsh_string(data: &[u8], offset: &mut usize) -> Result<String> {
    let length_end = offset
        .checked_add(4)
        .ok_or_else(|| anyhow::anyhow!("metadata string offset overflow"))?;
    let length_bytes: [u8; 4] = data
        .get(*offset..length_end)
        .ok_or_else(|| anyhow::anyhow!("metadata account ended before string length"))?
        .try_into()?;
    *offset = length_end;

    let string_length = usize::try_from(u32::from_le_bytes(length_bytes))?;
    let string_end = offset
        .checked_add(string_length)
        .ok_or_else(|| anyhow::anyhow!("metadata string length overflow"))?;
    let value = data
        .get(*offset..string_end)
        .ok_or_else(|| anyhow::anyhow!("metadata account ended inside string data"))?;
    *offset = string_end;
    Ok(std::str::from_utf8(value)?.to_string())
}

#[derive(Default)]
struct ObservedFees {
    trade_count: u64,
    fees: u64,
    creator_fees: u64,
}

fn extract_decoded_events(logs: &[String]) -> Vec<DecodedPumpFunEvent> {
    decode_data::extract_log_messages(logs)
        .filter_map(|message| match decode_data::convert_to_discm(message) {
            Ok(bytes) if bytes.len() < 8 => None,
            Ok(bytes) => match decode_data::decode_event_data(&bytes) {
                Ok(event) => Some(event),
                Err(error) if error.message == "Unsupported event discriminator." => None,
                Err(error) => {
                    log::debug!("Failed to decode Pump.fun event: {}", error.message);
                    None
                }
            },
            Err(error) => {
                log::debug!("Failed to decode base64 log message: {error}");
                None
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn append_borsh_string(data: &mut Vec<u8>, value: &str) {
        data.extend_from_slice(&(value.len() as u32).to_le_bytes());
        data.extend_from_slice(value.as_bytes());
    }

    #[test]
    fn parses_name_symbol_and_uri_from_metadata_account() {
        let mut data = vec![0; 1 + 32 + 32];
        append_borsh_string(&mut data, "Token");
        append_borsh_string(&mut data, "TOK");
        append_borsh_string(&mut data, "https://example.com/metadata.json");

        let metadata = parse_metadata(&data).unwrap();
        assert_eq!(metadata.name.as_deref(), Some("Token"));
        assert_eq!(metadata.symbol.as_deref(), Some("TOK"));
        assert_eq!(
            metadata.uri.as_deref(),
            Some("https://example.com/metadata.json")
        );
    }
}
