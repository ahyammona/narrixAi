use anyhow::Result;
use futures_util::StreamExt;
use solana_client::{
    nonblocking::{pubsub_client::PubsubClient, rpc_client::RpcClient},
    rpc_config::{RpcTransactionConfig, RpcTransactionLogsConfig, RpcTransactionLogsFilter},
    rpc_response::RpcLogsResponse,
};
use solana_sdk::{commitment_config::CommitmentConfig, signature::Signature};
use solana_transaction_status::{
    option_serializer::OptionSerializer, TransactionWithStatusMeta, UiTransactionEncoding,
};
use std::{str::FromStr, sync::Arc};

use crate::decode_data;

const PUMPFUN_PROGRAM_ID: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";

pub async fn stream_data(rpc_url: &str, wss: &str) -> Result<()> {
    let rpc_client = Arc::new(RpcClient::new_with_commitment(
        rpc_url.to_string(),
        CommitmentConfig::confirmed(),
    ));

    let pubsub_client = PubsubClient::new(&wss.to_string()).await?;

    let filter = RpcTransactionLogsFilter::Mentions(vec![PUMPFUN_PROGRAM_ID.to_string()]);

    let config = RpcTransactionLogsConfig {
        commitment: Some(CommitmentConfig::processed()),
    };

    let (mut stream, _unsubscribe) = pubsub_client.logs_subscribe(filter, config).await?;

    println!("🚀 Listening for new Pump.fun token mints...");
    while let Some(response) = stream.next().await {
        let log_info = response.value;
        // println!("{:?}", log_info);

        if log_info.err.is_some() {
            continue;
        }

        if let Some(encoded_data) = extract_decoded_event(log_info) {
            println!("{:#?}", encoded_data);
        }
    }

    Ok(())
}

fn extract_decoded_event(txn: RpcLogsResponse) -> Option<decode_data::DecodedEvent> {
    let logs = txn.logs;
    if let Some(data_msg) = decode_data::extract_log_message(&logs) {
        match base64::decode(&data_msg) {
            Ok(decoded_bytes) => match decode_data::decode_event_data(&decoded_bytes) {
                Ok(event) => return Some(event),
                Err(err) => {}
            },
            Err(err) => {
                eprintln!("❌ Failed to decode base64 log message: {}", err);
            }
        }
    }
    None
}
