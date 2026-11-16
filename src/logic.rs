use solana_sdk::signature::{Keypair, Signer};
use xplore::Xplore;

use crate::{
    blockchain::{
        stream_data,
        utils::{lamports_to_sol, sol_to_lamports},
        PumpFunClient,
    },
    error::PumpFunError,
    types::Trade,
    x::search_tweets,
};
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;

pub async fn search_on_x(xplore: &mut Xplore, keyword: &str) -> Result<String, PumpFunError> {
    let tweet_search = search_tweets(xplore, keyword).await?;
    Ok(tweet_search.2.to_string())
}

pub async fn pause_or_start_stream(choice: &str) -> anyhow::Result<Option<Trade>> {
    let decision = match choice {
        "1" => Some(stream_flow().await?),

        "2" => None,
        _ => None,
    };
    Ok(decision)
}

pub(crate) fn stream_endpoints() -> (String, String) {
    (
        "https://api.mainnet-beta.solana.com".to_string(),
        "wss://api.mainnet-beta.solana.com".to_string(),
    )
}

pub(crate) async fn devnet_wallet_balance_sol(wallet: &Keypair) -> anyhow::Result<f64> {
    Ok(PumpFunClient::devnet()
        .check_balance(&wallet.pubkey())
        .await?)
}

pub(crate) async fn sell_all_devnet_tokens(
    wallet: &Keypair,
    mint: &str,
) -> anyhow::Result<Option<String>> {
    let mint = Pubkey::from_str(mint)?;
    let result = PumpFunClient::devnet()
        .sell_all_tokens(wallet, &mint)
        .await?;
    Ok(result.map(|transaction| transaction.signature))
}

async fn stream_flow() -> anyhow::Result<Trade> {
    let (rpc_url, wss_url) = stream_endpoints();
    let streamed_data = stream_data(&rpc_url, &wss_url).await?;
    Ok(streamed_data)
}

pub(crate) async fn create_token_flow(
    wallet: &Keypair,
    network_choice: &str,
    create_mode: &str,
    token_info_decision: [&str; 5],
    buy_amount_sol: Option<f64>,
) -> anyhow::Result<(String, Option<std::result::Result<String, String>>)> {
    if let Some(amount_sol) = buy_amount_sol {
        if network_choice != "1" {
            anyhow::bail!("Automatic buy is enabled only on Devnet");
        }
        if !amount_sol.is_finite() || amount_sol <= 0.0 || amount_sol > 0.03 {
            anyhow::bail!("Devnet buy amount must be greater than zero and at most 0.03 SOL");
        }
    }

    println!("\n📝 Creator Wallet: {}", wallet.pubkey());

    let client = match network_choice {
        "1" => {
            println!("🌐 Using Devnet");
            PumpFunClient::devnet()
        }
        "2" => {
            println!("🌐 Using Mainnet");
            PumpFunClient::mainnet()
        }
        _ => anyhow::bail!("❌ Invalid network choice"),
    };

    let balance = client.check_balance(&wallet.pubkey()).await?;
    println!("💰 Balance: {} SOL", balance);

    if balance < 0.1 {
        anyhow::bail!("❌ Need at least 0.1 SOL for creation");
    }
    let token_info = token_info_decision;
    let name = token_info[0];
    if name.len() > 32 {
        anyhow::bail!("Token name must be at most 32 bytes for Pump.fun metadata");
    }
    let requested_symbol = token_info[1];
    let symbol = truncate_utf8(requested_symbol, 10);
    if symbol != requested_symbol {
        eprintln!(
            "Pump.fun symbols are limited to 10 bytes; using '{symbol}' instead of '{requested_symbol}'."
        );
    }
    let uri = token_info[2];
    if uri.len() > 200 {
        anyhow::bail!("Token metadata URI must be at most 200 bytes for Pump.fun metadata");
    }

    let result = match create_mode {
        "1" => {
            let is_mayhem_mode = !token_info[3].is_empty();
            let is_cashback_enabled = !token_info[4].is_empty();
            println!("\n🔑 Generating new mint keypair...");
            let mint = Keypair::new();
            println!("   Mint address: {}", mint.pubkey());

            println!("\n📤 Creating token with create_v2...");
            client
                .create_token_v2(
                    wallet,
                    &mint,
                    name.to_string(),
                    symbol.to_string(),
                    uri.to_string(),
                    is_mayhem_mode,
                    is_cashback_enabled,
                )
                .await
        }
        "2" => {
            println!("\n📤 Creating token with original create...");
            client
                .create_token(
                    wallet,
                    name.to_string(),
                    symbol.to_string(),
                    uri.to_string(),
                )
                .await
        }
        _ => anyhow::bail!("❌ Invalid method choice"),
    };

    let mint_pubkey = result?;
    println!("\n✅✅✅ TOKEN CREATED SUCCESSFULLY! ✅✅✅");
    println!("   Mint: {}", mint_pubkey);

    let buy_result = if let Some(amount_sol) = buy_amount_sol {
        println!(
            "🛒 Buying {:.6} SOL of the newly created token...",
            amount_sol
        );
        Some(
            client
                .buy_token_with_sol(wallet, &mint_pubkey, sol_to_lamports(amount_sol))
                .await
                .map(|transaction| transaction.signature)
                .map_err(|error| error.to_string()),
        )
    } else {
        None
    };

    println!("\n⏳ Fetching pool info to verify (waiting 3 seconds)...");
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    match client.get_pool_info(&mint_pubkey).await {
        Ok(pool) => {
            println!("\n📊 Pool Information:");
            println!("   Bonding Curve: {}", pool.bonding_curve);
            println!("   Creator: {}", pool.creator);
            println!(
                "   Virtual SOL: {} SOL",
                lamports_to_sol(pool.virtual_sol_reserves)
            );
            println!(
                "   Virtual Tokens: {} tokens",
                lamports_to_sol(pool.virtual_token_reserves)
            );
            println!("   Complete: {}", pool.complete);
        }
        Err(e) => {
            println!("⚠️  Pool not yet available: {}", e);
            println!("   Check back in a few seconds on Solscan.");
        }
    }

    println!("\n📝 SAVE THIS MINT ADDRESS:");
    println!("   TEST_TOKEN_MINT={}", mint_pubkey);

    Ok((mint_pubkey.to_string(), buy_result))
}

fn truncate_utf8(value: &str, max_bytes: usize) -> &str {
    let mut end = value.len().min(max_bytes);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}
