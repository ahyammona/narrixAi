use solana_sdk::signature::{Keypair, Signer};
use xplore::Xplore;

use crate::{
    blockchain::{
        stream_data,
        utils::{lamports_to_sol, load_keypair_from_env},
        PumpFunClient,
    },
    error::PumpFunError,
    types::Trade,
    x::search_tweets,
};

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

async fn stream_flow() -> anyhow::Result<Trade> {
    let (rpc_url, wss_url) = stream_endpoints();
    let streamed_data = stream_data(&rpc_url, &wss_url).await?;
    Ok(streamed_data)
}

pub(crate) async fn create_token_flow(
    network_choice: &str,
    create_mode: &str,
    token_info_decision: [&str; 5],
) -> anyhow::Result<String> {
    let wallet = load_keypair_from_env("PRIVATE_KEY")?;
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
    let symbol = token_info[1];
    let uri = token_info[2];

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
                    &wallet,
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
                    &wallet,
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

    Ok(mint_pubkey.to_string())
}
