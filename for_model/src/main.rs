use pumpfun_new_mint::{
    stream_data,
    utils::{lamports_to_sol, load_keypair_from_env},
    PumpFunClient,
};
use solana_sdk::signature::{Keypair, Signer};
use std::io::{self, Write};

/// Print a prompt, flush stdout so it shows before input, and read one trimmed line.
fn prompt(label: &str) -> io::Result<String> {
    print!("{}", label);
    io::stdout().flush()?;
    let mut buf = String::new();
    io::stdin().read_line(&mut buf)?;
    Ok(buf.trim().to_string())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    dotenv::dotenv().ok();

    println!("🚀 Pump.fun Toolkit");
    println!("==========================\n");

    println!("What do you want to do?");
    println!("1. Create a token");
    println!("2. Stream new Pump.fun mints (runs until Ctrl+C)");
    let mode = prompt("Choice: ")?;

    println!("\nSelect network:");
    println!("1. Devnet (testing)");
    println!("2. Mainnet (real)");
    let network_choice = prompt("Choice: ")?;

    match mode.as_str() {
        "1" => create_token_flow(&network_choice).await,
        "2" => stream_flow(&network_choice).await,
        _ => anyhow::bail!("❌ Invalid choice"),
    }
}

/// Listens for new Pump.fun mints. Never returns unless the connection fails.
async fn stream_flow(network_choice: &str) -> anyhow::Result<()> {
    // Default endpoints per network; override with RPC_URL / WSS_URL in .env.
    let (default_rpc, default_wss) = match network_choice {
        "1" => (
            "https://api.devnet.solana.com",
            "wss://api.devnet.solana.com",
        ),
        "2" => (
            "https://api.mainnet-beta.solana.com",
            "wss://api.mainnet-beta.solana.com",
        ),
        _ => anyhow::bail!("❌ Invalid network choice"),
    };

    let rpc_url = std::env::var("RPC_URL").unwrap_or_else(|_| default_rpc.to_string());
    let wss_url = std::env::var("WSS_URL").unwrap_or_else(|_| default_wss.to_string());

    println!("🌐 RPC: {}", rpc_url);
    println!("🔌 WSS: {}", wss_url);

    stream_data(&rpc_url, &wss_url).await
}

async fn create_token_flow(network_choice: &str) -> anyhow::Result<()> {
    let wallet =
        load_keypair_from_env("PRIVATE_KEY").expect("❌ Please set PRIVATE_KEY in .env file");
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

    println!("\nSelect create method:");
    println!("1. Create_v2");
    println!("2. Create");
    let method_choice = prompt("Choice: ")?;

    println!("\n📋 Enter token details:");
    let name = prompt("Token name: ")?;
    let symbol = prompt("Token symbol: ")?;
    let uri = prompt("Token URI (metadata): ")?;

    let result = match method_choice.as_str() {
        "1" => {
            let is_mayhem_mode = prompt("Mayhem mode? (y/n, default: n): ")?.to_lowercase() == "y";
            let is_cashback_enabled =
                prompt("Cashback enabled? (y/n, default: n): ")?.to_lowercase() == "y";

            println!("\n🔑 Generating new mint keypair...");
            let mint = Keypair::new();
            println!("   Mint address: {}", mint.pubkey());

            println!("\n📤 Creating token with create_v2...");
            client
                .create_token_v2(
                    &wallet,
                    &mint,
                    name,
                    symbol,
                    uri,
                    is_mayhem_mode,
                    is_cashback_enabled,
                )
                .await
        }
        "2" => {
            println!("\n📤 Creating token with original create...");
            client.create_token(&wallet, name, symbol, uri).await
        }
        _ => anyhow::bail!("❌ Invalid method choice"),
    };

    match result {
        Ok(mint_pubkey) => {
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
        }
        Err(e) => {
            println!("\n❌ Failed: {}", e);
        }
    }

    Ok(())
}
