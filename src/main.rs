mod ai;
mod api;
mod blockchain;
mod config;
mod error;
mod logic;
mod telegram;
mod telegram_bot;
mod types;
mod x;

use ai::run_agent;
use config::{Config, Secrets};
use std::{
    env,
    io::{self, Write},
};
use telegram::TelegramNotifier;
use xplore::Xplore;

/// Shared context used by the X module.
#[allow(dead_code)]
pub struct Ctx {
    pub cfg: Config,
    pub secrets: Secrets,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    dotenv::dotenv().map_err(|error| {
        anyhow::anyhow!("Failed to load .env from the current directory: {error}")
    })?;

    let args: Vec<String> = env::args().skip(1).collect();
    match args.as_slice() {
        [] => {}
        [arg] if arg == "--telegram-bot" => {
            let bot = telegram_bot::TelegramBot::from_env().await?;
            return tokio::task::LocalSet::new().run_until(bot.run()).await;
        }
        _ => anyhow::bail!(
            "Unknown argument(s): {}. Use --telegram-bot to start the Telegram control bot.",
            args.join(" ")
        ),
    }

    let launch_network = prompt_for_launch_network()?;

    let mut xplore = Xplore::new(None).await.expect("Failed to init Xplore");
    let cookie = env::var("X_COOKIE_STRING").expect("X_COOKIE_STRING not set in .env");
    xplore.set_cookie(&cookie).await;

    let gemini_api_key = env::var("GEMINI_API_KEY")
        .map_err(|_| anyhow::anyhow!("GEMINI_API_KEY not set in .env"))?;
    let groq_api_key = env::var("GROQ_API_KEY")
        .ok()
        .filter(|key| !key.trim().is_empty());
    if groq_api_key.is_none() {
        eprintln!("GROQ_API_KEY is not set; Gemini will run without a Groq fallback.");
    }
    let telegram = TelegramNotifier::from_env()?;
    if telegram.is_none() {
        eprintln!("Telegram notifications are disabled; set TELEGRAM_BOT_API and CHAT_BOT_API to enable them.");
    }
    run_agent(
        &gemini_api_key,
        groq_api_key.as_deref(),
        &mut xplore,
        &launch_network,
        telegram.as_ref(),
    )
    .await
}

fn prompt_for_launch_network() -> anyhow::Result<String> {
    println!("Select launch network:\n1. Devnet (recommended)\n2. Mainnet");
    print!("Choice: ");
    io::stdout().flush()?;

    let mut choice = String::new();
    io::stdin().read_line(&mut choice)?;
    let choice = choice.trim();
    match choice {
        "1" | "2" => Ok(choice.to_string()),
        _ => anyhow::bail!("Launch network must be 1 (devnet) or 2 (mainnet)"),
    }
}
