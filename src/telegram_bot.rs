use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use ring::{
    aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM},
    pbkdf2,
    rand::{SecureRandom, SystemRandom},
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use solana_sdk::signature::{Keypair, Signer};
use std::{
    collections::HashMap,
    num::NonZeroU32,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{sync::watch, task::JoinHandle};
use zeroize::Zeroize;

use crate::{
    ai::{run_agent_with_wallet, AgentSchedule, AgentSettings},
    blockchain::PumpFunClient,
    telegram::TelegramNotifier,
};

const USER_STORE_DIR: &str = ".telegram-bot/users";
const KDF_ITERATIONS: u32 = 600_000;
const MAX_BUY_SOL: f64 = 0.03;
const MIN_BUY_SOL: f64 = 0.005;
const RESERVED_SOL: f64 = 0.02;
const MAX_HOLD_SECS: u64 = 24 * 60 * 60;
const MIN_HOLD_SECS: u64 = 5 * 60;
const MIN_INTERVAL_SECS: u64 = 30;
const MAX_INTERVAL_SECS: u64 = 6 * 60 * 60;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum UserMode {
    AutoPilot,
    Interval,
}

fn truncate_activity(activity: &str) -> String {
    let mut chars = activity.chars();
    let text: String = chars.by_ref().take(3500).collect();
    if chars.next().is_some() {
        format!("{text}…")
    } else {
        text
    }
}

impl Default for UserMode {
    fn default() -> Self {
        Self::AutoPilot
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EncryptedWallet {
    salt: String,
    nonce: String,
    ciphertext: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UserRecord {
    telegram_id: i64,
    username: Option<String>,
    wallet: Option<EncryptedWallet>,
    #[serde(default)]
    wallet_address: Option<String>,
    mode: UserMode,
    interval_secs: u64,
    max_buy_sol: f64,
    max_hold_secs: u64,
}

impl UserRecord {
    fn new(telegram_id: i64, username: Option<String>) -> Self {
        Self {
            telegram_id,
            username,
            wallet: None,
            wallet_address: None,
            mode: UserMode::AutoPilot,
            interval_secs: 30 * 60,
            max_buy_sol: 0.01,
            max_hold_secs: 60 * 60,
        }
    }
}

#[derive(Debug, Deserialize)]
struct TelegramEnvelope<T> {
    ok: bool,
    result: T,
}

#[derive(Debug, Deserialize)]
struct Update {
    update_id: i64,
    message: Option<Message>,
    callback_query: Option<CallbackQuery>,
}

#[derive(Debug, Deserialize)]
struct Message {
    message_id: i64,
    chat: Chat,
    from: Option<TelegramUser>,
    text: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Chat {
    id: i64,
    #[serde(rename = "type")]
    kind: String,
}

#[derive(Debug, Clone, Deserialize)]
struct TelegramUser {
    id: i64,
    username: Option<String>,
    first_name: String,
}

#[derive(Debug, Deserialize)]
struct CallbackQuery {
    id: String,
    from: TelegramUser,
    message: Option<Message>,
    data: Option<String>,
}

#[derive(Debug, Clone)]
enum PendingInput {
    CreateWallet { message_id: i64 },
    UnlockWallet { start_after: bool, message_id: i64 },
}

struct RunningAgent {
    stop: watch::Sender<bool>,
    task: JoinHandle<()>,
}

pub struct TelegramBot {
    client: reqwest::Client,
    bot_token: String,
    gemini_api_key: String,
    groq_api_key: Option<String>,
    x_cookie: String,
    offset: i64,
    records: HashMap<i64, UserRecord>,
    pending_inputs: HashMap<i64, PendingInput>,
    unlocked_wallets: HashMap<i64, Arc<Keypair>>,
    activities: HashMap<i64, Arc<std::sync::Mutex<Option<String>>>>,
    agents: HashMap<i64, RunningAgent>,
}

impl TelegramBot {
    pub async fn from_env() -> Result<Self> {
        let bot_token = required_env("TELEGRAM_BOT_API")?;
        let gemini_api_key = required_env("GEMINI_API_KEY")?;
        let x_cookie = required_env("X_COOKIE_STRING")?;
        let groq_api_key = std::env::var("GROQ_API_KEY")
            .ok()
            .filter(|value| !value.trim().is_empty());
        std::fs::create_dir_all(USER_STORE_DIR)
            .with_context(|| format!("failed to create Telegram user store {USER_STORE_DIR}"))?;
        set_private_permissions(Path::new(USER_STORE_DIR), true)?;

        let bot = Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(35))
                .build()?,
            bot_token,
            gemini_api_key,
            groq_api_key,
            x_cookie,
            offset: 0,
            records: HashMap::new(),
            pending_inputs: HashMap::new(),
            unlocked_wallets: HashMap::new(),
            activities: HashMap::new(),
            agents: HashMap::new(),
        };
        bot.call("deleteWebhook", json!({"drop_pending_updates": false}))
            .await
            .context("failed to enable Telegram long polling")?;
        bot.call("getMe", json!({}))
            .await
            .context("Telegram bot token validation failed")?;
        Ok(bot)
    }

    pub async fn run(mut self) -> Result<()> {
        println!("Telegram control bot is polling for updates.");
        loop {
            let updates: TelegramEnvelope<Vec<Update>> = match self.get_updates().await {
                Ok(updates) => updates,
                Err(error) => {
                    eprintln!("Telegram getUpdates failed: {error:#}; retrying in 3 seconds.");
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    continue;
                }
            };
            if !updates.ok {
                eprintln!("Telegram returned an unsuccessful getUpdates response.");
                tokio::time::sleep(Duration::from_secs(3)).await;
                continue;
            }
            for update in updates.result {
                self.offset = update.update_id.saturating_add(1);
                if let Err(error) = self.handle_update(update).await {
                    eprintln!("Telegram update handling failed: {error:#}");
                }
            }
        }
    }

    async fn get_updates(&self) -> Result<TelegramEnvelope<Vec<Update>>> {
        let response = self
            .client
            .get(self.api_url("getUpdates"))
            .query(&[
                ("offset", self.offset.to_string()),
                ("timeout", "25".to_string()),
                (
                    "allowed_updates",
                    r#"["message","callback_query"]"#.to_string(),
                ),
            ])
            .send()
            .await
            .map_err(|error| {
                anyhow!(
                    "Telegram getUpdates request failed: {}",
                    error.without_url()
                )
            })?;
        let status = response.status();
        let envelope = response
            .json::<TelegramEnvelope<Vec<Update>>>()
            .await
            .map_err(|error| {
                anyhow!(
                    "Telegram returned invalid getUpdates JSON: {}",
                    error.without_url()
                )
            })?;
        if !status.is_success() {
            anyhow::bail!("Telegram getUpdates returned HTTP {status}");
        }
        Ok(envelope)
    }

    async fn handle_update(&mut self, update: Update) -> Result<()> {
        if let Some(callback) = update.callback_query {
            return self.handle_callback(callback).await;
        }
        if let Some(message) = update.message {
            return self.handle_message(message).await;
        }
        Ok(())
    }

    async fn handle_message(&mut self, mut message: Message) -> Result<()> {
        if message.chat.kind != "private" {
            return self
                .send_text(
                    message.chat.id,
                    "For wallet security, use this bot in a private chat.",
                )
                .await;
        }
        let Some(user) = message.from.clone() else {
            return Ok(());
        };
        let Some(mut text) = message.text.take() else {
            return Ok(());
        };

        if let Some(pending) = self.pending_inputs.remove(&user.id) {
            let message_id = match pending {
                PendingInput::CreateWallet { message_id }
                | PendingInput::UnlockWallet { message_id, .. } => message_id,
            };
            if let Err(error) = self
                .delete_message(message.chat.id, message.message_id)
                .await
            {
                eprintln!("Could not delete wallet passphrase message: {error:#}");
            }
            let result = match pending {
                PendingInput::CreateWallet { .. } => {
                    self.create_wallet(user.clone(), text.as_str()).await
                }
                PendingInput::UnlockWallet { start_after, .. } => {
                    self.unlock_wallet(user.clone(), text.as_str(), start_after)
                        .await
                }
            };
            text.zeroize();
            match result {
                Ok(notice) => {
                    self.edit_dashboard(message.chat.id, message_id, &notice)
                        .await?;
                }
                Err(error) => {
                    self.edit_text(
                        message.chat.id,
                        message_id,
                        &format!(
                            "Could not complete wallet setup: {error:#}\n\n{}",
                            self.dashboard_text(user.id)
                        ),
                        Some(self.dashboard_keyboard(user.id)?),
                    )
                    .await?;
                }
            }
            return Ok(());
        }

        if text == "/start" || text == "/menu" {
            self.record_for(&user)?;
            return self
                .send_menu(
                    message.chat.id,
                    &self.dashboard_text(user.id),
                    self.dashboard_keyboard(user.id)?,
                )
                .await;
        }
        if text == "/help" {
            return self.send_menu(
                message.chat.id,
                "This private control panel manages your Devnet AI agent. Create a wallet here; its signing key is encrypted on this server with your passphrase. Telegram chats are not end-to-end encrypted. Never reuse a password.",
                self.dashboard_keyboard(user.id)?,
            ).await;
        }
        self.ensure_user(message.chat.id, &user).await
    }

    async fn handle_callback(&mut self, callback: CallbackQuery) -> Result<()> {
        let Some(message) = callback.message else {
            return self
                .answer_callback(&callback.id, "Open the bot in a private chat first.")
                .await;
        };
        if message.chat.kind != "private" || callback.from.id != message.chat.id {
            self.answer_callback(&callback.id, "Private chats only.")
                .await?;
            return Ok(());
        }
        let data = callback.data.as_deref().unwrap_or_default();
        self.answer_callback(&callback.id, "").await?;
        if data == "dashboard" {
            self.pending_inputs.remove(&callback.from.id);
        }
        self.record_for(&callback.from)?;

        match data {
            "dashboard" => {}
            "wallet_create" => {
                let record = self.record_for(&callback.from)?;
                if record.wallet.is_some() {
                    return self
                        .edit_dashboard(
                            message.chat.id,
                            message.message_id,
                            "A wallet is already linked to this Telegram account. Use Unlock to activate it.",
                        )
                        .await;
                }
                self.pending_inputs.insert(
                    callback.from.id,
                    PendingInput::CreateWallet {
                        message_id: message.message_id,
                    },
                );
                return self
                    .edit_text(
                        message.chat.id,
                        message.message_id,
                        "Create your Devnet wallet\n\nSend a unique passphrase with at least 12 characters. The message will be deleted after processing, but Telegram is not end-to-end encrypted. The key is encrypted on disk; losing the passphrase means losing access to this wallet.",
                        Some(json!({"inline_keyboard":[[{"text":"Cancel","callback_data":"dashboard"}]]})),
                    )
                    .await;
            }
            "wallet_unlock" | "agent_start" => {
                let is_running = self
                    .agents
                    .get(&callback.from.id)
                    .map_or(false, |agent| !agent.task.is_finished());
                if !is_running {
                    if let Some(wallet) = self.unlocked_wallets.get(&callback.from.id).cloned() {
                        self.start_agent(callback.from.clone(), wallet).await?;
                    } else {
                        let record = self.record_for(&callback.from)?;
                        if record.wallet.is_none() {
                            return self
                                .edit_dashboard(
                                    message.chat.id,
                                    message.message_id,
                                    "Create a wallet before starting the agent.",
                                )
                                .await;
                        }
                        self.pending_inputs.insert(
                            callback.from.id,
                            PendingInput::UnlockWallet {
                                start_after: true,
                                message_id: message.message_id,
                            },
                        );
                        return self
                            .edit_text(
                                message.chat.id,
                                message.message_id,
                                "Send your wallet passphrase to unlock and start the agent. The message will be deleted after processing.",
                                Some(json!({"inline_keyboard":[[{"text":"Cancel","callback_data":"dashboard"}]]})),
                            )
                            .await;
                    }
                }
            }
            "agent_stop" => {
                self.stop_agent(callback.from.id).await?;
            }
            "ai_activity" => {
                let active = self
                    .agents
                    .get(&callback.from.id)
                    .map_or(false, |agent| !agent.task.is_finished());
                let latest = self
                    .activities
                    .get(&callback.from.id)
                    .map(|activity| {
                        activity
                            .lock()
                            .map_err(|_| anyhow!("AI activity state is poisoned"))
                            .map(|latest| latest.clone())
                    })
                    .transpose()?
                    .flatten();
                let activity = latest.unwrap_or_else(|| {
                    "No AI decision or trade update has been sent yet.".to_string()
                });
                let text = format!(
                    "AI activity: {}\n\nLatest update:\n{}",
                    if active { "Running" } else { "Stopped" },
                    truncate_activity(&activity)
                );
                return self
                    .edit_text(
                        message.chat.id,
                        message.message_id,
                        &text,
                        Some(self.dashboard_keyboard(callback.from.id)?),
                    )
                    .await;
            }
            "wallet_balance" => {
                if let Some(wallet) = self.unlocked_wallets.get(&callback.from.id).cloned() {
                    let balance = PumpFunClient::devnet()
                        .check_balance(&wallet.pubkey())
                        .await?;
                    return self
                        .edit_dashboard(
                            message.chat.id,
                            message.message_id,
                            &format!(
                                "Wallet balance: {balance:.6} Devnet SOL\n\n{}",
                                self.dashboard_text(callback.from.id)
                            ),
                        )
                        .await;
                }
                let record = self.record_for(&callback.from)?;
                if record.wallet.is_none() {
                    return self
                        .edit_dashboard(
                            message.chat.id,
                            message.message_id,
                            "Create a wallet first.",
                        )
                        .await;
                }
                self.pending_inputs.insert(
                    callback.from.id,
                    PendingInput::UnlockWallet {
                        start_after: false,
                        message_id: message.message_id,
                    },
                );
                return self
                    .edit_text(
                        message.chat.id,
                        message.message_id,
                        "Send your passphrase to unlock the wallet and check its balance.",
                        Some(json!({"inline_keyboard":[[{"text":"Cancel","callback_data":"dashboard"}]]})),
                    )
                    .await;
            }
            "mode_autopilot" | "mode_interval" => {
                let record = self.record_for_mut(&callback.from)?;
                record.mode = if data == "mode_autopilot" {
                    UserMode::AutoPilot
                } else {
                    UserMode::Interval
                };
                self.save_record(callback.from.id)?;
                self.restart_if_running(callback.from.clone()).await?;
            }
            "interval_menu" => {
                return self
                    .edit_text(
                        message.chat.id,
                        message.message_id,
                        "Choose how often the AI evaluates launch decisions in Interval mode. It does not guarantee a launch on every tick.",
                        Some(interval_keyboard()),
                    )
                    .await;
            }
            "buy_menu" => {
                return self
                    .edit_text(
                        message.chat.id,
                        message.message_id,
                        "Set the maximum SOL amount the AI may request per Devnet buy. The agent also keeps at least 0.02 SOL reserved.",
                        Some(buy_keyboard()),
                    )
                    .await;
            }
            "hold_menu" => {
                return self
                    .edit_text(
                        message.chat.id,
                        message.message_id,
                        "Set the maximum hold time. The AI may choose a shorter sell delay based on its assessment.",
                        Some(hold_keyboard()),
                    )
                    .await;
            }
            "interval_300" | "interval_900" | "interval_1800" | "interval_3600"
            | "interval_21600" => {
                let seconds = data.trim_start_matches("interval_").parse::<u64>()?;
                self.record_for_mut(&callback.from)?.interval_secs = seconds.max(MIN_INTERVAL_SECS);
                self.save_record(callback.from.id)?;
                self.restart_if_running(callback.from.clone()).await?;
            }
            "buy_005" | "buy_01" | "buy_02" | "buy_03" => {
                let amount = match data {
                    "buy_005" => 0.005,
                    "buy_01" => 0.01,
                    "buy_02" => 0.02,
                    _ => 0.03,
                };
                self.record_for_mut(&callback.from)?.max_buy_sol = amount;
                self.save_record(callback.from.id)?;
                self.restart_if_running(callback.from.clone()).await?;
            }
            "hold_300" | "hold_900" | "hold_3600" | "hold_21600" | "hold_86400" => {
                let seconds = data.trim_start_matches("hold_").parse::<u64>()?;
                self.record_for_mut(&callback.from)?.max_hold_secs = seconds.min(MAX_HOLD_SECS);
                self.save_record(callback.from.id)?;
                self.restart_if_running(callback.from.clone()).await?;
            }
            _ => {}
        }
        self.edit_dashboard(message.chat.id, message.message_id, "")
            .await
    }

    async fn create_wallet(&mut self, user: TelegramUser, passphrase: &str) -> Result<String> {
        if passphrase.chars().count() < 12 {
            anyhow::bail!("passphrase must contain at least 12 characters");
        }
        let mut record = self.record_for(&user)?.clone();
        if record.wallet.is_some() {
            anyhow::bail!("a wallet is already linked to this Telegram account");
        }
        let wallet = Keypair::new();
        record.wallet = Some(encrypt_wallet(&wallet, passphrase)?);
        record.wallet_address = Some(wallet.pubkey().to_string());
        write_user_record(&record)?;
        self.records.insert(user.id, record);
        let address = wallet.pubkey().to_string();
        self.unlocked_wallets.insert(user.id, Arc::new(wallet));
        Ok(format!(
            "Wallet created and linked to your Telegram account.\nAddress: {address}\nNetwork: Devnet. Fund it with Devnet SOL before starting."
        ))
    }

    async fn unlock_wallet(
        &mut self,
        user: TelegramUser,
        passphrase: &str,
        start_after: bool,
    ) -> Result<String> {
        let record = self.record_for(&user)?.clone();
        let encrypted = record
            .wallet
            .as_ref()
            .ok_or_else(|| anyhow!("no wallet is linked to this Telegram account"))?;
        let wallet = Arc::new(decrypt_wallet(encrypted, passphrase)?);
        self.unlocked_wallets.insert(user.id, wallet.clone());
        if start_after {
            self.start_agent(user.clone(), wallet).await?;
            Ok(format!(
                "Wallet unlocked and AI agent started in {} mode on Devnet.",
                mode_name(&record.mode)
            ))
        } else {
            let balance = PumpFunClient::devnet()
                .check_balance(&wallet.pubkey())
                .await?;
            Ok(format!(
                "Wallet unlocked.\nAddress: {}\nBalance: {balance:.6} Devnet SOL",
                wallet.pubkey()
            ))
        }
    }

    async fn start_agent(&mut self, user: TelegramUser, wallet: Arc<Keypair>) -> Result<()> {
        if let Some(agent) = self.agents.get(&user.id) {
            if !agent.task.is_finished() {
                return Ok(());
            }
            self.agents.remove(&user.id);
        }
        let record = self.record_for(&user)?.clone();
        let activity = self
            .activities
            .entry(user.id)
            .or_insert_with(|| Arc::new(std::sync::Mutex::new(None)))
            .clone();
        let telegram = TelegramNotifier::for_chat_with_activity(
            &self.bot_token,
            user.id.to_string(),
            activity,
        )?;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let gemini_api_key = self.gemini_api_key.clone();
        let groq_api_key = self.groq_api_key.clone();
        let x_cookie = self.x_cookie.clone();
        let user_id = user.id;
        let task = tokio::task::spawn_local(async move {
            let result = async {
                let mut xplore = xplore::Xplore::new(None)
                    .await
                    .map_err(|error| anyhow!("failed to initialize Xplore: {error}"))?;
                xplore.set_cookie(&x_cookie).await;
                let schedule = match record.mode {
                    UserMode::AutoPilot => AgentSchedule::AutoPilot,
                    UserMode::Interval => AgentSchedule::Interval,
                };
                run_agent_with_wallet(
                    wallet.as_ref(),
                    &gemini_api_key,
                    groq_api_key.as_deref(),
                    &mut xplore,
                    "1",
                    Some(&telegram),
                    schedule,
                    Some(
                        record
                            .interval_secs
                            .clamp(MIN_INTERVAL_SECS, MAX_INTERVAL_SECS),
                    ),
                    AgentSettings {
                        max_buy_sol: record.max_buy_sol.clamp(MIN_BUY_SOL, MAX_BUY_SOL),
                        max_hold_secs: record.max_hold_secs.clamp(MIN_HOLD_SECS, MAX_HOLD_SECS),
                    },
                    Some(format!("telegram_{user_id}")),
                    shutdown_rx,
                    false,
                )
                .await
            }
            .await;
            if let Err(error) = result {
                eprintln!("Telegram user's AI agent failed: {error:#}");
            }
        });
        self.agents.insert(
            user.id,
            RunningAgent {
                stop: shutdown_tx,
                task,
            },
        );
        Ok(())
    }

    async fn stop_agent(&mut self, user_id: i64) -> Result<()> {
        if let Some(agent) = self.agents.remove(&user_id) {
            let _ = agent.stop.send(true);
            let mut task = agent.task;
            match tokio::time::timeout(Duration::from_secs(10), &mut task).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    eprintln!("Telegram agent {user_id} stopped with an error: {error}");
                }
                Err(_) => {
                    eprintln!(
                        "Telegram agent {user_id} did not stop within 10 seconds; aborting it."
                    );
                    task.abort();
                    let _ = task.await;
                }
            }
        }
        Ok(())
    }

    async fn restart_if_running(&mut self, user: TelegramUser) -> Result<()> {
        if self
            .agents
            .get(&user.id)
            .map_or(false, |agent| !agent.task.is_finished())
        {
            self.stop_agent(user.id).await?;
            if let Some(wallet) = self.unlocked_wallets.get(&user.id).cloned() {
                self.start_agent(user, wallet).await?;
            }
        }
        Ok(())
    }

    async fn ensure_user(&mut self, chat_id: i64, user: &TelegramUser) -> Result<()> {
        self.send_menu(
            chat_id,
            &format!(
                "Welcome, {}. Use /start to open your AI agent dashboard.",
                user.first_name
            ),
            self.dashboard_keyboard(user.id)?,
        )
        .await
    }

    fn record_for(&mut self, user: &TelegramUser) -> Result<&UserRecord> {
        if !self.records.contains_key(&user.id) {
            let record = match read_user_record(user.id)? {
                Some(record) => record,
                None => UserRecord::new(user.id, user.username.clone()),
            };
            self.records.insert(user.id, record);
        }
        if let Some(username) = user.username.as_ref() {
            self.records.get_mut(&user.id).unwrap().username = Some(username.clone());
        }
        Ok(self.records.get(&user.id).unwrap())
    }

    fn record_for_mut(&mut self, user: &TelegramUser) -> Result<&mut UserRecord> {
        self.record_for(user)?;
        Ok(self.records.get_mut(&user.id).unwrap())
    }

    fn save_record(&self, user_id: i64) -> Result<()> {
        let record = self
            .records
            .get(&user_id)
            .ok_or_else(|| anyhow!("Telegram user record is not loaded"))?;
        write_user_record(record)
    }

    fn dashboard_text(&self, user_id: i64) -> String {
        let record = self.records.get(&user_id);
        let active = self
            .agents
            .get(&user_id)
            .map_or(false, |agent| !agent.task.is_finished());
        match record {
            Some(record) => format!(
                "🤖 Narrix AI Agent\n\nStatus: {}\nMode: {}\nNetwork: Devnet\nInterval: {}\nMax buy: {:.3} SOL\nMax hold: {}\nReserve: {:.3} SOL\nWallet: {}",
                if active { "Running" } else { "Stopped" },
                mode_name(&record.mode),
                format_duration(record.interval_secs),
                record.max_buy_sol,
                format_duration(record.max_hold_secs),
                RESERVED_SOL,
                record
                    .wallet_address
                    .as_deref()
                    .unwrap_or_else(|| if record.wallet.is_some() { "created" } else { "not created" }),
            ),
            None => "🤖 Narrix AI Agent".to_string(),
        }
    }

    fn dashboard_keyboard(&self, user_id: i64) -> Result<Value> {
        let record = self.records.get(&user_id);
        let is_running = self
            .agents
            .get(&user_id)
            .map_or(false, |agent| !agent.task.is_finished());
        let has_wallet = record.and_then(|record| record.wallet.as_ref()).is_some();
        let wallet_button = if has_wallet {
            json!({"text":"💰 Balance / Wallet","callback_data":"wallet_balance"})
        } else {
            json!({"text":"➕ Create Devnet wallet","callback_data":"wallet_create"})
        };
        let agent_button = if is_running {
            json!({"text":"⏹ Stop agent","callback_data":"agent_stop"})
        } else {
            json!({"text":"▶ Start agent","callback_data":"agent_start"})
        };
        Ok(json!({"inline_keyboard":[
            [agent_button, {"text":"🔄 Refresh","callback_data":"dashboard"}],
            [wallet_button],
            [{"text":"🧭 AI autopilot","callback_data":"mode_autopilot"},{"text":"⏱ Interval mode","callback_data":"mode_interval"}],
            [{"text":"🧠 What is the AI doing?","callback_data":"ai_activity"}],
            [{"text":"Set interval","callback_data":"interval_menu"},{"text":"Max buy","callback_data":"buy_menu"},{"text":"Max hold","callback_data":"hold_menu"}]
        ]}))
    }

    async fn edit_dashboard(&self, chat_id: i64, message_id: i64, prefix: &str) -> Result<()> {
        let mut text = String::new();
        if !prefix.is_empty() {
            text.push_str(prefix);
            text.push_str("\n\n");
        }
        text.push_str(&self.dashboard_text(chat_id));
        text.push_str("\n\nInterval mode means the AI evaluates at the selected cadence; Autopilot lets it evaluate every 30 seconds and respond to new ingress.");
        self.edit_text(
            chat_id,
            message_id,
            &text,
            Some(self.dashboard_keyboard(chat_id)?),
        )
        .await
    }

    async fn send_menu(&self, chat_id: i64, text: &str, keyboard: Value) -> Result<()> {
        self.call(
            "sendMessage",
            json!({
                "chat_id": chat_id,
                "text": text,
                "reply_markup": keyboard
            }),
        )
        .await?;
        Ok(())
    }

    async fn send_text(&self, chat_id: i64, text: &str) -> Result<()> {
        self.call(
            "sendMessage",
            json!({
                "chat_id": chat_id,
                "text": text
            }),
        )
        .await?;
        Ok(())
    }

    async fn edit_text(
        &self,
        chat_id: i64,
        message_id: i64,
        text: &str,
        keyboard: Option<Value>,
    ) -> Result<()> {
        let mut payload = json!({
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text
        });
        if let Some(keyboard) = keyboard {
            payload["reply_markup"] = keyboard;
        }
        self.call("editMessageText", payload).await?;
        Ok(())
    }

    async fn answer_callback(&self, callback_id: &str, text: &str) -> Result<()> {
        self.call(
            "answerCallbackQuery",
            json!({"callback_query_id": callback_id, "text": text}),
        )
        .await?;
        Ok(())
    }

    async fn delete_message(&self, chat_id: i64, message_id: i64) -> Result<()> {
        self.call(
            "deleteMessage",
            json!({"chat_id": chat_id, "message_id": message_id}),
        )
        .await?;
        Ok(())
    }

    async fn call(&self, method: &str, payload: Value) -> Result<Value> {
        let response = self
            .client
            .post(self.api_url(method))
            .json(&payload)
            .send()
            .await
            .map_err(|error| {
                anyhow!("Telegram {method} request failed: {}", error.without_url())
            })?;
        let status = response.status();
        let value = response.json::<Value>().await.map_err(|error| {
            anyhow!(
                "Telegram {method} response was invalid JSON: {}",
                error.without_url()
            )
        })?;
        if !status.is_success() || value.get("ok").and_then(Value::as_bool) != Some(true) {
            anyhow::bail!(
                "Telegram {method} failed (HTTP {status}): {}",
                value
                    .get("description")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown error")
            );
        }
        Ok(value)
    }

    fn api_url(&self, method: &str) -> String {
        format!("https://api.telegram.org/bot{}/{method}", self.bot_token)
    }
}

fn interval_keyboard() -> Value {
    json!({"inline_keyboard":[
        [{"text":"5 min","callback_data":"interval_300"},{"text":"15 min","callback_data":"interval_900"}],
        [{"text":"30 min","callback_data":"interval_1800"},{"text":"1 hour","callback_data":"interval_3600"}],
        [{"text":"6 hours","callback_data":"interval_21600"},{"text":"Back","callback_data":"dashboard"}]
    ]})
}

fn buy_keyboard() -> Value {
    json!({"inline_keyboard":[
        [{"text":"0.005 SOL","callback_data":"buy_005"},{"text":"0.01 SOL","callback_data":"buy_01"}],
        [{"text":"0.02 SOL","callback_data":"buy_02"},{"text":"0.03 SOL","callback_data":"buy_03"}],
        [{"text":"Back","callback_data":"dashboard"}]
    ]})
}

fn hold_keyboard() -> Value {
    json!({"inline_keyboard":[
        [{"text":"5 min","callback_data":"hold_300"},{"text":"15 min","callback_data":"hold_900"}],
        [{"text":"1 hour","callback_data":"hold_3600"},{"text":"6 hours","callback_data":"hold_21600"}],
        [{"text":"24 hours","callback_data":"hold_86400"},{"text":"Back","callback_data":"dashboard"}]
    ]})
}

fn mode_name(mode: &UserMode) -> &'static str {
    match mode {
        UserMode::AutoPilot => "AI Autopilot",
        UserMode::Interval => "Scheduled interval",
    }
}

fn format_duration(seconds: u64) -> String {
    if seconds >= 3600 && seconds % 3600 == 0 {
        format!("{} hour(s)", seconds / 3600)
    } else if seconds >= 60 && seconds % 60 == 0 {
        format!("{} min", seconds / 60)
    } else {
        format!("{seconds} sec")
    }
}

fn required_env(name: &str) -> Result<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| anyhow!("{name} must be set to run Telegram bot mode"))
}

fn wallet_path(user_id: i64) -> PathBuf {
    Path::new(USER_STORE_DIR).join(format!("{user_id}.json"))
}

fn read_user_record(user_id: i64) -> Result<Option<UserRecord>> {
    let path = wallet_path(user_id);
    match std::fs::read(&path) {
        Ok(bytes) => {
            let record: UserRecord = serde_json::from_slice(&bytes)
                .with_context(|| format!("invalid Telegram user data in {}", path.display()))?;
            if record.telegram_id != user_id {
                anyhow::bail!("Telegram user ID does not match profile file {}", path.display());
            }
            Ok(Some(record))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).with_context(|| format!("failed to read {}", path.display())),
    }
}

fn write_user_record(record: &UserRecord) -> Result<()> {
    let path = wallet_path(record.telegram_id);
    let temporary_path = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec(record)?;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(&temporary_path)
        .with_context(|| format!("failed to create {}", temporary_path.display()))?;
    use std::io::Write;
    file.write_all(&bytes)?;
    file.sync_all()?;
    set_private_permissions(&temporary_path, false)?;
    std::fs::rename(&temporary_path, &path)
        .with_context(|| format!("failed to save Telegram user data to {}", path.display()))
}

fn set_private_permissions(path: &Path, directory: bool) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = if directory { 0o700 } else { 0o600 };
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
            .with_context(|| format!("failed to restrict permissions on {}", path.display()))?;
    }
    Ok(())
}

fn encrypt_wallet(wallet: &Keypair, passphrase: &str) -> Result<EncryptedWallet> {
    let rng = SystemRandom::new();
    let mut salt = [0u8; 16];
    let mut nonce_bytes = [0u8; 12];
    rng.fill(&mut salt)
        .map_err(|_| anyhow!("secure random number generation failed"))?;
    rng.fill(&mut nonce_bytes)
        .map_err(|_| anyhow!("secure random number generation failed"))?;
    let mut key = [0u8; 32];
    pbkdf2::derive(
        pbkdf2::PBKDF2_HMAC_SHA256,
        NonZeroU32::new(KDF_ITERATIONS).unwrap(),
        &salt,
        passphrase.as_bytes(),
        &mut key,
    );
    let cipher = LessSafeKey::new(
        UnboundKey::new(&AES_256_GCM, &key)
            .map_err(|_| anyhow!("wallet encryption setup failed"))?,
    );
    key.zeroize();
    let mut private_key = wallet.to_bytes();
    let mut ciphertext = private_key.to_vec();
    private_key.zeroize();
    cipher
        .seal_in_place_append_tag(
            Nonce::assume_unique_for_key(nonce_bytes),
            Aad::empty(),
            &mut ciphertext,
        )
        .map_err(|_| anyhow!("wallet encryption failed"))?;
    Ok(EncryptedWallet {
        salt: STANDARD.encode(salt),
        nonce: STANDARD.encode(nonce_bytes),
        ciphertext: STANDARD.encode(ciphertext),
    })
}

fn decrypt_wallet(encrypted: &EncryptedWallet, passphrase: &str) -> Result<Keypair> {
    let salt = STANDARD
        .decode(&encrypted.salt)
        .context("wallet salt is invalid")?;
    let nonce_bytes = STANDARD
        .decode(&encrypted.nonce)
        .context("wallet nonce is invalid")?;
    if salt.len() != 16 || nonce_bytes.len() != 12 {
        anyhow::bail!("wallet encryption metadata is invalid");
    }
    let mut nonce = [0u8; 12];
    nonce.copy_from_slice(&nonce_bytes);
    let mut key = [0u8; 32];
    pbkdf2::derive(
        pbkdf2::PBKDF2_HMAC_SHA256,
        NonZeroU32::new(KDF_ITERATIONS).unwrap(),
        &salt,
        passphrase.as_bytes(),
        &mut key,
    );
    let cipher = LessSafeKey::new(
        UnboundKey::new(&AES_256_GCM, &key)
            .map_err(|_| anyhow!("wallet decryption setup failed"))?,
    );
    key.zeroize();
    let mut ciphertext = STANDARD
        .decode(&encrypted.ciphertext)
        .context("wallet ciphertext is invalid")?;
    let plaintext = cipher
        .open_in_place(
            Nonce::assume_unique_for_key(nonce),
            Aad::empty(),
            &mut ciphertext,
        )
        .map_err(|_| anyhow!("incorrect passphrase or damaged encrypted wallet"))?;
    if plaintext.len() != 64 {
        ciphertext.zeroize();
        anyhow::bail!("decrypted wallet has an invalid key length");
    }
    let wallet =
        Keypair::from_bytes(plaintext).map_err(|_| anyhow!("decrypted wallet key is invalid"));
    ciphertext.zeroize();
    wallet
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wallet_encryption_round_trips_and_rejects_wrong_passphrase() {
        let wallet = Keypair::new();
        let encrypted = encrypt_wallet(&wallet, "a-long-test-passphrase").unwrap();
        let decrypted = decrypt_wallet(&encrypted, "a-long-test-passphrase").unwrap();
        assert_eq!(wallet.pubkey(), decrypted.pubkey());
        assert!(decrypt_wallet(&encrypted, "wrong passphrase").is_err());
    }
}
