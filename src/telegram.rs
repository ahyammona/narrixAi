use anyhow::{anyhow, Result};
use serde::Deserialize;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct TelegramNotifier {
    client: reqwest::Client,
    bot_token: String,
    chat_id: String,
    activity: Option<Arc<Mutex<Option<String>>>>,
}

#[derive(Deserialize)]
struct TelegramResponse {
    ok: bool,
}

impl TelegramNotifier {
    pub fn from_env() -> Result<Option<Self>> {
        let bot_token = std::env::var("TELEGRAM_BOT_API")
            .ok()
            .filter(|value| !value.trim().is_empty());
        let chat_id = std::env::var("CHAT_BOT_API")
            .ok()
            .filter(|value| !value.trim().is_empty());

        match (bot_token, chat_id) {
            (None, None) => Ok(None),
            (Some(_), None) | (None, Some(_)) => {
                anyhow::bail!("TELEGRAM_BOT_API and CHAT_BOT_API must both be set")
            }
            (Some(bot_token), Some(chat_id)) => Ok(Some(Self {
                client: reqwest::Client::builder()
                    .timeout(std::time::Duration::from_secs(10))
                    .build()?,
                bot_token,
                chat_id,
                activity: None,
            })),
        }
    }

    pub fn for_chat(bot_token: &str, chat_id: impl Into<String>) -> Result<Self> {
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()?,
            bot_token: bot_token.to_string(),
            chat_id: chat_id.into(),
            activity: None,
        })
    }

    pub fn for_chat_with_activity(
        bot_token: &str,
        chat_id: impl Into<String>,
        activity: Arc<Mutex<Option<String>>>,
    ) -> Result<Self> {
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()?,
            bot_token: bot_token.to_string(),
            chat_id: chat_id.into(),
            activity: Some(activity),
        })
    }

    pub async fn send_message(&self, text: &str) -> Result<()> {
        let url = format!("https://api.telegram.org/bot{}/sendMessage", self.bot_token);
        let response = self
            .client
            .post(url)
            .json(&serde_json::json!({
                "chat_id": self.chat_id,
                "text": text,
            }))
            .send()
            .await
            .map_err(|error| {
                anyhow!(
                    "Telegram sendMessage request failed: {}",
                    error.without_url()
                )
            })?;

        let status = response.status();
        let result: TelegramResponse = response
            .json()
            .await
            .map_err(|_| anyhow!("Telegram returned an unreadable sendMessage response"))?;
        if !status.is_success() || !result.ok {
            anyhow::bail!("Telegram rejected sendMessage (HTTP {status})");
        }
        if let Some(activity) = &self.activity {
            *activity
                .lock()
                .map_err(|_| anyhow!("Telegram activity state is poisoned"))? =
                Some(text.to_string());
        }
        Ok(())
    }
}
