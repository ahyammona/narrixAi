use anyhow::{anyhow, bail, Result};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicU64, Ordering};

const GEMINI_URL: &str =
    "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent";
const GROQ_URL: &str = "https://api.groq.com/openai/v1/chat/completions";
pub const DEFAULT_MODEL: &str = "gemini-3.6-flash";
const DEFAULT_GROQ_MODEL: &str = "openai/gpt-oss-20b";
const DEFAULT_GEMINI_RATE_LIMIT_COOLDOWN_SECS: u64 = 60;

static GEMINI_COOLDOWN_UNTIL_UNIX: AtomicU64 = AtomicU64::new(0);

pub async fn generate_json_text(
    gemini_api_key: &str,
    groq_api_key: Option<&str>,
    system_instruction: &str,
    prompt: &str,
    temperature: f64,
    max_output_tokens: u32,
) -> Result<String> {
    let remaining_cooldown = gemini_cooldown_remaining();
    let gemini_result = if let Some(remaining) = remaining_cooldown {
        Err(anyhow!(
            "Gemini rate-limit cooldown active for about {} seconds",
            remaining
        ))
    } else {
        generate_gemini_json(
            gemini_api_key,
            system_instruction,
            prompt,
            temperature,
            max_output_tokens,
        )
        .await
    };

    match gemini_result {
        Ok(text) => Ok(text),
        Err(gemini_error) => {
            let Some(groq_api_key) = groq_api_key else {
                return Err(gemini_error);
            };
            if remaining_cooldown.is_some() {
                eprintln!("Gemini rate-limit cooldown is active; using Groq fallback.");
            } else {
                eprintln!("Gemini request failed ({gemini_error}); trying Groq fallback.");
            }
            generate_groq_json(
                groq_api_key,
                system_instruction,
                prompt,
                temperature,
                max_output_tokens,
            )
            .await
            .map_err(|groq_error| {
                anyhow!("Gemini failed: {gemini_error}; Groq fallback failed: {groq_error}")
            })
        }
    }
}

async fn generate_gemini_json(
    api_key: &str,
    system_instruction: &str,
    prompt: &str,
    temperature: f64,
    max_output_tokens: u32,
) -> Result<String> {
    let model = std::env::var("AI_MODEL")
        .ok()
        .filter(|model| !model.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_MODEL.to_string());
    let url = GEMINI_URL.replace("{model}", &model);
    let body = json!({
        "systemInstruction": {
            "parts": [{ "text": system_instruction }]
        },
        "contents": [{
            "role": "user",
            "parts": [{ "text": prompt }]
        }],
        "generationConfig": {
            "temperature": temperature,
            "maxOutputTokens": max_output_tokens,
            "responseMimeType": "application/json"
        }
    });

    let response = reqwest::Client::new()
        .post(url)
        .header("x-goog-api-key", api_key)
        .json(&body)
        .send()
        .await?;
    let status = response.status();
    let value: Value = response.json().await?;
    if !status.is_success() {
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let retry_after =
                retry_after_seconds(&value).unwrap_or(DEFAULT_GEMINI_RATE_LIMIT_COOLDOWN_SECS);
            set_gemini_cooldown(retry_after);
            eprintln!("Gemini rate limit reached; bypassing it for about {retry_after} seconds.");
        }
        bail!("Gemini API returned {status}: {value}");
    }
    let text = response_text(&value)?;
    validate_json(&text)?;
    Ok(text)
}

async fn generate_groq_json(
    api_key: &str,
    system_instruction: &str,
    prompt: &str,
    temperature: f64,
    max_output_tokens: u32,
) -> Result<String> {
    let model = std::env::var("GROQ_MODEL")
        .ok()
        .filter(|model| !model.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_GROQ_MODEL.to_string());
    let body = json!({
        "model": model,
        "max_tokens": max_output_tokens,
        "temperature": temperature,
        "response_format": { "type": "json_object" },
        "messages": [
            { "role": "system", "content": system_instruction },
            { "role": "user", "content": prompt }
        ]
    });
    let response = reqwest::Client::new()
        .post(GROQ_URL)
        .bearer_auth(api_key)
        .json(&body)
        .send()
        .await?;
    let status = response.status();
    let value: Value = response.json().await?;
    if !status.is_success() {
        bail!("Groq API returned {status}: {value}");
    }
    let text = value["choices"][0]["message"]["content"]
        .as_str()
        .ok_or_else(|| anyhow!("Groq response did not contain candidate text: {value}"))?
        .to_string();
    validate_json(&text)?;
    Ok(text)
}

fn validate_json(text: &str) -> Result<()> {
    serde_json::from_str::<Value>(text)
        .map(|_| ())
        .map_err(|error| anyhow!("model returned invalid JSON: {error}; response: {text}"))
}

fn retry_after_seconds(value: &Value) -> Option<u64> {
    value["error"]["details"]
        .as_array()?
        .iter()
        .filter(|detail| {
            detail["@type"]
                .as_str()
                .map_or(false, |kind| kind.ends_with(".RetryInfo"))
        })
        .find_map(|detail| {
            let retry_delay = detail["retryDelay"].as_str()?;
            let seconds = retry_delay.strip_suffix('s')?.parse::<f64>().ok()?;
            (seconds.is_finite() && seconds > 0.0).then(|| seconds.ceil() as u64)
        })
}

fn set_gemini_cooldown(seconds: u64) {
    let now = unix_time_seconds();
    let deadline = now.saturating_add(seconds.max(1));
    GEMINI_COOLDOWN_UNTIL_UNIX.fetch_max(deadline, Ordering::Relaxed);
}

fn gemini_cooldown_remaining() -> Option<u64> {
    let remaining = GEMINI_COOLDOWN_UNTIL_UNIX
        .load(Ordering::Relaxed)
        .saturating_sub(unix_time_seconds());
    (remaining > 0).then_some(remaining)
}

fn unix_time_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub fn response_text(value: &Value) -> Result<String> {
    let candidate = &value["candidates"][0];
    if candidate["finishReason"].as_str() == Some("MAX_TOKENS") {
        bail!("Gemini response was truncated because it reached the output-token limit");
    }
    let parts = candidate["content"]["parts"]
        .as_array()
        .ok_or_else(|| anyhow!("Gemini response did not contain candidate text: {value}"))?;
    let text = parts
        .iter()
        .filter_map(|part| part["text"].as_str())
        .collect::<String>();
    if text.trim().is_empty() {
        bail!("Gemini response contained no text: {value}");
    }
    Ok(text)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_gemini_candidate_text() {
        let response = json!({
            "candidates": [{
                "content": {
                    "parts": [
                        { "text": "{\"action\":" },
                        { "text": "\"save_ingress\"}" }
                    ]
                }
            }]
        });
        assert_eq!(
            response_text(&response).unwrap(),
            "{\"action\":\"save_ingress\"}"
        );
    }

    #[test]
    fn reports_missing_gemini_candidate_text() {
        let response = json!({"candidates": []});
        assert!(response_text(&response).is_err());
    }

    #[test]
    fn reports_truncated_gemini_responses() {
        let response = json!({
            "candidates": [{
                "finishReason": "MAX_TOKENS",
                "content": { "parts": [{ "text": "{\"action\":" }] }
            }]
        });
        assert!(response_text(&response)
            .unwrap_err()
            .to_string()
            .contains("truncated"));
    }

    #[test]
    fn validates_json_before_accepting_provider_response() {
        assert!(validate_json(r#"{"action":"save_ingress"}"#).is_ok());
        assert!(validate_json(r#"{"action":"save_ingress""#).is_err());
    }

    #[test]
    fn reads_google_retry_info_for_quota_cooldown() {
        let response = json!({
            "error": {
                "details": [
                    {
                        "@type": "type.googleapis.com/google.rpc.RetryInfo",
                        "retryDelay": "74526.8s"
                    }
                ]
            }
        });
        assert_eq!(retry_after_seconds(&response), Some(74_527));
        assert_eq!(retry_after_seconds(&json!({"error": {}})), None);
    }
}
