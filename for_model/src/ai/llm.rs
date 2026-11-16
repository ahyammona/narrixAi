use crate::{config::LlmCfg, types::*};
use anyhow::{anyhow, bail, Result};
use serde_json::{json, Value};

const SYSTEM: &str = r#"You design ORIGINAL meme-token concepts for pump.fun. You will be shown 1-5 WINNING tokens that are currently pumping on-chain, plus (optionally) what people are saying on X. Your job is to capture the SAME ENERGY as those winners in a fresh, original token.

Hard rules:
- Do NOT copy the exact name, symbol, or specific joke of any winning token shown. Do NOT rename them (e.g. "Dancing Cat" -> "Grooving Cat" is banned if that's the only change).
- Capture the MECHANIC and TONE: why is this token winning? playful animal? absurd scenario? ironic tech? If you can articulate why it's funny/compelling, execute that same energy in a new frame.
- Do NOT use real people's names, brands, trademarks, or copyrighted characters.
- Do NOT promise profit or returns.
- name: max 32 chars. symbol: 3-6 uppercase letters/digits. description: max 200 chars.
- image_prompt: simple, distinctive, original square mascot. No text in image, no real people, no logos.
Respond with ONLY a JSON object: {"name":"","symbol":"","description":"","image_prompt":""}"#;

pub async fn generate_concept(
    client: &reqwest::Client,
    api_key: &str,
    cfg: &LlmCfg,
    n: &Narrative,
    avoid: &[String],
    feedback: Option<&str>,
) -> Result<TokenConcept> {
    let seeds =
        if n.seed_tokens.is_empty() {
            "(no seed tokens — this narrative came from X trends only)".to_string()
        } else {
            n.seed_tokens
                .iter()
                .take(5)
                .map(|s| {
                    format!(
                    "- {} (${}) — mcap ${:.0}, {} replies, age {:.1}h, vol24h ${:.0}\n  desc: {}",
                    s.name, s.symbol, s.market_cap_usd, s.reply_count, s.age_hours, s.volume_24h,
                    s.description.chars().take(160).collect::<String>()
                )
                })
                .collect::<Vec<_>>()
                .join("\n")
        };

    let x_samples = if n.examples.is_empty() {
        "(no X samples)".to_string()
    } else {
        n.examples
            .iter()
            .map(|e| format!("- {e}"))
            .collect::<Vec<_>>()
            .join("\n")
    };

    let mut user = format!(
        "NARRATIVE KEYWORD: {}\n\nWINNING TOKENS (study why these are pumping):\n{}\n\nX SIGNALS:\n{}\n\nALREADY EXISTS (do NOT copy these names/symbols):\n{}\n",
        n.keyword,
        seeds,
        x_samples,
        if avoid.is_empty() { "(none found)".into() } else { avoid.join(", ") }
    );
    if let Some(f) = feedback {
        user.push_str(&format!(
            "\nYour previous attempt was rejected: {f}\nMake something clearly different.\n"
        ));
    }

    let body = json!({
        "model": cfg.model,
        "max_tokens": cfg.max_tokens,
        "temperature": 1.0,
        "response_format": { "type": "json_object" },
        "messages": [
            { "role": "system", "content": SYSTEM },
            { "role": "user", "content": user }
        ]
    });

    let resp = client
        .post(format!(
            "{}/chat/completions",
            cfg.base_url.trim_end_matches('/')
        ))
        .bearer_auth(api_key)
        .json(&body)
        .send()
        .await?;
    let status = resp.status();
    let v: Value = resp.json().await?;
    if !status.is_success() {
        bail!("deepseek api {}: {}", status, v);
    }
    let text = v["choices"][0]["message"]["content"]
        .as_str()
        .ok_or_else(|| anyhow!("no content in response: {v}"))?;
    let start = text
        .find('{')
        .ok_or_else(|| anyhow!("no json in response"))?;
    let end = text
        .rfind('}')
        .ok_or_else(|| anyhow!("no json in response"))?;
    let concept: TokenConcept = serde_json::from_str(&text[start..=end])?;
    let concept = concept.sanitize();
    if concept.name.is_empty() || concept.symbol.len() < 2 {
        bail!("llm returned an unusable concept");
    }
    Ok(concept)
}
