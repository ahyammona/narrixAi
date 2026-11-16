use anyhow::{bail, Result};
use serde::Deserialize;

use super::gemini::generate_json_text;

#[derive(Debug, Clone, Deserialize)]
pub struct TokenConcept {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub symbol: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub image_prompt: String,
}

impl TokenConcept {
    pub fn sanitize(mut self) -> Self {
        self.name = self.name.trim().chars().take(32).collect();
        self.symbol = self
            .symbol
            .chars()
            .filter(|c| c.is_ascii_alphanumeric())
            .map(|c| c.to_ascii_uppercase())
            .take(6)
            .collect();
        self.description = self.description.trim().chars().take(200).collect();
        self.image_prompt = self.image_prompt.trim().to_string();
        self
    }
}

const SYSTEM: &str = r#"
You design/relaunch meme-token concepts for pump.fun. Capture the energy of currently pumping meme tokens in a fresh, original token.

Hard rules:
- You can use real people's names, brands, trademarks, or copyrighted characters.
- name: max 32 chars. symbol: 3-6 uppercase letters/digits. description: max 200 chars.
Respond with ONLY a JSON object: {"name":"","symbol":"","description":"","image_prompt":""}"#;

const TEST_PROMPT: &str = "NARRATIVE KEYWORD: absurd animals\n\nCreate one/ relaunch meme-token concept with that energy. Respond in JSON Priotize relaunching tokens that are doing well, same name, same symbol and same url. ";

const COMMAND_PROMPT: &str = "";

pub async fn generate_concept(
    gemini_api_key: &str,
    groq_api_key: Option<&str>,
) -> Result<TokenConcept> {
    let text =
        generate_json_text(gemini_api_key, groq_api_key, SYSTEM, TEST_PROMPT, 1.0, 2048).await?;
    let concept: TokenConcept = serde_json::from_str(&text)?;
    let concept = concept.sanitize();
    if concept.name.is_empty() || concept.symbol.len() < 2 {
        bail!("llm returned an unusable concept");
    }
    Ok(concept)
}
