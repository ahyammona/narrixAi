use anyhow::{anyhow, bail, Result};
use std::collections::VecDeque;

pub const MAX_TOKENS: usize = 25;

#[derive(Debug, Clone)]
pub struct TokenRecord {
    pub id: u64,
    pub source_mint: Option<String>,
    pub name: String,
    pub symbol: String,
    /// File path or URL of the token image. May be empty until an image exists.
    pub image: String,
    /// Public metadata JSON URI used when launching this token.
    pub metadata_uri: String,
}

#[derive(Debug, Default)]
pub struct TokenDb {
    next_id: u64,
    tokens: VecDeque<TokenRecord>, // front = oldest, back = newest
}

impl TokenDb {
    pub fn new() -> Self {
        Self::default()
    }

    // ---- Create (evicts the oldest when full; returns the new record and the evicted one) ----
    pub fn create(
        &mut self,
        name: &str,
        symbol: &str,
        image: &str,
        metadata_uri: &str,
        source_mint: Option<&str>,
    ) -> Result<(TokenRecord, Option<TokenRecord>)> {
        let name = name.trim();
        let symbol = symbol.trim().to_uppercase();
        if name.is_empty() || symbol.is_empty() {
            bail!("name and symbol are required");
        }

        let evicted = if self.tokens.len() >= MAX_TOKENS {
            self.tokens.pop_front()
        } else {
            None
        };

        self.next_id += 1;
        let record = TokenRecord {
            id: self.next_id,
            source_mint: source_mint
                .map(str::trim)
                .filter(|mint| !mint.is_empty())
                .map(str::to_string),
            name: name.to_string(),
            symbol,
            image: image.trim().to_string(),
            metadata_uri: metadata_uri.trim().to_string(),
        };
        self.tokens.push_back(record.clone());
        Ok((record, evicted))
    }

    // ---- Read ----
    pub fn get(&self, id: u64) -> Option<&TokenRecord> {
        self.tokens.iter().find(|t| t.id == id)
    }

    pub fn find_by_symbol(&self, symbol: &str) -> Option<&TokenRecord> {
        self.tokens
            .iter()
            .find(|t| t.symbol.eq_ignore_ascii_case(symbol.trim()))
    }

    pub fn find_by_mint(&self, mint: &str) -> Option<&TokenRecord> {
        self.tokens
            .iter()
            .find(|token| token.source_mint.as_deref() == Some(mint))
    }

    /// Oldest first.
    pub fn list(&self) -> Vec<&TokenRecord> {
        self.tokens.iter().collect()
    }

    pub fn len(&self) -> usize {
        self.tokens.len()
    }

    // ---- Delete ----
    pub fn delete(&mut self, id: u64) -> bool {
        let before = self.tokens.len();
        self.tokens.retain(|t| t.id != id);
        self.tokens.len() != before
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_metadata_uri_is_stored_separately_from_image() {
        let mut tokens = TokenDb::new();
        let (token, _) = tokens
            .create(
                "Example",
                "EX",
                "https://example.com/image.png",
                "https://example.com/metadata.json",
                Some("Mint111"),
            )
            .unwrap();

        assert_eq!(token.image, "https://example.com/image.png");
        assert_eq!(token.metadata_uri, "https://example.com/metadata.json");
        assert_eq!(token.source_mint.as_deref(), Some("Mint111"));
        assert_eq!(
            tokens.find_by_mint("Mint111").map(|token| token.id),
            Some(token.id)
        );
    }
}
