use crate::blockchain::models;
use anyhow::{anyhow, bail, Result};
use dotenv::dotenv;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;
use std::env;
use std::str::FromStr;

pub fn load_keypair_from_env(var_name: &str) -> Result<Keypair> {
    dotenv().ok();

    let value = env::var(var_name).map_err(|_| {
        anyhow!(
            "{var_name} is not set in the process environment or the .env file loaded from the current directory"
        )
    })?;
    parse_keypair_value(&value).map_err(|_| {
        anyhow!(
            "{var_name} is set but is not a valid supported keypair. Use a base58-encoded 64-byte key, a JSON array of 64 bytes, or a path to a Solana keypair JSON file. A shell-exported value takes precedence over .env; check for an older value there."
        )
    })
}

fn parse_keypair_value(value: &str) -> Result<Keypair> {
    let value = value.trim();
    if value.is_empty() {
        bail!("keypair value is empty");
    }

    if let Ok(bytes) = bs58::decode(value).into_vec() {
        if bytes.len() == 64 {
            return Ok(Keypair::from_bytes(&bytes)?);
        }
    }

    if let Ok(bytes) = serde_json::from_str::<Vec<u8>>(value) {
        if bytes.len() == 64 {
            return Ok(Keypair::from_bytes(&bytes)?);
        }
    }

    if let Ok(keypair) = read_keypair_from_file(value) {
        return Ok(keypair);
    }

    bail!("value does not match a supported keypair format")
}

pub fn load_rpc_url_from_env(var_name: &str) -> String {
    dotenv().ok();
    env::var(var_name).unwrap_or_else(|_| "https://api.devnet.solana.com".to_string())
}

pub fn load_rpc_url_with_network(var_name: &str, network: &models::Network) -> String {
    dotenv().ok();

    match env::var(var_name) {
        Ok(url) => url,
        Err(_) => match network {
            models::Network::Devnet => "https://api.devnet.solana.com".to_string(),
            models::Network::Mainnet => "https://api.mainnet-beta.solana.com".to_string(),
            models::Network::Localnet => "http://localhost:8899".to_string(),
        },
    }
}

fn read_keypair_from_file(path: &str) -> Result<Keypair, Box<dyn std::error::Error>> {
    let data = std::fs::read_to_string(path)?;
    let bytes: Vec<u8> = serde_json::from_str(&data)?;
    Ok(Keypair::from_bytes(&bytes)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::signature::Signer;

    #[test]
    fn parses_base58_and_json_keypairs() {
        let expected = Keypair::new();
        let bytes = expected.to_bytes();

        let from_base58 = parse_keypair_value(&bs58::encode(bytes).into_string()).unwrap();
        let from_json =
            parse_keypair_value(&serde_json::to_string(&bytes.to_vec()).unwrap()).unwrap();

        assert_eq!(from_base58.pubkey(), expected.pubkey());
        assert_eq!(from_json.pubkey(), expected.pubkey());
    }

    #[test]
    fn invalid_keypair_error_does_not_include_the_secret_value() {
        let secret = "not-a-valid-key";
        let error = parse_keypair_value(secret).unwrap_err();
        assert!(!error.to_string().contains(secret));
    }
}

pub fn parse_pubkey(s: &str) -> Result<Pubkey, String> {
    Pubkey::from_str(s).map_err(|e| e.to_string())
}

pub fn lamports_to_sol(lamports: u64) -> f64 {
    lamports as f64 / 1e9
}

pub fn sol_to_lamports(sol: f64) -> u64 {
    (sol * 1e9) as u64
}

pub fn wait_for_enter() {
    let mut input = String::new();
    std::io::stdin().read_line(&mut input).unwrap();
}

pub fn get_explorer_url(signature: &str, network: &models::Network) -> String {
    match network {
        crate::blockchain::models::Network::Devnet => format!(
            "https://explorer.solana.com/tx/{}?cluster=devnet",
            signature
        ),
        crate::blockchain::models::Network::Mainnet => {
            format!("https://explorer.solana.com/tx/{}", signature)
        }
        crate::blockchain::models::Network::Localnet => format!(
            "https://explorer.solana.com/tx/{}?cluster=custom",
            signature
        ),
    }
}

pub fn format_number(num: f64) -> String {
    let mut parts = Vec::new();
    let num_str = format!("{:.2}", num);
    let num_parts: Vec<&str> = num_str.split('.').collect();
    let integer_part = num_parts[0];
    let decimal_part = if num_parts.len() > 1 {
        num_parts[1]
    } else {
        "00"
    };

    for (i, c) in integer_part.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            parts.push(',');
        }
        parts.push(c);
    }

    parts.reverse();
    let formatted_integer: String = parts.into_iter().collect();

    format!("{}.{}", formatted_integer, decimal_part)
}

pub fn is_valid_pubkey(s: &str) -> bool {
    Pubkey::from_str(s).is_ok()
}

pub fn current_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}
