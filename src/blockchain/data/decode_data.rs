use crate::blockchain::event::{
    CreateEvent, CreateEventEvent, TradeEvent, TradeEventEvent, CREATE_EVENT_EVENT_DISCM,
    TRADE_EVENT_EVENT_DISCM,
};
use base64::engine::general_purpose;
use base64::Engine;

#[derive(Debug)]
pub struct AccountEventError {
    pub message: String,
}

#[derive(Debug)]
pub enum DecodedPumpFunEvent {
    Create(CreateEvent),
    Trade(TradeEvent),
}

pub fn convert_to_discm(base64_string: &str) -> Result<Vec<u8>, base64::DecodeError> {
    general_purpose::STANDARD.decode(base64_string)
}

pub fn extract_log_messages(logs: &[String]) -> impl Iterator<Item = &str> {
    logs.iter()
        .filter_map(|message| message.strip_prefix("Program data: ").map(str::trim))
}

pub fn decode_event_data(buf: &[u8]) -> Result<DecodedPumpFunEvent, AccountEventError> {
    if buf.len() < 8 {
        return Err(AccountEventError {
            message: "Buffer too short to contain a valid discriminator.".to_string(),
        });
    }

    let discriminator: [u8; 8] = buf[..8]
        .try_into()
        .expect("Failed to extract first 8 bytes");

    match discriminator {
        CREATE_EVENT_EVENT_DISCM => CreateEventEvent::deserialize(&mut &buf[..])
            .map(|event| DecodedPumpFunEvent::Create(event.0))
            .map_err(|error| AccountEventError {
                message: format!("Failed to deserialize create event: {error}"),
            }),
        TRADE_EVENT_EVENT_DISCM => TradeEventEvent::deserialize(&mut &buf[..])
            .map(|event| DecodedPumpFunEvent::Trade(event.0))
            .map_err(|error| AccountEventError {
                message: format!("Failed to deserialize trade event: {error}"),
            }),
        _ => Err(AccountEventError {
            message: "Unsupported event discriminator.".to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blockchain::event::CreateEventEvent;
    use borsh::BorshSerialize;
    use solana_sdk::pubkey::Pubkey;

    #[test]
    fn decodes_create_event_metadata() {
        let event = CreateEvent {
            name: "Example Token".to_string(),
            symbol: "EXAMPLE".to_string(),
            uri: "https://example.com/token.json".to_string(),
            mint: Pubkey::new_unique(),
            bonding_curve: Pubkey::new_unique(),
            user: Pubkey::new_unique(),
            creator: Pubkey::new_unique(),
            timestamp: 1,
            virtual_token_reserves: 2,
            virtual_sol_reserves: 3,
            real_token_reserves: 4,
            token_total_supply: 5,
            token_program: Pubkey::new_unique(),
            is_mayhem_mode: false,
            is_cashback_enabled: false,
        };
        let mut encoded = Vec::new();
        CreateEventEvent(event).serialize(&mut encoded).unwrap();

        match decode_event_data(&encoded).unwrap() {
            DecodedPumpFunEvent::Create(decoded) => {
                assert_eq!(decoded.name, "Example Token");
                assert_eq!(decoded.symbol, "EXAMPLE");
            }
            DecodedPumpFunEvent::Trade(_) => panic!("decoded create event as trade"),
        }
    }

    #[test]
    fn ignores_unsupported_event_discriminators() {
        assert!(decode_event_data(&[0; 8]).is_err());
    }
}
