use crate::{
    blockchain::constants::*,
    blockchain::instruction::PumpInstructionBuilder,
    blockchain::models::*,
    blockchain::utils::{get_explorer_url, lamports_to_sol, load_rpc_url_with_network},
    error::{PumpFunError, Result},
};

use log::info;
use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    message::Message,
    pubkey::Pubkey,
    signature::{Keypair, Signature, Signer},
    transaction::Transaction,
};
use spl_associated_token_account::get_associated_token_address_with_program_id;
use std::str::FromStr;
use std::time::Duration;

const SPL_TOKEN_2022_PROGRAM_ID: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
const SPL_ASSOCIATED_TOKEN_PROGRAM_ID: &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";
const BONDING_CURVE_DISCRIMINATOR: [u8; 8] = [23, 183, 248, 55, 96, 216, 172, 96];
const GLOBAL_DISCRIMINATOR: [u8; 8] = [167, 232, 232, 177, 200, 108, 114, 127];
const GLOBAL_BUYBACK_RECIPIENTS_OFFSET: usize = 741;
const DEVNET_MAX_BUY_LAMPORTS: u64 = 30_000_000;
const DEVNET_SOL_RESERVE_LAMPORTS: u64 = 20_000_000;

fn parse_bonding_curve_data(data: &[u8]) -> Result<(Pubkey, bool, u64, u64, u64, u64, u64)> {
    if data.len() < 81 || data[..8] != BONDING_CURVE_DISCRIMINATOR {
        return Err(PumpFunError::InvalidAccountData(
            "Pump.fun bonding curve has an unsupported layout".to_string(),
        ));
    }

    let read_u64 = |start: usize| -> Result<u64> {
        let bytes = data
            .get(start..start + 8)
            .ok_or_else(|| PumpFunError::InvalidAccountData("Truncated bonding curve".into()))?;
        Ok(u64::from_le_bytes(bytes.try_into()?))
    };
    let creator = Pubkey::try_from(&data[49..81])
        .map_err(|_| PumpFunError::InvalidAccountData("Invalid bonding curve creator".into()))?;

    Ok((
        creator,
        data[48] != 0,
        read_u64(8)?,
        read_u64(16)?,
        read_u64(24)?,
        read_u64(32)?,
        read_u64(40)?,
    ))
}

pub struct PumpFunClient {
    pub rpc_client: RpcClient,
    pub instruction_builder: PumpInstructionBuilder,
    pub network: Network,
    pub token_2022_program_id: Pubkey,
    pub associated_token_program_id: Pubkey,
}

impl PumpFunClient {
    pub fn new(rpc_url: &str, network: Network) -> Self {
        println!("🔧 Creating new PumpFunClient");
        println!("   Network: {:?}", network);
        println!("   RPC URL: {}", rpc_url);

        let token_2022_program_id = Pubkey::from_str(SPL_TOKEN_2022_PROGRAM_ID).unwrap();
        let associated_token_program_id =
            Pubkey::from_str(SPL_ASSOCIATED_TOKEN_PROGRAM_ID).unwrap();

        println!("   Token 2022 Program ID: {}", token_2022_program_id);
        println!(
            "   Associated Token Program ID: {}",
            associated_token_program_id
        );

        Self {
            rpc_client: RpcClient::new_with_commitment(
                rpc_url.to_string(),
                CommitmentConfig::confirmed(),
            ),
            instruction_builder: PumpInstructionBuilder::new(network),
            network,
            token_2022_program_id,
            associated_token_program_id,
        }
    }

    pub fn devnet() -> Self {
        println!("🌐 Creating Devnet client");
        let network = Network::Devnet;
        let rpc_url = load_rpc_url_with_network("RPC_URL", &network);
        Self::new(&rpc_url, network)
    }

    pub fn mainnet() -> Self {
        println!("🌐 Creating Mainnet client");
        let network = Network::Mainnet;
        let rpc_url = load_rpc_url_with_network("RPC_URL", &network);
        Self::new(&rpc_url, network)
    }

    pub async fn check_balance(&self, wallet: &Pubkey) -> Result<f64> {
        println!("💰 Checking balance for wallet: {}", wallet);
        let balance = self.rpc_client.get_balance(wallet)?;
        let sol_balance = lamports_to_sol(balance);
        println!("   Balance: {} SOL ({} lamports)", sol_balance, balance);
        info!("💰 Wallet balance: {} SOL", sol_balance);
        Ok(sol_balance)
    }

    // ==================== CREATE (ORIGINAL) ====================

    pub async fn create_token(
        &self,
        creator_keypair: &Keypair,
        name: String,
        symbol: String,
        uri: String,
    ) -> Result<Pubkey> {
        println!("\n🪙 Creating token with original create...");
        println!("   Name: {}", name);
        println!("   Symbol: {}", symbol);
        println!("   URI: {}", uri);
        println!("   Creator: {}", creator_keypair.pubkey());

        let mint_keypair = Keypair::new();
        println!("   Mint (new keypair): {}", mint_keypair.pubkey());

        let create_ix = self.instruction_builder.build_create_instruction(
            &creator_keypair.pubkey(),
            &mint_keypair.pubkey(),
            name,
            symbol,
            uri,
        )?;

        let config = SwapConfig {
            priority_fee_microlamports: Some(5000),
            compute_unit_limit: Some(400_000),
            network: self.network,
            slippage_bps: 0,
            use_jito: false,
        };

        let result = self
            .send_transaction_with_signers(
                &[creator_keypair, &mint_keypair],
                vec![create_ix],
                &config,
            )
            .await?;

        let mint = mint_keypair.pubkey();

        println!("\n✅ Token created successfully!");
        println!("   Mint: {}", mint);
        println!("   Signature: {}", result.signature);
        println!("   Explorer: {}", result.explorer_url);

        let bonding_curve = Pubkey::find_program_address(
            &[b"bonding-curve", mint.as_ref()],
            &self.instruction_builder.program_id,
        )
        .0;

        println!("\n📝 Save these values:");
        println!("   MINT={}", mint);
        println!("   BONDING_CURVE={}", bonding_curve);

        Ok(mint)
    }

    // ==================== CREATE V2 ====================

    pub async fn create_token_v2(
        &self,
        creator_keypair: &Keypair,
        mint_keypair: &Keypair,
        name: String,
        symbol: String,
        uri: String,
        is_mayhem_mode: bool,
        is_cashback_enabled: bool,
    ) -> Result<Pubkey> {
        println!("\n🪙 Creating token with create_v2...");
        println!("   Name: {}", name);
        println!("   Symbol: {}", symbol);
        println!("   URI: {}", uri);
        println!("   Mint (new keypair): {}", mint_keypair.pubkey());
        println!("   Creator: {}", creator_keypair.pubkey());
        println!("   Mayhem mode: {}", is_mayhem_mode);
        println!("   Cashback enabled: {}", is_cashback_enabled);

        let create_ix = self.instruction_builder.build_create_v2_instruction(
            &creator_keypair.pubkey(),
            &mint_keypair.pubkey(),
            name,
            symbol,
            uri,
            is_mayhem_mode,
            is_cashback_enabled,
        )?;

        let config = SwapConfig {
            priority_fee_microlamports: Some(5000),
            compute_unit_limit: Some(400_000),
            network: self.network,
            slippage_bps: 0,
            use_jito: false,
        };

        let result = self
            .send_transaction_with_signers(
                &[creator_keypair, mint_keypair],
                vec![create_ix],
                &config,
            )
            .await?;

        let mint = mint_keypair.pubkey();

        println!("\n✅ Token created successfully!");
        println!("   Mint: {}", mint);
        println!("   Signature: {}", result.signature);
        println!("   Explorer: {}", result.explorer_url);

        let bonding_curve = Pubkey::find_program_address(
            &[b"bonding-curve", mint.as_ref()],
            &self.instruction_builder.program_id,
        )
        .0;

        println!("\n📝 Save these values:");
        println!("   MINT={}", mint);
        println!("   BONDING_CURVE={}", bonding_curve);

        Ok(mint)
    }

    // ==================== POOL INFO ====================

    pub async fn get_pool_info(&self, mint: &Pubkey) -> Result<PumpPoolInfo> {
        println!("\n📊 Fetching pool info for mint: {}", mint);

        let (bonding_curve, _) = Pubkey::find_program_address(
            &[b"bonding-curve", mint.as_ref()],
            &self.instruction_builder.program_id,
        );
        let token_program = self.get_token_program(mint)?;
        let associated_bonding_curve =
            get_associated_token_address_with_program_id(&bonding_curve, mint, &token_program);
        let curve_account = self.rpc_client.get_account(&bonding_curve)?;
        let (
            creator,
            complete,
            virtual_token_reserves,
            virtual_sol_reserves,
            real_token_reserves,
            real_sol_reserves,
            token_total_supply,
        ) = parse_bonding_curve_data(&curve_account.data)?;

        println!("✅ Pool found!");
        println!("   Bonding Curve: {}", bonding_curve);
        println!("   Creator: {}", creator);
        println!("   Complete: {}", complete);
        println!(
            "   Virtual SOL: {} SOL",
            lamports_to_sol(virtual_sol_reserves)
        );
        println!(
            "   Virtual Tokens: {} tokens",
            lamports_to_sol(virtual_token_reserves)
        );

        Ok(PumpPoolInfo {
            mint: *mint,
            bonding_curve,
            associated_bonding_curve,
            creator,
            virtual_token_reserves,
            virtual_sol_reserves,
            real_token_reserves,
            real_sol_reserves,
            token_total_supply,
            complete,
            created_at: None,
        })
    }

    pub async fn buy_token_with_sol(
        &self,
        wallet: &Keypair,
        mint: &Pubkey,
        max_sol_cost: u64,
    ) -> Result<TransactionResult> {
        self.require_devnet_trading()?;
        if max_sol_cost == 0 || max_sol_cost > DEVNET_MAX_BUY_LAMPORTS {
            return Err(PumpFunError::Custom(
                "Devnet buy amount must be between 1 lamport and 0.03 SOL".to_string(),
            ));
        }

        let balance = self.rpc_client.get_balance(&wallet.pubkey())?;
        if balance < max_sol_cost.saturating_add(DEVNET_SOL_RESERVE_LAMPORTS) {
            return Err(PumpFunError::InsufficientBalance {
                need: lamports_to_sol(max_sol_cost + DEVNET_SOL_RESERVE_LAMPORTS),
                have: lamports_to_sol(balance),
            });
        }

        let pool = self.get_pool_info(mint).await?;
        if pool.complete {
            return Err(PumpFunError::PoolComplete);
        }
        let token_program = self.get_token_program(mint)?;
        let fee_recipient = self.get_fee_recipient()?;
        let buyback_fee_recipient = self.get_buyback_fee_recipient()?;
        // Pump.fun buys require the recipient ATA to exist before the instruction executes.
        // Without this, the program can reject the transaction with a custom 3012 error.
        let create_ata_ix =
            spl_associated_token_account::instruction::create_associated_token_account_idempotent(
                &wallet.pubkey(),
                &wallet.pubkey(),
                mint,
                &token_program,
            );

        // Leave headroom for protocol/creator fees so total SOL cost stays below max_sol_cost.
        let curve_cost = u128::from(max_sol_cost) * 80 / 100;
        let token_amount = curve_cost
            .checked_mul(u128::from(pool.virtual_token_reserves))
            .and_then(|value| value.checked_div(u128::from(pool.virtual_sol_reserves) + curve_cost))
            .and_then(|value| u64::try_from(value).ok())
            .ok_or(PumpFunError::ArithmeticOverflow)?;
        if token_amount == 0 {
            return Err(PumpFunError::InsufficientLiquidity);
        }

        let instruction = self.instruction_builder.build_buy_instruction(
            &wallet.pubkey(),
            mint,
            &pool.creator,
            &token_program,
            &fee_recipient,
            &buyback_fee_recipient,
            token_amount,
            max_sol_cost,
        )?;
        let config = SwapConfig {
            priority_fee_microlamports: Some(5_000),
            compute_unit_limit: Some(400_000),
            network: self.network,
            slippage_bps: 2_000,
            use_jito: false,
        };
        let instructions = vec![create_ata_ix, instruction];
        self.send_transaction_with_signers(&[wallet], instructions, &config)
            .await
    }

    pub async fn sell_all_tokens(
        &self,
        wallet: &Keypair,
        mint: &Pubkey,
    ) -> Result<Option<TransactionResult>> {
        self.require_devnet_trading()?;
        let token_program = self.get_token_program(mint)?;
        let associated_user =
            get_associated_token_address_with_program_id(&wallet.pubkey(), mint, &token_program);
        let token_account = self
            .rpc_client
            .get_account_with_commitment(&associated_user, CommitmentConfig::confirmed())?
            .value;
        let Some(token_account) = token_account else {
            return Ok(None);
        };
        if token_account.data.len() < 72 {
            return Err(PumpFunError::InvalidAccountData(
                "Token account is shorter than the SPL token account layout".to_string(),
            ));
        }
        let amount = u64::from_le_bytes(token_account.data[64..72].try_into()?);
        if amount == 0 {
            return Ok(None);
        }

        let pool = self.get_pool_info(mint).await?;
        if pool.complete {
            return Err(PumpFunError::PoolComplete);
        }
        let fee_recipient = self.get_fee_recipient()?;
        let buyback_fee_recipient = self.get_buyback_fee_recipient()?;
        let expected_sol_output = u128::from(amount)
            .checked_mul(u128::from(pool.virtual_sol_reserves))
            .and_then(|value| {
                value.checked_div(u128::from(pool.virtual_token_reserves) + u128::from(amount))
            })
            .and_then(|value| u64::try_from(value).ok())
            .ok_or(PumpFunError::ArithmeticOverflow)?;
        let min_sol_output = expected_sol_output.saturating_mul(80) / 100;

        let instruction = self.instruction_builder.build_sell_instruction(
            &wallet.pubkey(),
            mint,
            &pool.creator,
            &token_program,
            &fee_recipient,
            &buyback_fee_recipient,
            amount,
            min_sol_output,
        )?;
        let config = SwapConfig {
            priority_fee_microlamports: Some(5_000),
            compute_unit_limit: Some(400_000),
            network: self.network,
            slippage_bps: 2_000,
            use_jito: false,
        };
        self.send_transaction_with_signers(&[wallet], vec![instruction], &config)
            .await
            .map(Some)
    }

    fn get_token_program(&self, mint: &Pubkey) -> Result<Pubkey> {
        let mint_account = self.rpc_client.get_account(mint)?;
        if mint_account.owner != accounts::TOKEN_PROGRAM
            && mint_account.owner.to_string() != SPL_TOKEN_2022_PROGRAM_ID
        {
            return Err(PumpFunError::InvalidAccountData(format!(
                "Mint {mint} is owned by unsupported token program {}",
                mint_account.owner
            )));
        }
        Ok(mint_account.owner)
    }

    fn get_fee_recipient(&self) -> Result<Pubkey> {
        let global = self
            .rpc_client
            .get_account(&self.instruction_builder.get_global_pda())?;
        if global.data.len() < 73 || global.data[..8] != GLOBAL_DISCRIMINATOR {
            return Err(PumpFunError::InvalidAccountData(
                "Pump.fun global account has an unsupported layout".to_string(),
            ));
        }
        Pubkey::try_from(&global.data[41..73])
            .map_err(|_| PumpFunError::InvalidAccountData("Invalid fee recipient".to_string()))
    }

    fn get_buyback_fee_recipient(&self) -> Result<Pubkey> {
        let global = self
            .rpc_client
            .get_account(&self.instruction_builder.get_global_pda())?;
        let recipients_end = GLOBAL_BUYBACK_RECIPIENTS_OFFSET + 32;
        if global.data.len() < recipients_end || global.data[..8] != GLOBAL_DISCRIMINATOR {
            return Err(PumpFunError::InvalidAccountData(
                "Pump.fun global account does not contain buyback fee configuration".to_string(),
            ));
        }

        let recipient =
            Pubkey::try_from(&global.data[GLOBAL_BUYBACK_RECIPIENTS_OFFSET..recipients_end])
                .map_err(|_| {
                    PumpFunError::InvalidAccountData(
                        "Invalid Pump.fun buyback fee recipient".to_string(),
                    )
                })?;
        if recipient == Pubkey::default() {
            return Err(PumpFunError::InvalidAccountData(
                "Pump.fun global account has no configured buyback fee recipient".to_string(),
            ));
        }
        Ok(recipient)
    }

    fn require_devnet_trading(&self) -> Result<()> {
        if self.network != Network::Devnet {
            return Err(PumpFunError::Custom(
                "Automatic buy/sell is enabled only on Devnet".to_string(),
            ));
        }
        Ok(())
    }

    // ==================== TRANSACTION HELPERS ====================

    async fn send_transaction_with_signers(
        &self,
        signers: &[&Keypair],
        instructions: Vec<solana_sdk::instruction::Instruction>,
        config: &SwapConfig,
    ) -> Result<TransactionResult> {
        println!("\n   🔍 SENDING TRANSACTION");
        println!("   Instructions: {}", instructions.len());
        println!("   Signers: {}", signers.len());

        let mut final_instructions = Vec::new();

        if let Some(limit) = config.compute_unit_limit {
            final_instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(limit));
        }
        if let Some(fee) = config.priority_fee_microlamports {
            final_instructions.push(ComputeBudgetInstruction::set_compute_unit_price(fee));
        }

        final_instructions.extend(instructions);

        let recent_blockhash = self.rpc_client.get_latest_blockhash()?;

        let message = Message::new(&final_instructions, Some(&signers[0].pubkey()));
        let mut transaction = Transaction::new_unsigned(message);
        transaction.sign(signers, recent_blockhash);

        let simulation = self.rpc_client.simulate_transaction(&transaction)?;
        if let Some(err) = simulation.value.err {
            let logs = simulation.value.logs.unwrap_or_default().join("\n");
            return Err(PumpFunError::TransactionFailed(format!(
                "Simulation failed: {err:?}\n{logs}"
            )));
        }

        let signature = self.send_with_retry(&transaction).await?;
        self.confirm_transaction(&signature).await?;

        let signature_str = signature.to_string();
        let explorer_url = get_explorer_url(&signature_str, &self.network);
        let slot = self.rpc_client.get_slot()?;

        Ok(TransactionResult {
            signature: signature_str,
            explorer_url,
            slot,
            block_time: None,
            fee: 5000,
        })
    }

    async fn send_with_retry(&self, transaction: &Transaction) -> Result<Signature> {
        let max_retries = 3;
        let mut last_error = None;

        for i in 0..max_retries {
            println!("   Send attempt {}/{}", i + 1, max_retries);
            match self.rpc_client.send_transaction(transaction) {
                Ok(sig) => {
                    println!("   ✅ Send successful");
                    return Ok(sig);
                }
                Err(e) => {
                    println!("   ⚠️ Send failed: {}", e);
                    last_error = Some(e);
                    if i < max_retries - 1 {
                        tokio::time::sleep(Duration::from_millis(500 * (i + 1))).await;
                    }
                }
            }
        }

        Err(PumpFunError::TransactionFailed(format!(
            "Failed after {} retries: {:?}",
            max_retries, last_error
        )))
    }

    async fn confirm_transaction(&self, signature: &Signature) -> Result<()> {
        println!("   Waiting for confirmation...");
        let timeout = Duration::from_secs(30);
        let start = std::time::Instant::now();

        while start.elapsed() < timeout {
            match self.rpc_client.get_signature_status(signature)? {
                Some(status) => {
                    if status.is_ok() {
                        println!("   ✅ Transaction confirmed");
                        return Ok(());
                    } else {
                        return Err(PumpFunError::TransactionFailed(format!(
                            "Transaction failed: {:?}",
                            status
                        )));
                    }
                }
                None => {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }

        Err(PumpFunError::TransactionTimeout)
    }
}
