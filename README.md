# 🚀 Create and Deploy Token on Pump.fun

A Rust code example for creating and deploying tokens on [Pump.fun](https://pump.fun).

## Features
- ✅ Create tokens (`create` and `create_v2`)
- ✅ Live pool verification after deployment
- ✅ Devnet & Mainnet support

## Environment Setup
Create a `.env` file in your project root:

```dotenv
# Your Solana keypair: base58-encoded 64-byte secret, JSON array of 64 bytes, or path to a keypair JSON file.
# A PRIVATE_KEY exported in your shell takes precedence over this .env value.
PRIVATE_KEY=YOUR_BASE58_PRIVATE_KEY_HERE

# Gemini API key from https://aistudio.google.com/apikey
GEMINI_API_KEY=YOUR_GEMINI_API_KEY
# Optional; defaults to gemini-3.6-flash
AI_MODEL=gemini-3.6-flash
# Optional fallback API key from https://console.groq.com/keys
GROQ_API_KEY=YOUR_GROQ_API_KEY
# Optional; defaults to openai/gpt-oss-20b
GROQ_MODEL=openai/gpt-oss-20b

# Launch RPC endpoint — get a free key at shyft.to or helius.dev
RPC_URL=https://devnet-rpc.shyft.to?api_key=YOUR_API_KEY

# Pump.fun program ID 
PUMP_FUN_PROGRAM=6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P

# Optional Telegram group notifications. Add the bot to the group and grant it
# permission to send messages; CHAT_BOT_API is the group's chat ID.
TELEGRAM_BOT_API=YOUR_TELEGRAM_BOT_TOKEN
CHAT_BOT_API=YOUR_TELEGRAM_GROUP_CHAT_ID

# Optional: Jito MEV (mainnet only)
JITO_UUID=
JITO_BLOCK_ENGINE_URL=https://mainnet.block-engine.jito.wtf
```

## Usage

Run the interactive CLI:
```bash
cargo run
```
Or run the example directly:
```bash
cargo run --example create_token
```

`cargo run` first asks for the token-launch network, before initializing the AI.
Choose Devnet while testing. Ingress always uses the hardcoded Solana mainnet
RPC/WebSocket endpoints in `src/logic.rs`; change those in code if you want to
stream from another network. `RPC_URL` is used by the token-launch client.
The agent prioritizes launching saved tokens before X searches or saving new
ingress. On Devnet only, tokens without metadata use the shared
`https://invalid.local/custody-alias-sibling.json` test URI so both create
methods can be exercised; it is not a real metadata host, so launched tokens
will not have usable metadata. Mainnet launches still require a valid public
metadata JSON URI. A current price drop by itself does not block eligible
launches, and an image URL is not a substitute for metadata.
Although ingress events can arrive continuously, AI decisions are limited to
one request per 30 seconds. Events received during that cooldown update the
latest ingress context and do not trigger extra Gemini requests.
Gemini is the primary AI provider. If its request fails or returns invalid JSON,
the agent retries the same request through Groq when `GROQ_API_KEY` is set.
When Gemini returns HTTP 429, the agent honors Google's retry delay and uses
Groq directly during the cooldown instead of repeatedly calling the exhausted
Gemini quota. Without a Groq key, it continues with Gemini only.
The agent aims for 10–25 successful launches per UTC day when eligible saved
tokens and wallet funds are available. It tracks successful launches in the
git-ignored `.launch-count.json`; the count resets on the next UTC day. The
token store holds up to 25 candidates. This is a target, not a guarantee:
Mainnet launches require a valid metadata URI.
Routine missing metadata and DexScreener lookup failures are quiet; enable
debug logging with `RUST_LOG=debug` when investigating data-fetch issues.
When both Telegram variables are configured, each AI action and each successful
token launch is posted to the configured group. Notifications are optional;
delivery failures are reported in the console without stopping the agent.

The standalone example (`cargo run --example create_token`) has its own guided
creation prompts. Here is what to expect when running that example:

---

### Step 1 — Network selection

```
📝 Creator Wallet: YOUR_WALLET

Select network:
1. Devnet (testing)
2. Mainnet (real)
Choice:
```
Type `1` for Devnet or `2` for Mainnet and press Enter.

---

### Step 2 — Create method

```
Select create method:
1. Create_v2 
2. Create 
Choice:
```
- **`1` — create_v2**
- **`2` — create** 

---

### Step 3 — Token details

```
Token name: My Token
Token symbol: MYTKN
Token URI (metadata): https://your-metadata-uri.com/token.json
```
Enter your token name , symbol, and a publicly accessible metadata URI (click enter at interval). The URI should point to a JSON file with at least `name`, `symbol`, `description`, and `image` fields.

---

### Step 4 — Mayhem & cashback (`create_v2` only)

If you selected `create_v2` you will also be asked (only when you run `cargo run`.. `cargo run --exampe create_token` already have redefined mayhem and cashback):

```
Mayhem mode? (y/n, default: n):
Cashback enabled? (y/n, default: n):
```
- Type `y` or `n` for each and press Enter.
- For standard token creation leave both as `n`.
- Mayhem mode enables the Pump.fun mayhem program integration.
- Cashback routes a portion of fees back to the creator.

> If you selected **Create** these two prompts are skipped entirely.

---

### Step 5 — Result

On success you will see:
```
✅ TOKEN CREATED SUCCESSFULLY!
   Mint: 9X3yZ5gfr4oKBApyEEMWVjWdwmzNrk66N6TtrTMgmj9R
   Signature: 5Zy...
   Explorer: https://explorer.solana.com/tx/...?cluster=devnet

📝 Save these values:
   MINT=9X3yZ5gfr4oKBApyEEMWVjWdwmzNrk66N6TtrTMgmj9R
   BONDING_CURVE=GzLP7XnEduyKmSWFXb968bYfnd8fzHFKSfzu3iyfTWhF
```
Save the mint address — you will need it for any further interactions with the token.

> 💡 Test on **Devnet** first. You need at least **0.1 SOL** to cover rent and fees. Get free Devnet SOL at [faucet.solana.com](https://faucet.solana.com).

---

## Switching to Mainnet

Two things to update:

1. **`RPC_URL`** in your `.env` — swap to a mainnet endpoint e.g. `https://rpc.shyft.to?api_key=YOUR_API_KEY`
2. **Priority fee** in `client.rs` — increase from `5_000` to at least `50_000` microlamports, mainnet is competitive and low fees will get your transaction dropped

The program ID is the same on both networks. No other changes needed.

ps: the project is set to dev net. recommended you run first on devnet before mainnet
## Related Links

- [Shyft gRPC Docs](https://docs.shyft.to/solana-fast-grpc/grpc-docs)
- [Start Streaming with Shyft](https://shyft.to/solana-yellowstone-grpc)
- [Shyft Website](https://shyft.to/)
# narrixAi
