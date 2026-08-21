# Solana On-Chain Pool Indexer

Direct on-chain pool discovery for Solana DEXes to get accurate token addresses, decimals, and metadata.

## Features

- **Direct On-Chain Discovery**: Uses `getProgramAccounts` to discover pools directly from Solana programs
- **Accurate Token Data**: Reads token mints and decimals directly from pool state
- **Multiple DEX Support**: Raydium CLMM, Orca Whirlpool, Meteora DLMM
- **Token Metadata**: Fetches symbols, names, and logos from Jupiter's token list API
- **Database Integration**: Saves pools to PostgreSQL compatible with existing price fetcher

## Supported DEXes

| DEX | Program ID | Pool Type | Status |
|-----|------------|-----------|---------|
| Raydium CLMM | `CAMMCzo5YL8w4VFF8KVHrK22GGUQpMTdQgXX3HSyiDIJ` | Concentrated Liquidity | ✅ |
| Orca Whirlpool | `whirLbMi1e9m3LWKTEjZMmYFJ3h9h4wJ5KR9eV9CdnU` | Concentrated Liquidity | ✅ |
| Meteora DLMM | `LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo` | Dynamic Liquidity Market Maker | ✅ |

## Environment Variables

- `DATABASE_URL`: PostgreSQL connection string (required)
- `SOLANA_RPC_ENDPOINT`: Solana RPC endpoint (default: `https://api.mainnet-beta.solana.com`)

## Usage

### Local Development
```bash
export DATABASE_URL="postgres://user:password@localhost/db"
export SOLANA_RPC_ENDPOINT="https://api.mainnet-beta.solana.com"
go run main.go
```

### Docker
```bash
docker build -t solana-pool-indexer .
docker run -e DATABASE_URL="..." solana-pool-indexer
```

### Fly.io Deployment
```bash
fly auth login
fly apps create solana-pool-indexer
fly secrets set DATABASE_URL="..."
fly deploy
```

## How It Works

1. **Pool Discovery**: Uses `getProgramAccounts` RPC call to get all pool accounts for each DEX program
2. **Pool Parsing**: Decodes pool state to extract token mint addresses
3. **Token Metadata**: 
   - Gets decimals directly from on-chain mint accounts
   - Fetches symbol/name/logo from Jupiter's token list API
   - Falls back to hardcoded metadata for common tokens
4. **Database Storage**: Saves pools in the same format as the existing GeckoTerminal indexer

## Database Schema

Pools are saved to the `pairs` table with these key fields:
- `id`: Format `solana_{pool_address}`
- `network`: Always "solana"
- `base_token`/`quote_token`: JSON with accurate on-chain data
- `base_token_decimals`/`quote_token_decimals`: On-chain decimals
- `pool_address`: Pool account address
- `dex_name`: Human-readable DEX name

## Rate Limiting

- 50ms delay between pool parsing operations
- Uses timeouts for all external API calls
- Graceful error handling for failed token metadata fetches

## Future Enhancements

- [ ] Add Raydium CPMM support
- [ ] Add Pump.fun pool support  
- [ ] Implement token metadata caching
- [ ] Add liquidity amount parsing
- [ ] Support for more Meteora pool types