// evm_indexer.go — On-chain EVM pool indexer for BSC and Base
//
// Strategy: for each factory contract, listen to the PoolCreated event
// (V2/V3 standard) or Initialize event (V4/Infinity CLMM) via eth_subscribe
// on the WS RPC. On each new pool event:
//   1. Extract token0, token1 directly from the event log topics/data
//   2. Fetch decimals for both tokens via eth_call decimals()
//   3. Upsert into the pairs table with correct on-chain ordering
//
// Also runs a one-time backfill on startup using The Graph public subgraphs
// to pull top pools by liquidity — so existing pairs are corrected immediately
// without waiting for new pool creation events.
//
// Supported:
//   BSC  — PancakeSwap V2, PancakeSwap V3, PancakeSwap Infinity (CLMM), Uniswap V3
//   Base — Uniswap V3, Aerodrome Slipstream
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"database/sql"

	"github.com/gorilla/websocket"
)

// ── Factory / event constants ─────────────────────────────────────────────────

// V2 PairCreated(address indexed token0, address indexed token1, address pair, uint)
// topic0 = keccak256("PairCreated(address,address,address,uint256)")
const topicV2PairCreated = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"

// V3 PoolCreated(address indexed token0, address indexed token1, uint24 indexed fee, int24 tickSpacing, address pool)
// topic0 = keccak256("PoolCreated(address,address,uint24,int24,address)")
const topicV3PoolCreated = "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118"

// PancakeSwap Infinity CLPoolManager Initialize event
// event Initialize(PoolId indexed id, Currency indexed currency0, Currency indexed currency1,
//                  IHooks hooks, uint24 fee, bytes32 parameters, uint160 sqrtPriceX96, int24 tick)
// topic0 = keccak256("Initialize(bytes32,address,address,address,uint24,bytes32,uint160,int24)")
// topics[1] = poolId (bytes32) — used as pool_address
// topics[2] = currency0 (token0 address)
// topics[3] = currency1 (token1 address)
const topicInfinityInitialize = "0x426cc62fe6a33a40ba2788c2c87a9c34ee4582b95bc9fa5a7bb7ae70b750b99c"

// Uniswap V4 BSC PoolManager Initialize event
// event Initialize(PoolId indexed id, Currency indexed currency0, Currency indexed currency1,
//                  uint24 fee, int24 tickSpacing, IHooks hooks, uint160 sqrtPriceX96, int24 tick)
// topic0 = keccak256("Initialize(bytes32,address,address,uint24,int24,address,uint160,int24)")
// Same topic layout: topics[1]=poolId, topics[2]=currency0, topics[3]=currency1
const topicUniswapV4Initialize = "0xdd466e674ea557f56295e2d0218a125ea4b4f0f6f3307b95f85e6110838d6438"

// CLPoolManager address — the singleton that owns all Infinity CL pools on BSC
const infinityCLPoolManager = "0xa0ffb9c1ce1fe56963b0321b32e7a0302114058b"

// Uniswap V4 PoolManager on BSC
const uniswapV4PoolManagerBSC = "0x28e2ea090877bf75740558f6bfb36a5ffee9e9df"

// Uniswap V4 PoolManager on Base
const uniswapV4PoolManagerBase = "0x498581ff718922c3f8e6a244956af099b2652b2b"

// Known factory addresses per chain/DEX
var evmFactories = []EVMFactory{
	// ── BSC ─────────────────────────────────────────────────────────────────
	{
		Network:   "bsc",
		DexName:   "PancakeSwap V2",
		DexID:     "pancakeswap_v2",
		PoolType:  "v2",
		Address:   "0xca143ce32fe78f1f7019d7d551a6402fc5350c73",
		EventTopic: topicV2PairCreated,
		Kind:      factoryKindV2,
	},
	{
		Network:   "bsc",
		DexName:   "PancakeSwap V3",
		DexID:     "pancakeswap_v3",
		PoolType:  "v3",
		Address:   "0x0bfbcf9fa4f9c56b0f40a671ad40e0805a091865",
		EventTopic: topicV3PoolCreated,
		Kind:      factoryKindV3,
	},
	{
		Network:   "bsc",
		DexName:   "Uniswap V3",
		DexID:     "uniswap_v3",
		PoolType:  "v3",
		Address:   "0xdb1d10011ad0ff90774d0c6bb92e5c5c8b4461f7",
		EventTopic: topicV3PoolCreated,
		Kind:      factoryKindV3,
	},
	{
		// Singleton CLPoolManager — not a factory. One contract owns all CL pools.
		// Pool creation fires Initialize(bytes32 poolId, address currency0, address currency1, ...)
		// poolId (topic[1]) is the bytes32 pool key hash — stored as pool_address.
		// currency0 / currency1 are in topic[2] / topic[3].
		Network:    "bsc",
		DexName:    "PancakeSwap Infinity",
		DexID:      "pancakeswap_infinity",
		PoolType:   "clmm",
		Address:    infinityCLPoolManager,
		EventTopic: topicInfinityInitialize,
		Kind:       factoryKindInfinity,
	},
	{
		// Uniswap V4 BSC PoolManager — also a singleton.
		// Same event layout as Infinity: topics[1]=poolId, topics[2]=currency0, topics[3]=currency1
		// poolId stored as pool_address (bytes32 hash of the PoolKey struct).
		Network:    "bsc",
		DexName:    "Uniswap V4",
		DexID:      "uniswap_v4",
		PoolType:   "v4",
		Address:    uniswapV4PoolManagerBSC,
		EventTopic: topicUniswapV4Initialize,
		Kind:       factoryKindInfinity, // same extraction logic: poolId/currency0/currency1 all indexed
	},
	// ── Base ────────────────────────────────────────────────────────────────
	{
		Network:   "base",
		DexName:   "Uniswap V3",
		DexID:     "uniswap_v3",
		PoolType:  "v3",
		Address:   "0x33128a8fc17869897dce68ed026d694621f6fdfd",
		EventTopic: topicV3PoolCreated,
		Kind:      factoryKindV3,
	},
	{
		Network:   "base",
		DexName:   "Aerodrome Slipstream",
		DexID:     "aerodrome_slipstream",
		PoolType:  "v3",
		// Aerodrome CL Factory on Base
		Address:   "0x5e7bb104d84c7cb9b682aac2f3d509f5f406809a",
		EventTopic: topicV3PoolCreated,
		Kind:      factoryKindV3,
	},
	{
		Network:   "base",
		DexName:   "Aerodrome",
		DexID:     "aerodrome",
		PoolType:  "v2",
		// Aerodrome basic AMM Factory on Base
		Address:   "0x420dd381b31aef6683db6b902084cb0ffece40da",
		EventTopic: topicV2PairCreated,
		Kind:      factoryKindV2,
	},
	{
		// Uniswap V4 Base PoolManager — singleton, same event layout as BSC V4.
		// topics[1]=poolId(bytes32), topics[2]=currency0, topics[3]=currency1
		Network:    "base",
		DexName:    "Uniswap V4",
		DexID:      "uniswap_v4",
		PoolType:   "v4",
		Address:    uniswapV4PoolManagerBase,
		EventTopic: topicUniswapV4Initialize,
		Kind:       factoryKindInfinity,
	},
}

type factoryKind int

const (
	factoryKindV2       factoryKind = iota // PairCreated: token0/token1 in topic1/topic2, pair in data[0]
	factoryKindV3                          // PoolCreated: token0/token1 in topic1/topic2, pool in data last word
	factoryKindInfinity                    // Initialize: poolId in topic1, currency0 in topic2, currency1 in topic3
)

type EVMFactory struct {
	Network    string
	DexName    string
	DexID      string
	PoolType   string
	Address    string      // factory contract address (this is also the program_id for EVM)
	EventTopic string
	Kind       factoryKind
}

// ── EVM RPC helper ───────────────────────────────────────────────────────────

type EVMRPCClient struct {
	endpoint string
	mu       sync.Mutex
	client   *http.Client
}

func newEVMRPC(endpoint string) *EVMRPCClient {
	return &EVMRPCClient{
		endpoint: endpoint,
		client:   &http.Client{Timeout: 15 * time.Second},
	}
}

func (c *EVMRPCClient) call(method string, params []interface{}) (json.RawMessage, error) {
	body, _ := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0", "id": 1,
		"method": method, "params": params,
	})
	req, _ := http.NewRequest("POST", c.endpoint, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var result struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	if result.Error != nil {
		return nil, fmt.Errorf("rpc %s: %s", method, result.Error.Message)
	}
	return result.Result, nil
}

// ethCall makes a read-only contract call and returns the raw hex result.
func (c *EVMRPCClient) ethCall(to, data string) (string, error) {
	raw, err := c.call("eth_call", []interface{}{
		map[string]string{"to": to, "data": data}, "latest",
	})
	if err != nil {
		return "", err
	}
	var s string
	json.Unmarshal(raw, &s)
	return s, nil
}

// decodeAddress decodes a 32-byte ABI-padded address result.
func decodeAddress(hex string) string {
	hex = strings.TrimPrefix(hex, "0x")
	if len(hex) < 40 {
		return ""
	}
	return "0x" + strings.ToLower(hex[len(hex)-40:])
}

// decodeUint8 decodes a uint8/uint256 ABI result to int.
func decodeUint8(hex string) int {
	hex = strings.TrimPrefix(hex, "0x")
	if hex == "" {
		return 0
	}
	n, _ := new(big.Int).SetString(hex, 16)
	if n == nil {
		return 0
	}
	return int(n.Int64())
}

// getTokenDecimals fetches ERC-20 decimals() for a token address.
// Falls back to 18 on error.
func (c *EVMRPCClient) getTokenDecimals(token string) int {
	res, err := c.ethCall(token, "0x313ce567") // decimals()
	if err != nil || res == "0x" || res == "" {
		return 18
	}
	d := decodeUint8(res)
	if d <= 0 || d > 77 {
		return 18
	}
	return d
}

// getTokenSymbol fetches ERC-20 symbol() as a string.
// Returns abbreviated address on error.
func (c *EVMRPCClient) getTokenSymbol(token string) string {
	// symbol() → 0x95d89b41
	res, err := c.ethCall(token, "0x95d89b41")
	if err != nil || res == "0x" || res == "" {
		if len(token) >= 6 {
			return token[:6]
		}
		return token
	}
	// ABI-decode string: offset at [0:32], length at [32:64], data at [64:]
	hex := strings.TrimPrefix(res, "0x")
	if len(hex) < 128 {
		return token[:6]
	}
	lengthHex := hex[64:128]
	length, _ := new(big.Int).SetString(lengthHex, 16)
	if length == nil || length.Int64() <= 0 || length.Int64() > 64 {
		return token[:6]
	}
	strBytes := hex[128 : 128+int(length.Int64())*2]
	decoded, err := hexDecodeString(strBytes)
	if err != nil {
		return token[:6]
	}
	sym := strings.TrimSpace(string(decoded))
	if sym == "" {
		return token[:6]
	}
	return sym
}

func hexDecodeString(s string) ([]byte, error) {
	if len(s)%2 != 0 {
		s = "0" + s
	}
	b := make([]byte, len(s)/2)
	for i := range b {
		_, err := fmt.Sscanf(s[2*i:2*i+2], "%02x", &b[i])
		if err != nil {
			return nil, err
		}
	}
	return b, nil
}

// ── EVM decimal cache ────────────────────────────────────────────────────────

type EVMDecimalCache struct {
	mu    sync.RWMutex
	cache map[string]int // key: network:address
}

func newEVMDecimalCache() *EVMDecimalCache {
	return &EVMDecimalCache{cache: make(map[string]int)}
}

func (dc *EVMDecimalCache) key(network, addr string) string {
	return network + ":" + strings.ToLower(addr)
}

func (dc *EVMDecimalCache) Get(network, addr string) (int, bool) {
	dc.mu.RLock()
	defer dc.mu.RUnlock()
	v, ok := dc.cache[dc.key(network, addr)]
	return v, ok
}

func (dc *EVMDecimalCache) Set(network, addr string, decimals int) {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	dc.cache[dc.key(network, addr)] = decimals
}

func (dc *EVMDecimalCache) GetOrFetch(network, addr string, rpc *EVMRPCClient) int {
	if v, ok := dc.Get(network, addr); ok {
		return v
	}
	d := rpc.getTokenDecimals(addr)
	dc.Set(network, addr, d)
	return d
}

// ── The Graph subgraph backfill ──────────────────────────────────────────────

// graphPool is a minimal pool record from The Graph.
type graphPool struct {
	ID     string `json:"id"`
	Token0 struct {
		ID       string `json:"id"`
		Symbol   string `json:"symbol"`
		Decimals string `json:"decimals"`
	} `json:"token0"`
	Token1 struct {
		ID       string `json:"id"`
		Symbol   string `json:"symbol"`
		Decimals string `json:"decimals"`
	} `json:"token1"`
	TotalValueLockedUSD string `json:"totalValueLockedUSD"`
}

// subgraphEndpoints maps (network, dexID) to a public The Graph endpoint.
// These are the official/community subgraphs — no API key needed for basic queries.
var subgraphEndpoints = map[string]string{
	// The Graph decentralized network — requires API key in the URL.
	// Replace <YOUR_API_KEY> with your key from https://thegraph.com/studio/
	// If no key is set, EVM backfill is skipped and only live eth_subscribe runs.
	// PancakeSwap V2 BSC — decentralized network (requires API key)
	"bsc:pancakeswap_v2": "https://gateway.thegraph.com/api/<YOUR_API_KEY>/subgraphs/id/HNhoD4sDN3QBSPJb3kPgtbFDDqxrMJGQnN2gbMnqnV9A",
	// PancakeSwap V3 BSC — official decentralized subgraph
	"bsc:pancakeswap_v3": "https://gateway.thegraph.com/api/<YOUR_API_KEY>/subgraphs/id/Hv1GncLY5docZoGtXjo4kwbTvxm3MAhVZqBZE4sUT9eZ",
	// Uniswap V3 BSC — decentralized network
	"bsc:uniswap_v3": "https://gateway.thegraph.com/api/<YOUR_API_KEY>/subgraphs/id/A1fvJWQLBeUAggX2WBm61BCJKR1oV1Y2tkfh6tRfEEAB",
	// Uniswap V3 Base — decentralized network
	"base:uniswap_v3": "https://gateway.thegraph.com/api/<YOUR_API_KEY>/subgraphs/id/GqzP4Xaehti8KSfQmv3ZctFSjnSUYZ4En5NRsiTbvZpz",
	// Aerodrome Slipstream Base — community subgraph
	"base:aerodrome_slipstream": "https://gateway.thegraph.com/api/<YOUR_API_KEY>/subgraphs/id/ELUcJLa9NL4KbsnrthFJNXSaWR9nuiTQtYXMgDMRiKDS",
	// Aerodrome Base
	"base:aerodrome": "https://gateway.thegraph.com/api/<YOUR_API_KEY>/subgraphs/id/CtVkYCGXqSswH7EHqXNUCEtYuNbQq5cnUBHZaVQZfF5u",
}

// fetchTopPoolsFromSubgraph queries a GraphQL subgraph for the top N pools
// ordered by TVL. Returns raw pool records.
func fetchTopPoolsFromSubgraph(endpoint string, limit int) ([]graphPool, error) {
	query := fmt.Sprintf(`{
		pools(first: %d, orderBy: totalValueLockedUSD, orderDirection: desc, where: {totalValueLockedUSD_gt: "1000"}) {
			id
			token0 { id symbol decimals }
			token1 { id symbol decimals }
			totalValueLockedUSD
		}
	}`, limit)

	body, _ := json.Marshal(map[string]string{"query": query})
	resp, err := http.Post(endpoint, "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("subgraph HTTP %d: %s", resp.StatusCode, string(b)[:min(200, len(b))])
	}

	var result struct {
		Data struct {
			Pools []graphPool `json:"pools"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	if len(result.Errors) > 0 {
		return nil, fmt.Errorf("subgraph error: %s", result.Errors[0].Message)
	}
	return result.Data.Pools, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// backfillEVM pulls top pools from The Graph subgraphs and upserts them.
// The Graph data already contains token0/token1 in canonical on-chain order
// (lower address sort), so no extra token0()/token1() call is needed.
// We still fetch decimals from the RPC as the authoritative source.
func backfillEVM(db *sql.DB, rpcBSC, rpcBase *EVMRPCClient, dc *EVMDecimalCache) {
	type backfillJob struct {
		network   string
		dexID     string
		dexName   string
		poolType  string
		programID string // factory contract address
		rpc       *EVMRPCClient
	}

	jobs := []backfillJob{
		{"bsc",  "pancakeswap_v2",       "PancakeSwap V2",       "v2",   "0xca143ce32fe78f1f7019d7d551a6402fc5350c73", rpcBSC},
		{"bsc",  "pancakeswap_v3",       "PancakeSwap V3",       "v3",   "0x0bfbcf9fa4f9c56b0f40a671ad40e0805a091865", rpcBSC},
		{"bsc",  "uniswap_v3",           "Uniswap V3",           "v3",   "0xdb1d10011ad0ff90774d0c6bb92e5c5c8b4461f7", rpcBSC},
		{"bsc",  "pancakeswap_infinity", "PancakeSwap Infinity",  "clmm", infinityCLPoolManager,     rpcBSC},
		{"bsc",  "uniswap_v4",           "Uniswap V4",           "v4",   uniswapV4PoolManagerBSC,   rpcBSC},
		{"base", "uniswap_v3",           "Uniswap V3",           "v3",   "0x33128a8fc17869897dce68ed026d694621f6fdfd", rpcBase},
		{"base", "aerodrome_slipstream", "Aerodrome Slipstream", "v3",   "0x5e7bb104d84c7cb9b682aac2f3d509f5f406809a", rpcBase},
		{"base", "aerodrome",            "Aerodrome",            "v2",   "0x420dd381b31aef6683db6b902084cb0ffece40da", rpcBase},
		{"base", "uniswap_v4",           "Uniswap V4",           "v4",   uniswapV4PoolManagerBase, rpcBase},
	}

	for _, job := range jobs {
		key := job.network + ":" + job.dexID
		endpoint, ok := subgraphEndpoints[key]
		if !ok {
			continue
		}

		// Skip if the API key placeholder is still in the URL
		if strings.Contains(endpoint, "<YOUR_API_KEY>") {
			log.Printf("[evm-backfill] %s/%s — subgraph API key not configured, skipping backfill (live indexer will catch new pools)", job.network, job.dexID)
			continue
		}

		log.Printf("[evm-backfill] fetching top pools for %s/%s…", job.network, job.dexID)
		pools, err := fetchTopPoolsFromSubgraph(endpoint, 200)
		if err != nil {
			log.Printf("[evm-backfill] %s/%s subgraph error: %v", job.network, job.dexID, err)
			continue
		}
		log.Printf("[evm-backfill] %s/%s — got %d pools from subgraph", job.network, job.dexID, len(pools))

		saved := 0
		for _, p := range pools {
			if p.ID == "" || p.Token0.ID == "" || p.Token1.ID == "" {
				continue
			}

			// Use on-chain RPC as authoritative source for decimals.
			// Subgraph decimals can be wrong (especially for non-standard tokens).
			dec0 := dc.GetOrFetch(job.network, p.Token0.ID, job.rpc)
			dec1 := dc.GetOrFetch(job.network, p.Token1.ID, job.rpc)

			sym0 := p.Token0.Symbol
			sym1 := p.Token1.Symbol
			if sym0 == "" {
				sym0 = job.rpc.getTokenSymbol(p.Token0.ID)
			}
			if sym1 == "" {
				sym1 = job.rpc.getTokenSymbol(p.Token1.ID)
			}

			r := EVMPoolRecord{
				Network:     job.network,
				DexName:     job.dexName,
				DexID:       job.dexID,
				PoolType:    job.poolType,
				PoolAddress: strings.ToLower(p.ID),
				Token0:      strings.ToLower(p.Token0.ID),
				Token1:      strings.ToLower(p.Token1.ID),
				Decimals0:   dec0,
				Decimals1:   dec1,
				Symbol0:     sym0,
				Symbol1:     sym1,
				ProgramID:   job.programID,
			}
			if err := upsertEVMPool(db, r); err != nil {
				log.Printf("[evm-backfill] upsert failed for %s: %v", p.ID[:10], err)
			} else {
				saved++
			}
			time.Sleep(10 * time.Millisecond)
		}
		log.Printf("[evm-backfill] %s/%s — saved %d/%d pools", job.network, job.dexID, saved, len(pools))
	}
}

// ── EVM pool record + upsert ─────────────────────────────────────────────────

type EVMPoolRecord struct {
	Network     string
	DexName     string
	DexID       string
	PoolType    string
	PoolAddress string
	Token0      string // on-chain canonical token0 (lower address)
	Token1      string // on-chain canonical token1
	Decimals0   int
	Decimals1   int
	Symbol0     string
	Symbol1     string
	ProgramID   string // factory contract address that created this pool
}

func upsertEVMPool(db *sql.DB, r EVMPoolRecord) error {
	baseTokenJSON, _ := json.Marshal(map[string]interface{}{
		"address":  r.Token0,
		"symbol":   r.Symbol0,
		"decimals": r.Decimals0,
	})
	quoteTokenJSON, _ := json.Marshal(map[string]interface{}{
		"address":  r.Token1,
		"symbol":   r.Symbol1,
		"decimals": r.Decimals1,
	})

	pairID := fmt.Sprintf("%s_%s", r.Network, r.PoolAddress)
	poolName := fmt.Sprintf("%s/%s", r.Symbol0, r.Symbol1)
	dex := strings.ToLower(strings.Split(r.DexName, " ")[0])

	_, err := db.Exec(`
		INSERT INTO pairs (
			id, network, pair_address, dex_id, dex_name, pool_type, program_id,
			base_token, quote_token, base_symbol, quote_symbol, dex,
			pool_address, base_token_decimals, quote_token_decimals,
			pool_name, indexed_at
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,
			$8,$9,$10,$11,$12,
			$13,$14,$15,
			$16, NOW()
		)
		ON CONFLICT (id) DO UPDATE SET
			base_token           = EXCLUDED.base_token,
			quote_token          = EXCLUDED.quote_token,
			base_symbol          = EXCLUDED.base_symbol,
			quote_symbol         = EXCLUDED.quote_symbol,
			base_token_decimals  = EXCLUDED.base_token_decimals,
			quote_token_decimals = EXCLUDED.quote_token_decimals,
			pool_name            = EXCLUDED.pool_name,
			dex_name             = EXCLUDED.dex_name,
			dex_id               = EXCLUDED.dex_id,
			pool_type            = EXCLUDED.pool_type,
			program_id           = EXCLUDED.program_id,
			indexed_at           = NOW()
	`,
		pairID, r.Network, r.PoolAddress, r.DexID, r.DexName, r.PoolType, r.ProgramID,
		string(baseTokenJSON), string(quoteTokenJSON), r.Symbol0, r.Symbol1, dex,
		r.PoolAddress, r.Decimals0, r.Decimals1,
		poolName,
	)
	return err
}

// ── Live EVM indexer (eth_subscribe logs) ────────────────────────────────────

type EVMLiveIndexer struct {
	network    string
	wsEndpoint string
	factories  []EVMFactory
	rpc        *EVMRPCClient
	dc         *EVMDecimalCache
	db         *sql.DB
}

func (li *EVMLiveIndexer) Run() {
	for {
		if err := li.connectAndListen(); err != nil {
			log.Printf("[evm-live/%s] disconnected: %v — reconnecting in 5s", li.network, err)
			time.Sleep(5 * time.Second)
		}
	}
}

func (li *EVMLiveIndexer) connectAndListen() error {
	log.Printf("[evm-live/%s] connecting to %s", li.network, li.wsEndpoint)
	conn, _, err := websocket.DefaultDialer.Dial(li.wsEndpoint, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	// Subscribe to PoolCreated events from all factories in one filter.
	// Each factory emits on a different topic0 but we subscribe per-factory
	// so the address filter keeps things clean.
	factoryByAddr := make(map[string]EVMFactory)
	var addresses []string
	for _, f := range li.factories {
		if f.Network != li.network {
			continue
		}
		addr := strings.ToLower(f.Address)
		factoryByAddr[addr] = f
		addresses = append(addresses, addr)
	}
	if len(addresses) == 0 {
		log.Printf("[evm-live/%s] no factories configured — exiting", li.network)
		return nil
	}

	subReq := map[string]interface{}{
		"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe",
		"params": []interface{}{
			"logs",
			map[string]interface{}{
				"address": addresses,
				"topics": [][]string{
					{topicV2PairCreated, topicV3PoolCreated, topicInfinityInitialize, topicUniswapV4Initialize},
				},
			},
		},
	}
	if err := conn.WriteJSON(subReq); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}

	for {
		conn.SetReadDeadline(time.Now().Add(120 * time.Second))
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}

		var raw map[string]json.RawMessage
		if err := json.Unmarshal(msg, &raw); err != nil {
			continue
		}

		method, _ := raw["method"]
		if string(method) != `"eth_subscription"` {
			// Could be subscription confirmation
			if _, ok := raw["result"]; ok {
				log.Printf("[evm-live/%s] subscription confirmed", li.network)
			}
			continue
		}

		var params struct {
			Result struct {
				Address string   `json:"address"`
				Topics  []string `json:"topics"`
				Data    string   `json:"data"`
			} `json:"result"`
		}
		if err := json.Unmarshal(raw["params"], &params); err != nil {
			continue
		}

		addr := strings.ToLower(params.Result.Address)
		factory, ok := factoryByAddr[addr]
		if !ok {
			continue
		}

		topics := params.Result.Topics
		if len(topics) < 3 {
			continue
		}

		// Extract token0, token1, and pool address based on factory kind:
		//   V2  — topics[1]=token0, topics[2]=token1, pool in data word 0
		//   V3  — topics[1]=token0, topics[2]=token1, pool in data word 1
		//   Infinity — topics[1]=poolId(bytes32), topics[2]=currency0, topics[3]=currency1
		//              pool_address = topics[1] (the bytes32 pool key hash)
		var token0, token1, poolAddr string

		switch factory.Kind {
		case factoryKindInfinity:
			if len(topics) < 4 {
				continue
			}
			// poolId is topics[1] as bytes32 — keep the full 0x+64hex as the pool_address
			poolAddr = strings.ToLower(topics[1])
			token0 = decodeAddress(topics[2])
			token1 = decodeAddress(topics[3])
		default:
			token0 = decodeAddress(topics[1])
			token1 = decodeAddress(topics[2])
			poolAddr = strings.ToLower(decodePoolAddressFromData(params.Result.Data, factory.Kind))
		}

		if token0 == "" || token1 == "" || poolAddr == "" {
			continue
		}

		dec0 := li.dc.GetOrFetch(li.network, token0, li.rpc)
		dec1 := li.dc.GetOrFetch(li.network, token1, li.rpc)
		sym0 := li.rpc.getTokenSymbol(token0)
		sym1 := li.rpc.getTokenSymbol(token1)

		r := EVMPoolRecord{
			Network:     li.network,
			DexName:     factory.DexName,
			DexID:       factory.DexID,
			PoolType:    factory.PoolType,
			PoolAddress: poolAddr,
			Token0:      token0,
			Token1:      token1,
			Decimals0:   dec0,
			Decimals1:   dec1,
			Symbol0:     sym0,
			Symbol1:     sym1,
			ProgramID:   strings.ToLower(factory.Address),
		}
		if err := upsertEVMPool(li.db, r); err != nil {
			log.Printf("[evm-live/%s] upsert failed for %s: %v", li.network, poolAddr[:10], err)
		} else {
			log.Printf("[evm-live/%s] ✅ new pool %s — %s/%s (%s dec %d/%d)",
				li.network, poolAddr[:10], sym0, sym1, factory.DexName, dec0, dec1)
		}
	}
}

// decodePoolAddressFromData extracts the pool contract address from the
// non-indexed event data field.
// V2 PairCreated data: [pair_address (32 bytes), all_pairs_length (32 bytes)]
// V3 PoolCreated data: [tick_spacing (32 bytes), pool_address (32 bytes)]
func decodePoolAddressFromData(data string, kind factoryKind) string {
	hex := strings.TrimPrefix(data, "0x")
	switch kind {
	case factoryKindV2:
		// First word is the pair address
		if len(hex) < 64 {
			return ""
		}
		return decodeAddress(hex[:64])
	case factoryKindV3:
		// Second word is the pool address (first is tickSpacing padded to 32 bytes)
		if len(hex) < 128 {
			return ""
		}
		return decodeAddress(hex[64:128])
	}
	return ""
}

// ── EVM indexer entry point ───────────────────────────────────────────────────

// StartEVMIndexer launches backfill + live indexing for BSC and Base.
// Called from main() after Solana backfill completes.
func StartEVMIndexer(db *sql.DB, bscHTTP, baseHTTP, bscWS, baseWSEndpoint string) {
	rpcBSC := newEVMRPC(bscHTTP)
	rpcBase := newEVMRPC(baseHTTP)
	dc := newEVMDecimalCache()

	// Apply The Graph API key to subgraph endpoints if configured
	apiKey := os.Getenv("THEGRAPH_API_KEY")
	if apiKey != "" {
		for k, v := range subgraphEndpoints {
			subgraphEndpoints[k] = strings.ReplaceAll(v, "<YOUR_API_KEY>", apiKey)
		}
		log.Printf("[evm] The Graph API key configured — EVM subgraph backfill enabled")
	} else {
		log.Printf("[evm] THEGRAPH_API_KEY not set — EVM subgraph backfill disabled (live eth_subscribe active)")
	}

	// Backfill top pools from The Graph
	log.Println("[evm] starting backfill from subgraphs…")
	backfillEVM(db, rpcBSC, rpcBase, dc)
	log.Println("[evm] backfill complete — starting live indexers")

	// Live indexers — one per chain, runs forever in background
	bscIndexer := &EVMLiveIndexer{
		network:    "bsc",
		wsEndpoint: bscWS,
		factories:  evmFactories,
		rpc:        rpcBSC,
		dc:         dc,
		db:         db,
	}
	baseIndexer := &EVMLiveIndexer{
		network:    "base",
		wsEndpoint: baseWSEndpoint,
		factories:  evmFactories,
		rpc:        rpcBase,
		dc:         dc,
		db:         db,
	}

	go bscIndexer.Run()
	go baseIndexer.Run()

	log.Println("[evm] live indexers running for BSC and Base")
}
