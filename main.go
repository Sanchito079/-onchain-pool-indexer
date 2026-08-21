// main.go — on-chain pool indexer
//
// Flags:
//   -migrate   run the DB schema migration then exit
//   (no flag)  run the full backfill + live indexer
package main

import (
	"bytes"
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

// ── Program IDs ──────────────────────────────────────────────────────────────

const (
	ProgramRaydiumCLMM = "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK"
)

// PoolCreatedEvent discriminator: sha256("event:PoolCreatedEvent")[0:8]
// Verified from raydium_clmm.json IDL: [25, 94, 75, 47, 112, 99, 53, 63]
var poolCreatedEventDisc = [8]byte{25, 94, 75, 47, 112, 99, 53, 63}

// PoolCreatedEvent layout (after 8-byte discriminator, all little-endian):
//   [8:40]   token_mint_0  (pubkey, 32 bytes)
//   [40:72]  token_mint_1  (pubkey, 32 bytes)
//   [72:74]  tick_spacing  (u16, 2 bytes)
//   [74:106] pool_state    (pubkey, 32 bytes)
//   [106:122] sqrt_price_x64 (u128, 16 bytes)
//   [122:126] tick          (i32, 4 bytes)
//   [126:158] token_vault_0 (pubkey, 32 bytes)
//   [158:190] token_vault_1 (pubkey, 32 bytes)
const (
	eventMint0Offset  = 8
	eventMint1Offset  = 40
	eventTickSpacing  = 72
	eventPoolOffset   = 74
	eventSqrtOffset   = 106
	eventTickOffset   = 122
	eventVault0Offset = 126
	eventVault1Offset = 158
	eventMinLen       = 190
)

// Raydium CLMM PoolState offsets (dataSize = 665, repr(C, packed)):
//   [73:105]  token_mint_0 (32 bytes)
//   [105:137] token_mint_1 (32 bytes)
//   [233]     mint_decimals_0 (u8)
//   [234]     mint_decimals_1 (u8)
const (
	stateMint0Offset = 73
	stateMint1Offset = 105
	stateDataSize    = 665
)

// ── Types ────────────────────────────────────────────────────────────────────

type Config struct {
	DatabaseURL       string
	RPCEndpoint       string // Solana HTTP RPC — used for live decimal lookups (WS fallback)
	BackfillRPC       string // Solana HTTP RPC for getProgramAccounts — must allow it (public/paid)
	WSEndpoint        string // Solana WS RPC
	BSCEndpoint       string // BSC HTTP RPC
	BSCWSEndpoint     string // BSC WS RPC
	BaseEndpoint      string // Base HTTP RPC
	BaseWSEndpoint    string // Base WS RPC
}

type PoolRecord struct {
	PoolAddress string
	Mint0       string // on-chain token_mint_0 (canonical order)
	Mint1       string // on-chain token_mint_1 (canonical order)
	Decimals0   int
	Decimals1   int
	Symbol0     string
	Symbol1     string
	DexName     string
	Network     string
	PoolType    string // clmm | dlmm | amm | v3 | v2
	ProgramID   string // on-chain program ID that owns this pool
	BinStep     int    // Meteora DLMM bin step (0 for non-DLMM pools)
}

// ── RPC Client ───────────────────────────────────────────────────────────────

type RPCClient struct {
	Endpoint  string
	fallbacks []string // tried in order when Endpoint fails
	mu        sync.Mutex
	client    *http.Client
}

func newRPCClient(endpoint string) *RPCClient {
	return &RPCClient{
		Endpoint: endpoint,
		client:   &http.Client{Timeout: 30 * time.Second},
	}
}

// newBackfillRPCClient creates an RPC client with fallback endpoints and a
// long timeout for getProgramAccounts (which can take 60-180s for large programs).
// It tries the configured endpoint first, then rotates through public RPCs
// that are known to allow getProgramAccounts.
func newBackfillRPCClient(primary string) *RPCClient {
	fallbacks := []string{
		"https://api.mainnet-beta.solana.com",
		"https://rpc.ankr.com/solana",
		"https://solana-mainnet.rpc.extrnode.com",
	}
	// Remove primary from fallbacks to avoid duplicates
	var filtered []string
	for _, f := range fallbacks {
		if f != primary {
			filtered = append(filtered, f)
		}
	}
	return &RPCClient{
		Endpoint:  primary,
		fallbacks: filtered,
		client:    &http.Client{Timeout: 300 * time.Second},
	}
}

// newLiveRPCClient creates an RPC client for live decimal lookups with a
// short timeout and a public fallback for cases where the primary is rate-limited.
func newLiveRPCClient(primary string) *RPCClient {
	fallbacks := []string{
		"https://api.mainnet-beta.solana.com",
		"https://solana-rpc.publicnode.com",
	}
	var filtered []string
	for _, f := range fallbacks {
		if f != primary {
			filtered = append(filtered, f)
		}
	}
	return &RPCClient{
		Endpoint:  primary,
		fallbacks: filtered,
		client:    &http.Client{Timeout: 10 * time.Second},
	}
}
func (c *RPCClient) call(method string, params []interface{}) (json.RawMessage, error) {
	endpoints := append([]string{c.Endpoint}, c.fallbacks...)
	var lastErr error
	for _, endpoint := range endpoints {
		result, err := c.callEndpoint(endpoint, method, params)
		if err != nil {
			if len(c.fallbacks) > 0 {
				log.Printf("[rpc] %s failed on %s: %v — trying next",
					method, shortEndpoint(endpoint), err)
			}
			lastErr = err
			continue
		}
		if endpoint != c.Endpoint {
			log.Printf("[rpc] %s succeeded on fallback %s", method, shortEndpoint(endpoint))
		}
		return result, nil
	}
	return nil, lastErr
}

func (c *RPCClient) callEndpoint(endpoint, method string, params []interface{}) (json.RawMessage, error) {
	body, _ := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0", "id": 1,
		"method": method, "params": params,
	})
	req, err := http.NewRequest("POST", endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	// Detect non-JSON responses (HTML error pages, rate-limit responses)
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || (trimmed[0] != '{' && trimmed[0] != '[') {
		preview := string(trimmed)
		if len(preview) > 120 {
			preview = preview[:120]
		}
		return nil, fmt.Errorf("non-JSON response (rate-limited/blocked): %q", preview)
	}

	var result struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(trimmed, &result); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}
	if result.Error != nil {
		return nil, fmt.Errorf("RPC %s error %d: %s", method, result.Error.Code, result.Error.Message)
	}
	return result.Result, nil
}

func shortEndpoint(ep string) string {
	ep = strings.TrimPrefix(ep, "https://")
	ep = strings.TrimPrefix(ep, "http://")
	if idx := strings.Index(ep, "/"); idx > 0 {
		ep = ep[:idx]
	}
	if len(ep) > 35 {
		return ep[:35] + "…"
	}
	return ep
}

// getMintDecimals reads the SPL mint account and returns the decimals field (byte 44).
func (c *RPCClient) getMintDecimals(mint string) (int, error) {
	result, err := c.call("getAccountInfo", []interface{}{
		mint,
		map[string]interface{}{"encoding": "base64"},
	})
	if err != nil {
		return 0, err
	}

	var info struct {
		Value *struct {
			Data []string `json:"data"`
		} `json:"value"`
	}
	if err := json.Unmarshal(result, &info); err != nil {
		return 0, err
	}
	if info.Value == nil || len(info.Value.Data) == 0 {
		return 0, fmt.Errorf("mint account %s not found", mint[:8])
	}

	data, err := base64.StdEncoding.DecodeString(info.Value.Data[0])
	if err != nil {
		return 0, err
	}
	// SPL Mint layout: decimals at byte 44
	if len(data) < 45 {
		return 0, fmt.Errorf("mint data too short (%d bytes)", len(data))
	}
	return int(data[44]), nil
}

type accountEntry struct {
	Pubkey  string `json:"pubkey"`
	Account struct {
		Data []string `json:"data"`
	} `json:"account"`
}

// getProgramAccounts fetches all accounts owned by a program matching the given filters.
func (c *RPCClient) getProgramAccounts(programID string, dataSize int) ([]accountEntry, error) {
	result, err := c.call("getProgramAccounts", []interface{}{
		programID,
		map[string]interface{}{
			"encoding": "base64",
			"filters": []map[string]interface{}{
				{"dataSize": dataSize},
			},
		},
	})
	if err != nil {
		return nil, err
	}
	var accounts []accountEntry
	if err := json.Unmarshal(result, &accounts); err != nil {
		return nil, fmt.Errorf("parse getProgramAccounts: %w", err)
	}
	return accounts, nil
}

// getProgramAccountsSliced fetches only the bytes we need from each account
// (using dataSlice), dramatically reducing response size for large programs
// like Raydium CLMM which has 18,000+ accounts at 665 bytes each.
// offset = first byte we need, length = how many bytes to return.
func (c *RPCClient) getProgramAccountsSliced(programID string, dataSize, sliceOffset, sliceLength int) ([]accountEntry, error) {
	result, err := c.call("getProgramAccounts", []interface{}{
		programID,
		map[string]interface{}{
			"encoding": "base64",
			"dataSlice": map[string]interface{}{
				"offset": sliceOffset,
				"length": sliceLength,
			},
			"filters": []map[string]interface{}{
				{"dataSize": dataSize},
			},
		},
	})
	if err != nil {
		return nil, err
	}
	var accounts []accountEntry
	if err := json.Unmarshal(result, &accounts); err != nil {
		return nil, fmt.Errorf("parse getProgramAccounts sliced: %w", err)
	}
	return accounts, nil
}

// ── Decimal cache ─────────────────────────────────────────────────────────────
// Avoids re-fetching decimals for the same mint across many pools.

type DecimalCache struct {
	mu    sync.RWMutex
	cache map[string]int
}

func newDecimalCache() *DecimalCache {
	return &DecimalCache{cache: make(map[string]int)}
}

func (dc *DecimalCache) Get(mint string) (int, bool) {
	dc.mu.RLock()
	defer dc.mu.RUnlock()
	v, ok := dc.cache[mint]
	return v, ok
}

func (dc *DecimalCache) Set(mint string, decimals int) {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	dc.cache[mint] = decimals
}

// GetOrFetch returns cached decimals or fetches from RPC and caches the result.
// Falls back to 9 (SOL default) on error, but retries with a short delay first
// to handle brand-new mints that haven't propagated to the RPC node yet.
func (dc *DecimalCache) GetOrFetch(mint string, rpc *RPCClient) int {
	if v, ok := dc.Get(mint); ok {
		return v
	}
	dec, err := rpc.getMintDecimals(mint)
	if err != nil {
		// New mints may not have propagated yet — wait 2s and retry once
		time.Sleep(2 * time.Second)
		dec, err = rpc.getMintDecimals(mint)
		if err != nil {
			log.Printf("[decimal-cache] fetch failed for %s...: %v — defaulting to 9", mint[:8], err)
			dec = 9
		}
	}
	dc.Set(mint, dec)
	return dec
}

// ── Symbol helpers ────────────────────────────────────────────────────────────

// knownSymbols maps well-known mint addresses to their ticker symbol.
// This avoids any external API calls for the most common tokens.
var knownSymbols = map[string]string{
	"So11111111111111111111111111111111111111112":  "SOL",
	"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": "USDC",
	"Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB": "USDT",
	"DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263": "BONK",
	"7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs": "ETH",
	"mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So":  "mSOL",
	"bSo13r4TkiE4KumL71LsHTPpL2euBYLFx6h9HP3piy1":  "bSOL",
	"JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN":  "JUP",
	"WENWENvqqNya429ubCdR81ZmD69brwQaaBYY6p3LCpk":  "WEN",
	"3NZ9JMVBmGAqocybic2c7LQCJScmgsAZ6vQqTDzcqmJh": "WBTC",
	"HZ1JovNiVvGrDwdc4GXEgx6iqjFvmQogynEAFpZpppW4": "PYTH",
	"rndrizKT3MK1iimdxRdWabcF7Zg7AR5T4nud4EkHBof":  "RNDR",
}

func symbolForMint(mint string) string {
	if s, ok := knownSymbols[mint]; ok {
		return s
	}
	// Use first 4 chars of mint as fallback — better than empty string
	if len(mint) >= 4 {
		return mint[:4] + "…"
	}
	return mint
}

// ── DB upsert ─────────────────────────────────────────────────────────────────

// upsertPool writes or overwrites a pool record.
// On conflict it always updates on-chain fields (token addresses, decimals,
// symbols, program_id, pool_type) so GeckoTerminal's wrong data gets corrected.
func upsertPool(db *sql.DB, r PoolRecord) error {
	baseTokenJSON, _ := json.Marshal(map[string]interface{}{
		"address":  r.Mint0,
		"symbol":   r.Symbol0,
		"decimals": r.Decimals0,
	})
	quoteTokenJSON, _ := json.Marshal(map[string]interface{}{
		"address":  r.Mint1,
		"symbol":   r.Symbol1,
		"decimals": r.Decimals1,
	})

	pairID := fmt.Sprintf("%s_%s", r.Network, r.PoolAddress)
	poolName := fmt.Sprintf("%s/%s", r.Symbol0, r.Symbol1)
	if r.BinStep > 0 {
		poolName = fmt.Sprintf("%s/%s (binStep=%d)", r.Symbol0, r.Symbol1, r.BinStep)
	}
	dexID := strings.ToLower(strings.ReplaceAll(r.DexName, " ", "_"))

	_, err := db.Exec(`
		INSERT INTO pairs (
			id, network, pair_address, dex_id, dex_name, pool_type, program_id,
			base_token, quote_token, base_symbol, quote_symbol, dex,
			pool_address, base_token_decimals, quote_token_decimals,
			pool_name, indexed_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7,
			$8, $9, $10, $11, $12,
			$13, $14, $15,
			$16, NOW()
		)
		ON CONFLICT (id) DO UPDATE SET
			base_token            = EXCLUDED.base_token,
			quote_token           = EXCLUDED.quote_token,
			base_symbol           = EXCLUDED.base_symbol,
			quote_symbol          = EXCLUDED.quote_symbol,
			base_token_decimals   = EXCLUDED.base_token_decimals,
			quote_token_decimals  = EXCLUDED.quote_token_decimals,
			pool_name             = EXCLUDED.pool_name,
			dex_name              = EXCLUDED.dex_name,
			dex_id                = EXCLUDED.dex_id,
			pool_type             = EXCLUDED.pool_type,
			program_id            = EXCLUDED.program_id,
			indexed_at            = NOW()
	`,
		pairID, r.Network, r.PoolAddress, dexID, r.DexName, r.PoolType, r.ProgramID,
		string(baseTokenJSON), string(quoteTokenJSON), r.Symbol0, r.Symbol1,
		strings.ToLower(strings.Split(r.DexName, " ")[0]),
		r.PoolAddress, r.Decimals0, r.Decimals1,
		poolName,
	)
	return err
}

// ── Backfill ──────────────────────────────────────────────────────────────────

// backfillCLMM scans all existing Raydium CLMM pool accounts via
// getProgramAccounts and upserts them with correct on-chain data.
// Uses dataSlice to fetch only the 64 bytes containing both mint addresses
// (offset 73, length 64), reducing response size by ~90% so public RPCs
// don't silently truncate the results for this large program.
func backfillCLMM(db *sql.DB, rpc *RPCClient, dc *DecimalCache) {
	log.Println("[backfill] starting Raydium CLMM scan…")
	// Only fetch the 64 bytes we need: mint0 @ 73 (32 bytes) + mint1 @ 105 (32 bytes)
	// This cuts response size from ~12MB to ~1.1MB for ~18k pools.
	const sliceOffset = stateMint0Offset // 73
	const sliceLength = 64              // 32 (mint0) + 32 (mint1)
	accounts, err := rpc.getProgramAccountsSliced(ProgramRaydiumCLMM, stateDataSize, sliceOffset, sliceLength)
	if err != nil {
		log.Printf("[backfill] getProgramAccounts failed: %v", err)
		return
	}
	log.Printf("[backfill] found %d CLMM pool accounts", len(accounts))

	saved, skipped := 0, 0
	for _, acct := range accounts {
		if len(acct.Account.Data) == 0 {
			skipped++
			continue
		}
		data, err := base64.StdEncoding.DecodeString(acct.Account.Data[0])
		if err != nil || len(data) < 64 {
			skipped++
			continue
		}

		// With dataSlice(offset=73, length=64):
		// data[0:32]  = token_mint_0  (was at account offset 73)
		// data[32:64] = token_mint_1  (was at account offset 105)
		mint0 := encodeBase58(data[0:32])
		mint1 := encodeBase58(data[32:64])
		if mint0 == "" || mint1 == "" {
			skipped++
			continue
		}

		dec0 := dc.GetOrFetch(mint0, rpc)
		dec1 := dc.GetOrFetch(mint1, rpc)

		r := PoolRecord{
			PoolAddress: acct.Pubkey,
			Mint0:       mint0,
			Mint1:       mint1,
			Decimals0:   dec0,
			Decimals1:   dec1,
			Symbol0:     symbolForMint(mint0),
			Symbol1:     symbolForMint(mint1),
			DexName:     "Raydium CLMM",
			Network:     "solana",
			PoolType:    "clmm",
			ProgramID:   ProgramRaydiumCLMM,
		}
		if err := upsertPool(db, r); err != nil {
			log.Printf("[backfill] upsert failed for %s: %v", acct.Pubkey[:8], err)
			skipped++
		} else {
			saved++
		}
		// Small rate-limit pause to avoid overwhelming the RPC node.
		time.Sleep(5 * time.Millisecond)
	}
	log.Printf("[backfill] done — saved=%d skipped=%d", saved, skipped)
}

// ── Live indexer (logsSubscribe) ─────────────────────────────────────────────

type LiveIndexer struct {
	wsEndpoint string
	db         *sql.DB
	rpc        *RPCClient
	dc         *DecimalCache
}

func (li *LiveIndexer) Run() {
	for {
		if err := li.connectAndListen(); err != nil {
			log.Printf("[live] disconnected: %v — reconnecting in 5s", err)
			time.Sleep(5 * time.Second)
		}
	}
}

func (li *LiveIndexer) connectAndListen() error {
	log.Printf("[live] connecting to %s", li.wsEndpoint)
	conn, _, err := websocket.DefaultDialer.Dial(li.wsEndpoint, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	// Subscribe to all logs that mention the CLMM program.
	sub := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "logsSubscribe",
		"params": []interface{}{
			map[string]interface{}{
				"mentions": []string{ProgramRaydiumCLMM},
			},
			map[string]interface{}{"commitment": "confirmed"},
		},
	}
	if err := conn.WriteJSON(sub); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}

	for {
		conn.SetReadDeadline(time.Now().Add(120 * time.Second))
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		li.handleMessage(msg)
	}
}

type logsMsg struct {
	Method string `json:"method"`
	Params *struct {
		Result *struct {
			Value *struct {
				Signature string   `json:"signature"`
				Logs      []string `json:"logs"`
				Err       any      `json:"err"`
			} `json:"value"`
		} `json:"result"`
	} `json:"params"`
	ID    *int            `json:"id"`
	Error json.RawMessage `json:"error"`
}

func (li *LiveIndexer) handleMessage(raw []byte) {
	var msg logsMsg
	if err := json.Unmarshal(raw, &msg); err != nil {
		return
	}
	// Subscription confirmation
	if msg.ID != nil {
		if len(msg.Error) > 0 {
			log.Printf("[live] subscription error: %s", msg.Error)
		} else {
			log.Printf("[live] subscribed to CLMM program logs")
		}
		return
	}
	if msg.Method != "logsNotification" || msg.Params == nil {
		return
	}
	v := msg.Params.Result
	if v == nil || v.Value == nil || v.Value.Err != nil {
		return
	}

	// Scan all "Program data:" lines in the transaction logs.
	for _, line := range v.Value.Logs {
		if !strings.Contains(line, "Program data: ") {
			continue
		}
		b64 := strings.TrimSpace(strings.SplitN(line, "Program data:", 2)[1])
		data, err := base64.StdEncoding.DecodeString(b64)
		if err != nil || len(data) < eventMinLen {
			continue
		}

		// Check discriminator
		var disc [8]byte
		copy(disc[:], data[0:8])
		if disc != poolCreatedEventDisc {
			continue
		}

		// Parse PoolCreatedEvent fields
		mint0 := encodeBase58(data[eventMint0Offset : eventMint0Offset+32])
		mint1 := encodeBase58(data[eventMint1Offset : eventMint1Offset+32])
		poolState := encodeBase58(data[eventPoolOffset : eventPoolOffset+32])

		if mint0 == "" || mint1 == "" || poolState == "" {
			continue
		}

		// tick_spacing from event (not strictly needed for DB but useful for logging)
		tickSpacing := binary.LittleEndian.Uint16(data[eventTickSpacing : eventTickSpacing+2])

		dec0 := li.dc.GetOrFetch(mint0, li.rpc)
		dec1 := li.dc.GetOrFetch(mint1, li.rpc)

		r := PoolRecord{
			PoolAddress: poolState,
			Mint0:       mint0,
			Mint1:       mint1,
			Decimals0:   dec0,
			Decimals1:   dec1,
			Symbol0:     symbolForMint(mint0),
			Symbol1:     symbolForMint(mint1),
			DexName:     "Raydium CLMM",
			Network:     "solana",
			PoolType:    "clmm",
			ProgramID:   ProgramRaydiumCLMM,
		}

		if err := upsertPool(li.db, r); err != nil {
			log.Printf("[live] upsert failed for pool %s: %v", poolState[:8], err)
		} else {
			log.Printf("[live] ✅ new CLMM pool %s — %s/%s (dec %d/%d tickSpacing=%d tx=%s)",
				poolState[:8], r.Symbol0, r.Symbol1, dec0, dec1, tickSpacing, v.Value.Signature[:8])
		}
		// A single tx can only create one pool, so stop after first match.
		break
	}
}

// ── main ──────────────────────────────────────────────────────────────────────

func main() {
	migrateOnly := flag.Bool("migrate", false, "run DB schema migration and exit")
	checkDB     := flag.Bool("check",   false, "print DB health report and exit")
	fixCPMM     := flag.Bool("fix-cpmm", false, "delete corrupted CPMM rows (wrong decimals) so backfill rewrites them")
	flag.Parse()

	cfg := loadConfig()

	db, err := sql.Open("postgres", cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("db open: %v", err)
	}
	defer db.Close()

	connected := false
	for attempt := 1; attempt <= 5; attempt++ {
		if err := db.Ping(); err != nil {
			log.Printf("[db] ping attempt %d/5 failed: %v — retrying in 3s", attempt, err)
			time.Sleep(3 * time.Second)
			continue
		}
		log.Println("[db] connected")
		connected = true
		break
	}
	if !connected {
		log.Fatal("[db] could not connect after 5 attempts")
	}

	if *migrateOnly {
		runMigration(db)
		return
	}

	if *checkDB {
		runDBCheck(db)
		return
	}

	if *fixCPMM {
		log.Println("[fix-cpmm] deleting CPMM rows with corrupted decimals (>9)…")
		res, err := db.Exec(`
			DELETE FROM pairs
			WHERE program_id = 'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C'
			  AND (base_token_decimals > 38 OR quote_token_decimals > 38)
		`)
		if err != nil {
			log.Fatalf("[fix-cpmm] delete failed: %v", err)
		}
		n, _ := res.RowsAffected()
		log.Printf("[fix-cpmm] deleted %d corrupted CPMM rows", n)

		// Also nuke any CPMM row where the base or quote token address looks
		// like a vault account (44-char base58) rather than a mint — those
		// were indexed with the old wrong offsets and have wrong token addresses.
		// Re-running backfill will re-index them all correctly.
		log.Println("[fix-cpmm] done — run .\\indexer.exe to re-backfill CPMM pools with correct data")
		return
	}

	rpc := newLiveRPCClient(cfg.RPCEndpoint)
	backfillRPC := newBackfillRPCClient(cfg.BackfillRPC)
	dc := newDecimalCache()

	// BACKFILL_SKIP is a comma-separated list of programs to skip backfilling.
	// Useful when a program's backfill is already confirmed working and you
	// want to test others without waiting.
	// Example in .env: BACKFILL_SKIP=pumpswap,cpmm,damm
	skipSet := make(map[string]bool)
	if skip := strings.TrimSpace(os.Getenv("BACKFILL_SKIP")); skip != "" {
		for _, s := range strings.Split(skip, ",") {
			skipSet[strings.ToLower(strings.TrimSpace(s))] = true
		}
		log.Printf("[backfill] skipping: %v", skip)
	}
	shouldBackfill := func(name string) bool {
		return !skipSet[strings.ToLower(name)]
	}

	// ── Solana CLMM ──────────────────────────────────────────────────────────
	if shouldBackfill("clmm") {
		log.Println("[solana] backfilling Raydium CLMM pools…")
		backfillCLMM(db, backfillRPC, dc)
	}
	go (&LiveIndexer{wsEndpoint: cfg.WSEndpoint, db: db, rpc: rpc, dc: dc}).Run()

	// ── Solana PumpSwap ───────────────────────────────────────────────────────
	if shouldBackfill("pumpswap") {
		log.Println("[solana] backfilling PumpSwap pools…")
		backfillPumpSwap(db, backfillRPC, dc)
	}
	go (&PumpSwapLiveIndexer{wsEndpoint: cfg.WSEndpoint, db: db, rpc: rpc, dc: dc}).Run()

	// ── Solana Raydium CPMM ───────────────────────────────────────────────────
	if shouldBackfill("cpmm") {
		log.Println("[solana] backfilling Raydium CPMM pools…")
		backfillRaydiumCPMM(db, backfillRPC, dc)
	}
	go (&RaydiumCPMMLiveIndexer{wsEndpoint: cfg.WSEndpoint, db: db, rpc: rpc, dc: dc, seen: make(map[string]bool)}).Run()

	// ── Solana Meteora DLMM ───────────────────────────────────────────────────
	if shouldBackfill("dlmm") {
		log.Println("[solana] backfilling Meteora DLMM pools…")
		backfillMeteoraDLMM(db, backfillRPC, dc)
	}
	go (&MeteoraDLMMIndexer{wsEndpoint: cfg.WSEndpoint, db: db, rpc: rpc, dc: dc}).Run()

	// ── Solana Meteora DAMM V2 ────────────────────────────────────────────────
	if shouldBackfill("damm") {
		log.Println("[solana] backfilling Meteora DAMM V2 pools…")
		backfillMeteoraDammV2(db, backfillRPC, dc)
	}
	go (&MeteoraDammV2Indexer{wsEndpoint: cfg.WSEndpoint, db: db, rpc: rpc, dc: dc}).Run()

	// ── Solana Orca Whirlpool ─────────────────────────────────────────────────
	if shouldBackfill("orca") {
		log.Println("[solana] backfilling Orca Whirlpool pools…")
		backfillOrca(db, backfillRPC, dc)
	}
	go (&OrcaIndexer{wsEndpoint: cfg.WSEndpoint, db: db, rpc: rpc, dc: dc}).Run()

	// ── EVM (BSC + Base) ─────────────────────────────────────────────────────
	if cfg.BSCEndpoint != "" || cfg.BaseEndpoint != "" {
		go StartEVMIndexer(db,
			cfg.BSCEndpoint, cfg.BaseEndpoint,
			cfg.BSCWSEndpoint, cfg.BaseWSEndpoint,
		)
	} else {
		log.Println("[evm] BSC_RPC_ENDPOINT and BASE_RPC_ENDPOINT not set — skipping EVM indexing")
	}

	// Block forever — all work is done in goroutines.
	select {}
}

// runMigration applies the schema changes needed for on-chain indexing.
func runMigration(db *sql.DB) {
	steps := []struct {
		desc string
		sql  string
	}{
		{
			"add program_id column",
			`ALTER TABLE pairs ADD COLUMN IF NOT EXISTS program_id TEXT`,
		},
		{
			"comment program_id",
			`COMMENT ON COLUMN pairs.program_id IS 'On-chain program / factory contract that owns this pool. Solana: base58 program ID. EVM: lowercase factory address.'`,
		},
		{
			"add pool_type column",
			`ALTER TABLE pairs ADD COLUMN IF NOT EXISTS pool_type TEXT`,
		},
		{
			"comment pool_type",
			`COMMENT ON COLUMN pairs.pool_type IS 'Pool AMM type: clmm | v3 | v2 | bin. Written by the on-chain indexer.'`,
		},
		{
			"add dex_id column",
			`ALTER TABLE pairs ADD COLUMN IF NOT EXISTS dex_id TEXT`,
		},
		{
			"index program_id",
			`CREATE INDEX IF NOT EXISTS idx_pairs_program_id ON pairs(program_id)`,
		},
		{
			"index pool_type",
			`CREATE INDEX IF NOT EXISTS idx_pairs_pool_type ON pairs(pool_type)`,
		},
		{
			"truncate pairs (local dev only — skipped if SKIP_TRUNCATE=1)",
			`SELECT 1 WHERE $1::text != '1'`,
		},	}

	for _, step := range steps {
		log.Printf("[migration] %s…", step.desc)
		if _, err := db.Exec(step.sql); err != nil {
			log.Fatalf("[migration] FAILED — %s: %v", step.desc, err)
		}
		log.Printf("[migration] ✅ %s", step.desc)
	}

	// Verify
	var count int
	db.QueryRow("SELECT COUNT(*) FROM pairs").Scan(&count)
	log.Printf("[migration] pairs row count after TRUNCATE: %d", count)

	rows, err := db.Query(`
		SELECT column_name, data_type
		FROM information_schema.columns
		WHERE table_name = 'pairs'
		  AND column_name IN ('program_id', 'pool_type', 'dex_id')
		ORDER BY column_name
	`)
	if err == nil {
		defer rows.Close()
		log.Println("[migration] confirmed columns:")
		for rows.Next() {
			var col, dtype string
			rows.Scan(&col, &dtype)
			log.Printf("[migration]   %-25s %s", col, dtype)
		}
	}
	log.Println("[migration] done — database is ready for on-chain indexing")
}

// runDBCheck prints a full health report of the pairs table.
func runDBCheck(db *sql.DB) {	sep := strings.Repeat("─", 80)
	fmt.Println(sep)

	// 1. Total
	var total int
	db.QueryRow("SELECT COUNT(*) FROM pairs").Scan(&total)
	fmt.Printf("\n📊 Total pairs: %d\n", total)

	if total == 0 {
		fmt.Println("\n⚠️  Table is empty — run the indexer first: .\\indexer.exe")
		fmt.Println(sep)
		return
	}

	// 2. By network
	fmt.Println("\n📡 By network:")
	rows, _ := db.Query(`SELECT network, COUNT(*) FROM pairs GROUP BY network ORDER BY 2 DESC`)
	defer rows.Close()
	for rows.Next() {
		var n string; var c int
		rows.Scan(&n, &c)
		fmt.Printf("   %-12s %d\n", n, c)
	}

	// 3. By DEX
	fmt.Println("\n🏦 By DEX / pool_type / program_id:")
	rows2, _ := db.Query(`
		SELECT COALESCE(dex_name,'(null)'), COALESCE(pool_type,'(null)'),
		       COALESCE(LEFT(program_id,20),'(null)'), COUNT(*)
		FROM pairs
		GROUP BY dex_name, pool_type, program_id
		ORDER BY 4 DESC LIMIT 30`)
	defer rows2.Close()
	fmt.Printf("   %-28s %-8s %-22s %s\n", "dex_name", "type", "program_id (first 20)", "count")
	fmt.Println("   " + strings.Repeat("─", 70))
	for rows2.Next() {
		var dex, pt, prog string; var c int
		rows2.Scan(&dex, &pt, &prog, &c)
		fmt.Printf("   %-28s %-8s %-22s %d\n", dex, pt, prog, c)
	}

	// 4. Data quality
	fmt.Println("\n🔍 Data quality checks:")
	checks := []struct{ label, q string }{
		{"NULL pool_address",                    "SELECT COUNT(*) FROM pairs WHERE pool_address IS NULL OR pool_address=''"},
		{"NULL program_id",                      "SELECT COUNT(*) FROM pairs WHERE program_id IS NULL OR program_id=''"},
		{"NULL pool_type",                       "SELECT COUNT(*) FROM pairs WHERE pool_type IS NULL OR pool_type=''"},
		{"Solana rows with decimals=0 or 18",    "SELECT COUNT(*) FROM pairs WHERE network='solana' AND (base_token_decimals=0 OR base_token_decimals=18 OR quote_token_decimals=0 OR quote_token_decimals=18)"},
		{"NULL base_symbol",                     "SELECT COUNT(*) FROM pairs WHERE base_symbol IS NULL OR base_symbol=''"},
		{"base_token = quote_token (broken)",    "SELECT COUNT(*) FROM pairs WHERE base_token::text = quote_token::text"},
	}
	allGood := true
	for _, c := range checks {
		var n int
		db.QueryRow(c.q).Scan(&n)
		icon := "✅"
		if n > 0 { icon = "⚠️ "; allGood = false }
		fmt.Printf("   %s %-55s %d\n", icon, c.label, n)
	}
	if allGood {
		fmt.Println("\n   All checks passed — data looks clean!")
	}

	// 4b. CPMM-specific decimal breakdown
	fmt.Println("\n🔍 Raydium CPMM decimal breakdown:")
	var cpmmTotal, cpmmClean, cpmmCorrupted, cpmmZero int
	db.QueryRow(`
		SELECT
			COUNT(*),
			COUNT(*) FILTER (WHERE base_token_decimals BETWEEN 1 AND 38 AND quote_token_decimals BETWEEN 1 AND 38),
			COUNT(*) FILTER (WHERE base_token_decimals > 38 OR quote_token_decimals > 38),
			COUNT(*) FILTER (WHERE base_token_decimals = 0 OR quote_token_decimals = 0)
		FROM pairs WHERE program_id = 'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C'
	`).Scan(&cpmmTotal, &cpmmClean, &cpmmCorrupted, &cpmmZero)
	fmt.Printf("   Total CPMM pools:       %d\n", cpmmTotal)
	fmt.Printf("   Clean  (dec 1–38):      %d\n", cpmmClean)
	fmt.Printf("   Corrupted (dec >38):    %d\n", cpmmCorrupted)
	fmt.Printf("   Zero decimals:          %d\n", cpmmZero)
	if cpmmCorrupted == 0 && cpmmZero == 0 {
		fmt.Println("   ✅ All CPMM decimals are correct!")
	} else {
		fmt.Printf("   ⚠️  %d rows still bad — run .\\indexer.exe -fix-cpmm then re-backfill\n", cpmmCorrupted+cpmmZero)
	}

	// 4c. Sample of any remaining bad CPMM rows
	if cpmmCorrupted > 0 {
		fmt.Println("\n   Sample corrupted CPMM rows:")
		badRows, _ := db.Query(`
			SELECT pool_address, base_symbol, quote_symbol, base_token_decimals, quote_token_decimals
			FROM pairs
			WHERE program_id = 'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C'
			  AND (base_token_decimals > 38 OR quote_token_decimals > 38)
			LIMIT 5
		`)
		if badRows != nil {
			defer badRows.Close()
			for badRows.Next() {
				var pool, bs, qs string
				var bd, qd int
				badRows.Scan(&pool, &bs, &qs, &bd, &qd)
				fmt.Printf("   pool:%s  %s/%s  dec:%d/%d\n", pool[:14], bs, qs, bd, qd)
			}
		}
	}

	// 5. One sample row per DEX
	fmt.Println("\n📋 Sample row per DEX (most recent):")
	rows3, _ := db.Query(`
		SELECT DISTINCT ON (dex_name)
			network, dex_name, pool_type,
			LEFT(pool_address,14), base_symbol, quote_symbol,
			base_token_decimals, quote_token_decimals,
			LEFT(COALESCE(program_id,''),20),
			TO_CHAR(indexed_at,'YYYY-MM-DD HH24:MI:SS')
		FROM pairs
		WHERE pool_address IS NOT NULL
		ORDER BY dex_name, indexed_at DESC NULLS LAST`)
	defer rows3.Close()
	fmt.Println()
	for rows3.Next() {
		var net, dex, pt, pool, bs, qs, prog, ts string
		var bd, qd int
		rows3.Scan(&net, &dex, &pt, &pool, &bs, &qs, &bd, &qd, &prog, &ts)
		fmt.Printf("   [%-6s] %-24s %-6s  %s/%s  dec:%d/%d  pool:%s…  prog:%s…  %s\n",
			net, dex, pt, bs, qs, bd, qd, pool, prog, ts)
	}

	// 6. 10 most recent
	fmt.Println("\n🕐 10 most recently indexed:")
	rows4, _ := db.Query(`
		SELECT network, dex_name, base_symbol, quote_symbol,
		       base_token_decimals, quote_token_decimals,
		       LEFT(pool_address,14),
		       TO_CHAR(indexed_at,'HH24:MI:SS')
		FROM pairs ORDER BY indexed_at DESC NULLS LAST LIMIT 10`)
	defer rows4.Close()
	for rows4.Next() {
		var net, dex, bs, qs, pool, ts string
		var bd, qd int
		rows4.Scan(&net, &dex, &bs, &qs, &bd, &qd, &pool, &ts)
		fmt.Printf("   [%-6s] %-24s  %s/%s  dec:%d/%d  %s…  %s\n",
			net, dex, bs, qs, bd, qd, pool, ts)
	}

	fmt.Println("\n" + sep)
}

func loadConfig() Config {
	// Load .env file from the working directory if it exists.
	// Variables already set in the environment take precedence.
	if err := godotenv.Load(); err != nil {
		// Not fatal — .env is optional when running in prod with real env vars
		log.Println("[config] no .env file found, using environment variables")
	} else {
		log.Println("[config] loaded .env")
	}
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("DATABASE_URL required")
	}
	rpc := os.Getenv("SOLANA_RPC_ENDPOINT")
	if rpc == "" {
		rpc = "https://api.mainnet-beta.solana.com"
	}
	ws := os.Getenv("SOLANA_WS_ENDPOINT")
	if ws == "" {
		ws = strings.Replace(rpc, "https://", "wss://", 1)
		ws = strings.Replace(ws, "http://", "ws://", 1)
	}

	// Backfill RPC must allow getProgramAccounts.
	// Chainstack free plan blocks it — use a public endpoint instead.
	// Falls back through: env var → publicnode → mainnet-beta.
	backfillRPC := os.Getenv("SOLANA_BACKFILL_RPC_ENDPOINT")
	if backfillRPC == "" {
		backfillRPC = "https://solana-rpc.publicnode.com"
	}

	bscHTTP := os.Getenv("BSC_RPC_ENDPOINT")
	if bscHTTP == "" {
		bscHTTP = os.Getenv("RPC_ENDPOINT_BSC") // fall back to price-fetcher var name
	}
	bscWS := os.Getenv("BSC_WS_ENDPOINT")
	if bscWS == "" && bscHTTP != "" {
		bscWS = strings.Replace(bscHTTP, "https://", "wss://", 1)
		bscWS = strings.Replace(bscWS, "http://", "ws://", 1)
	}

	baseHTTP := os.Getenv("BASE_RPC_ENDPOINT")
	if baseHTTP == "" {
		baseHTTP = os.Getenv("RPC_ENDPOINT_BASE")
	}
	baseWS := os.Getenv("BASE_WS_ENDPOINT")
	if baseWS == "" && baseHTTP != "" {
		baseWS = strings.Replace(baseHTTP, "https://", "wss://", 1)
		baseWS = strings.Replace(baseWS, "http://", "ws://", 1)
	}

	return Config{
		DatabaseURL:    dbURL,
		RPCEndpoint:    rpc,
		BackfillRPC:    backfillRPC,
		WSEndpoint:     ws,
		BSCEndpoint:    bscHTTP,
		BSCWSEndpoint:  bscWS,
		BaseEndpoint:   baseHTTP,
		BaseWSEndpoint: baseWS,
	}
}

// ── Base58 encoding ───────────────────────────────────────────────────────────

func encodeBase58(input []byte) string {
	const alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
	if len(input) == 0 {
		return ""
	}
	zeros := 0
	for zeros < len(input) && input[zeros] == 0 {
		zeros++
	}
	n := new(big.Int).SetBytes(input)
	if n.Sign() == 0 {
		return strings.Repeat("1", zeros)
	}
	base := big.NewInt(58)
	rem := new(big.Int)
	var enc []byte
	for n.Sign() > 0 {
		n.DivMod(n, base, rem)
		enc = append(enc, alphabet[rem.Int64()])
	}
	for i, j := 0, len(enc)-1; i < j; i, j = i+1, j-1 {
		enc[i], enc[j] = enc[j], enc[i]
	}
	return strings.Repeat("1", zeros) + string(enc)
}
