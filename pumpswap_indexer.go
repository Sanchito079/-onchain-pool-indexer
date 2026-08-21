// pumpswap_indexer.go — PumpSwap (Pump.fun AMM) pool indexer
//
// Program: pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA
//
// Strategy:
//   Backfill  — getProgramAccounts with dataSize=261 (Pool account size).
//               Reads base_mint and quote_mint directly from pool state bytes.
//               Decimals come straight from the event fields — no extra RPC call needed
//               for the live path. Backfill fetches them from the SPL mint account.
//
//   Live      — logsSubscribe on the PumpSwap program.
//               CreatePoolEvent discriminator: [177, 49, 12, 210, 160, 118, 167, 116]
//               The event already contains base_mint_decimals and quote_mint_decimals
//               as u8 fields — zero extra RPC calls needed per new pool.
//
// CreatePoolEvent layout (after 8-byte discriminator):
//   [8:16]   timestamp         i64
//   [16:18]  index             u16
//   [18:50]  creator           pubkey
//   [50:82]  base_mint         pubkey   ← token0
//   [82:114] quote_mint        pubkey   ← token1
//   [114]    base_mint_decimals  u8
//   [115]    quote_mint_decimals u8
//   ... (amounts, lp fields, etc.)
//   [173:205] pool             pubkey   ← pool address
//
// Pool account layout (after 8-byte discriminator):
//   [8]      pool_bump         u8
//   [9:11]   index             u16
//   [11:43]  creator           pubkey
//   [43:75]  base_mint         pubkey
//   [75:107] quote_mint        pubkey
//   ...                        (remaining fields not needed for indexing)
package main

import (
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

const (
	ProgramPumpSwap = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"

	// Pool account dataSize = 8 (disc) + 1 + 2 + 32*7 + 8 + 1 + 1 + 16 = 261
	pumpSwapPoolDataSize = 261

	// Pool account offsets (after 8-byte discriminator)
	pumpPoolBaseMintOffset  = 43  // base_mint pubkey
	pumpPoolQuoteMintOffset = 75  // quote_mint pubkey

	// CreatePoolEvent offsets (after 8-byte discriminator)
	pumpEventBaseMintOffset     = 50  // base_mint pubkey (32 bytes)
	pumpEventQuoteMintOffset    = 82  // quote_mint pubkey (32 bytes)
	pumpEventBaseDecimalsOffset = 114 // base_mint_decimals u8
	pumpEventQuoteDecimalsOffset = 115 // quote_mint_decimals u8
	pumpEventPoolOffset         = 173 // pool pubkey (32 bytes)
	pumpEventMinLen             = 205 // minimum blob length to read pool address
)

// CreatePoolEvent discriminator: sha256("event:CreatePoolEvent")[0:8]
// Verified from pump_amm.json IDL: [177, 49, 12, 210, 160, 118, 167, 116]
var pumpSwapCreatePoolDisc = [8]byte{177, 49, 12, 210, 160, 118, 167, 116}

// Pool account discriminator: [241, 154, 109, 4, 17, 177, 109, 188]
// Used to double-check backfill accounts before parsing.
var pumpSwapPoolDisc = [8]byte{241, 154, 109, 4, 17, 177, 109, 188}

// ── Backfill ──────────────────────────────────────────────────────────────────

// backfillPumpSwap scans all existing PumpSwap pool accounts via
// getProgramAccounts and upserts them with correct on-chain data.
func backfillPumpSwap(db *sql.DB, rpc *RPCClient, dc *DecimalCache) {
	log.Println("[backfill/pumpswap] starting PumpSwap pool scan…")

	accounts, err := rpc.getProgramAccounts(ProgramPumpSwap, pumpSwapPoolDataSize)
	if err != nil {
		log.Printf("[backfill/pumpswap] getProgramAccounts failed: %v", err)
		return
	}
	log.Printf("[backfill/pumpswap] found %d pool accounts", len(accounts))

	saved, skipped := 0, 0
	for _, acct := range accounts {
		if len(acct.Account.Data) == 0 {
			skipped++
			continue
		}
		data, err := base64.StdEncoding.DecodeString(acct.Account.Data[0])
		if err != nil || len(data) < pumpPoolQuoteMintOffset+32 {
			skipped++
			continue
		}

		// Verify discriminator
		var disc [8]byte
		copy(disc[:], data[0:8])
		if disc != pumpSwapPoolDisc {
			skipped++
			continue
		}

		baseMint := encodeBase58(data[pumpPoolBaseMintOffset : pumpPoolBaseMintOffset+32])
		quoteMint := encodeBase58(data[pumpPoolQuoteMintOffset : pumpPoolQuoteMintOffset+32])
		if baseMint == "" || quoteMint == "" {
			skipped++
			continue
		}

		dec0 := dc.GetOrFetch(baseMint, rpc)
		dec1 := dc.GetOrFetch(quoteMint, rpc)

		r := PoolRecord{
			PoolAddress: acct.Pubkey,
			Mint0:       baseMint,
			Mint1:       quoteMint,
			Decimals0:   dec0,
			Decimals1:   dec1,
			Symbol0:     symbolForMint(baseMint),
			Symbol1:     symbolForMint(quoteMint),
			DexName:     "PumpSwap",
			Network:     "solana",
			PoolType:    "amm",
			ProgramID:   ProgramPumpSwap,
		}
		if err := upsertPool(db, r); err != nil {
			log.Printf("[backfill/pumpswap] upsert failed for %s: %v", acct.Pubkey[:8], err)
			skipped++
		} else {
			saved++
		}
		time.Sleep(5 * time.Millisecond)
	}
	log.Printf("[backfill/pumpswap] done — saved=%d skipped=%d", saved, skipped)
}

// ── Live indexer ──────────────────────────────────────────────────────────────

// PumpSwapLiveIndexer subscribes to the PumpSwap program via logsSubscribe
// and indexes new pools as they are created. The CreatePoolEvent contains
// base_mint, quote_mint, their decimals, AND the pool address — so no extra
// RPC calls are needed per new pool.
type PumpSwapLiveIndexer struct {
	wsEndpoint string
	db         *sql.DB
	rpc        *RPCClient
	dc         *DecimalCache
}

func (li *PumpSwapLiveIndexer) Run() {
	for {
		if err := li.connectAndListen(); err != nil {
			log.Printf("[live/pumpswap] disconnected: %v — reconnecting in 5s", err)
			time.Sleep(5 * time.Second)
		}
	}
}

func (li *PumpSwapLiveIndexer) connectAndListen() error {
	log.Printf("[live/pumpswap] connecting to %s", li.wsEndpoint)
	conn, _, err := websocket.DefaultDialer.Dial(li.wsEndpoint, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	sub := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "logsSubscribe",
		"params": []interface{}{
			map[string]interface{}{
				"mentions": []string{ProgramPumpSwap},
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

func (li *PumpSwapLiveIndexer) handleMessage(raw []byte) {
	var msg logsMsg
	if err := json.Unmarshal(raw, &msg); err != nil {
		return
	}
	if msg.ID != nil {
		if len(msg.Error) > 0 {
			log.Printf("[live/pumpswap] subscription error: %s", msg.Error)
		} else {
			log.Printf("[live/pumpswap] subscribed to PumpSwap program logs")
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

	for _, line := range v.Value.Logs {
		if !strings.Contains(line, "Program data: ") {
			continue
		}
		b64 := strings.TrimSpace(strings.SplitN(line, "Program data:", 2)[1])
		data, err := base64.StdEncoding.DecodeString(b64)
		if err != nil || len(data) < pumpEventMinLen {
			continue
		}

		// Check discriminator
		var disc [8]byte
		copy(disc[:], data[0:8])
		if disc != pumpSwapCreatePoolDisc {
			continue
		}

		// Parse fields directly from event — no extra RPC needed
		baseMint := encodeBase58(data[pumpEventBaseMintOffset : pumpEventBaseMintOffset+32])
		quoteMint := encodeBase58(data[pumpEventQuoteMintOffset : pumpEventQuoteMintOffset+32])
		poolAddr := encodeBase58(data[pumpEventPoolOffset : pumpEventPoolOffset+32])

		if baseMint == "" || quoteMint == "" || poolAddr == "" {
			continue
		}

		// Decimals are embedded in the event — no RPC call needed
		dec0 := int(data[pumpEventBaseDecimalsOffset])
		dec1 := int(data[pumpEventQuoteDecimalsOffset])

		// Sanity check decimals (valid SPL range: 0–9)
		if dec0 < 0 || dec0 > 18 || dec1 < 0 || dec1 > 18 {
			log.Printf("[live/pumpswap] suspicious decimals for %s: base=%d quote=%d — fetching from chain",
				poolAddr[:8], dec0, dec1)
			dec0 = li.dc.GetOrFetch(baseMint, li.rpc)
			dec1 = li.dc.GetOrFetch(quoteMint, li.rpc)
		} else {
			// Cache the decimals so later lookups don't hit RPC
			li.dc.Set(baseMint, dec0)
			li.dc.Set(quoteMint, dec1)
		}

		// Parse index (u16 at offset 16) for logging
		index := binary.LittleEndian.Uint16(data[16:18])

		r := PoolRecord{
			PoolAddress: poolAddr,
			Mint0:       baseMint,
			Mint1:       quoteMint,
			Decimals0:   dec0,
			Decimals1:   dec1,
			Symbol0:     symbolForMint(baseMint),
			Symbol1:     symbolForMint(quoteMint),
			DexName:     "PumpSwap",
			Network:     "solana",
			PoolType:    "amm",
			ProgramID:   ProgramPumpSwap,
		}

		if err := upsertPool(li.db, r); err != nil {
			log.Printf("[live/pumpswap] upsert failed for pool %s: %v", poolAddr[:8], err)
		} else {
			log.Printf("[live/pumpswap] ✅ new pool #%d %s — %s/%s (dec %d/%d tx=%s)",
				index, poolAddr[:8], r.Symbol0, r.Symbol1, dec0, dec1, v.Value.Signature[:8])
		}
		break // one CreatePoolEvent per tx
	}
}
