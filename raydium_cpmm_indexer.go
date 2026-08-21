// raydium_cpmm_indexer.go — Raydium CPMM (Constant Product AMM) pool indexer
//
// Program: CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C
//
// PoolState layout (bytemuckunsafe, verified from raydium_cp_swap.json IDL):
//   [0:8]    discriminator  [247,237,227,245,215,195,222,70]
//   [8:40]   amm_config     pubkey
//   [40:72]  pool_creator   pubkey
//   [72:104] token_0_vault  pubkey  ← SPL token account holding token_0
//   [104:136] token_1_vault pubkey  ← SPL token account holding token_1
//   [136:168] lp_mint       pubkey
//   [168:200] token_0_mint  pubkey  ← base token mint address
//   [200:232] token_1_mint  pubkey  ← quote token mint address
//
//   Live      — logsSubscribe on the CPMM program.
//               SwapEvent discriminator shared, but CPMM also emits an
//               InitializePool-like event. We use the SwapEvent (same disc
//               as CLMM) which is fired on every interaction — for live
//               pool creation we watch for new pool addresses in swap events.
//               Actually CPMM fires a LpChangeEvent on pool creation with
//               discriminator [121,163,205,201,57,218,117,60] but that event
//               doesn't contain mint addresses. So for live we use the
//               approach of reading the pool account when we see a new pool.
package main

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

const (
	ProgramRaydiumCPMM = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C"

	// PoolState account offsets (bytemuckunsafe, verified from IDL):
	//   [0:8]    discriminator
	//   [8:40]   amm_config     pubkey
	//   [40:72]  pool_creator   pubkey
	//   [72:104] token_0_vault  pubkey  (NOT mint — verified from raydium_cp_swap.json IDL)
	//   [104:136] token_1_vault pubkey
	//   [136:168] lp_mint       pubkey
	//   [168:200] token_0_mint  pubkey  ← base token
	//   [200:232] token_1_mint  pubkey  ← quote token
	cpmmToken0Offset = 168
	cpmmToken1Offset = 200

	// dataSlice: start at token_0_mint, fetch both mints (64 bytes)
	cpmmSliceOffset = 168
	cpmmSliceLength = 64 // 32 (token0_mint) + 32 (token1_mint)

	cpmmMinSlicedLen = 64
)

// PoolState discriminator: [247, 237, 227, 245, 215, 195, 222, 70]
// Verified from raydium_cp.txt IDL
var cpmmPoolStateDisc = [8]byte{247, 237, 227, 245, 215, 195, 222, 70}

// backfillRaydiumCPMM scans all existing Raydium CPMM pool accounts.
// Uses discriminator memcmp filter (no dataSize) since CPMM accounts have
// variable sizes and mainnet-beta returns 0 with any dataSize filter.
// mainnet-beta.solana.com allows CPMM getProgramAccounts (455k+ pools).
func backfillRaydiumCPMM(db *sql.DB, rpc *RPCClient, dc *DecimalCache) {
	log.Println("[backfill/cpmm] starting Raydium CPMM pool scan…")

	discBytes := make([]byte, 8)
	copy(discBytes, cpmmPoolStateDisc[:])
	discBase64 := base64.StdEncoding.EncodeToString(discBytes)

	// Use mainnet-beta specifically for CPMM — it allows this call
	// while blocking CLMM. We override the client's endpoint temporarily.
	cpmmRPC := newBackfillRPCClient("https://api.mainnet-beta.solana.com")

	result, err := cpmmRPC.call("getProgramAccounts", []interface{}{
		ProgramRaydiumCPMM,
		map[string]interface{}{
			"encoding": "base64",
			"dataSlice": map[string]interface{}{
				"offset": cpmmSliceOffset,
				"length": cpmmSliceLength,
			},
			"filters": []map[string]interface{}{
				{
					"memcmp": map[string]interface{}{
						"offset":   0,
						"bytes":    discBase64,
						"encoding": "base64",
					},
				},
			},
		},
	})
	if err != nil {
		log.Printf("[backfill/cpmm] getProgramAccounts failed: %v", err)
		return
	}

	var accounts []accountEntry
	if err := json.Unmarshal(result, &accounts); err != nil {
		log.Printf("[backfill/cpmm] parse failed: %v", err)
		return
	}
	log.Printf("[backfill/cpmm] found %d CPMM pool accounts", len(accounts))

	saved, skipped := 0, 0
	for _, acct := range accounts {
		if len(acct.Account.Data) == 0 {
			skipped++
			continue
		}
		data, err := base64.StdEncoding.DecodeString(acct.Account.Data[0])
		if err != nil || len(data) < cpmmMinSlicedLen {
			skipped++
			continue
		}

		// With dataSlice(offset=168, length=64):
		// data[0:32]  = token_0_mint (was at account offset 168)
		// data[32:64] = token_1_mint (was at account offset 200)
		token0 := encodeBase58(data[0:32])
		token1 := encodeBase58(data[32:64])
		if token0 == "" || token1 == "" {
			skipped++
			continue
		}

		dec0 := dc.GetOrFetch(token0, rpc)
		dec1 := dc.GetOrFetch(token1, rpc)

		r := PoolRecord{
			PoolAddress: acct.Pubkey,
			Mint0:       token0,
			Mint1:       token1,
			Decimals0:   dec0,
			Decimals1:   dec1,
			Symbol0:     symbolForMint(token0),
			Symbol1:     symbolForMint(token1),
			DexName:     "Raydium CPMM",
			Network:     "solana",
			PoolType:    "amm",
			ProgramID:   ProgramRaydiumCPMM,
		}
		if err := upsertPool(db, r); err != nil {
			log.Printf("[backfill/cpmm] upsert failed for %s: %v", acct.Pubkey[:8], err)
			skipped++
		} else {
			saved++
		}
		time.Sleep(5 * time.Millisecond)
	}
	log.Printf("[backfill/cpmm] done — saved=%d skipped=%d", saved, skipped)
}

// ── Live indexer ──────────────────────────────────────────────────────────────
// CPMM emits a LpChangeEvent [121,163,205,201,57,218,117,60] when pool is
// initialized but it only contains vault/LP amounts — no mint addresses.
// For live pool creation we watch for new CPMM pool addresses appearing in
// logsSubscribe and then fetch the pool account to get the mints.

type RaydiumCPMMLiveIndexer struct {
	wsEndpoint string
	db         *sql.DB
	rpc        *RPCClient
	dc         *DecimalCache
	// seen tracks pool addresses we've already indexed to avoid duplicate fetches
	seen map[string]bool
}

func (li *RaydiumCPMMLiveIndexer) Run() {
	for {
		if err := li.connectAndListen(); err != nil {
			log.Printf("[live/cpmm] disconnected: %v — reconnecting in 5s", err)
			time.Sleep(5 * time.Second)
		}
	}
}

func (li *RaydiumCPMMLiveIndexer) connectAndListen() error {
	log.Printf("[live/cpmm] connecting to %s", li.wsEndpoint)
	conn, _, err := websocket.DefaultDialer.Dial(li.wsEndpoint, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	sub := map[string]interface{}{
		"jsonrpc": "2.0", "id": 1,
		"method": "logsSubscribe",
		"params": []interface{}{
			map[string]interface{}{"mentions": []string{ProgramRaydiumCPMM}},
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

func (li *RaydiumCPMMLiveIndexer) handleMessage(raw []byte) {
	var msg logsMsg
	if err := json.Unmarshal(raw, &msg); err != nil {
		return
	}
	if msg.ID != nil {
		if len(msg.Error) > 0 {
			log.Printf("[live/cpmm] subscription error: %s", msg.Error)
		} else {
			log.Printf("[live/cpmm] subscribed to Raydium CPMM program logs")
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

	// Scan logs for "Program log: Instruction: CreatePool" or pool address mentions
	// CPMM logs the pool address in the instruction context
	for _, line := range v.Value.Logs {
		// Look for "initialize" instruction indicator
		if !strings.Contains(line, "initialize") && !strings.Contains(line, "Initialize") {
			continue
		}
		// When a new CPMM pool is created, the transaction signature is unique.
		// Fetch the transaction to get the new pool account address.
		// For simplicity: any "initialize" log in a CPMM transaction means a new pool.
		// We fetch the account data for the pool that appears in the transaction.
		go li.fetchNewPoolFromTx(v.Value.Signature, v.Value.Logs)
		return
	}
}

func (li *RaydiumCPMMLiveIndexer) fetchNewPoolFromTx(sig string, logs []string) {
	// The CPMM pool address is mentioned in the logs as an account key.
	// We look for "Program log: ... pool" mentions or parse account keys.
	// Simplest reliable approach: scan logs for a base58 pubkey that looks like a pool
	// by checking if it's owned by the CPMM program.
	for _, line := range logs {
		// Skip common noise lines
		if strings.HasPrefix(line, "Program log:") || strings.HasPrefix(line, "Program data:") {
			continue
		}
		// Lines like "Program CPMMoo8L... invoke [1]" tell us the program is being called
		// The actual pool pubkey appears in "Program log: pool: <PUBKEY>" style logs
		if strings.Contains(line, "pool:") {
			parts := strings.Fields(line)
			for _, p := range parts {
				if len(p) >= 32 && len(p) <= 44 && p != ProgramRaydiumCPMM {
					li.tryIndexPool(p)
				}
			}
		}
	}
}

func (li *RaydiumCPMMLiveIndexer) tryIndexPool(poolAddr string) {
	if li.seen[poolAddr] {
		return
	}
	li.seen[poolAddr] = true

	// Fetch pool account to verify it's a CPMM pool and get mints
	result, err := li.rpc.call("getAccountInfo", []interface{}{
		poolAddr,
		map[string]interface{}{
			"encoding":  "base64",
			"dataSlice": map[string]interface{}{"offset": cpmmSliceOffset, "length": cpmmSliceLength},
		},
	})
	if err != nil {
		return
	}

	var info struct {
		Value *struct {
			Owner string   `json:"owner"`
			Data  []string `json:"data"`
		} `json:"value"`
	}
	if err := json.Unmarshal(result, &info); err != nil || info.Value == nil {
		return
	}
	if info.Value.Owner != ProgramRaydiumCPMM {
		return
	}
	if len(info.Value.Data) == 0 {
		return
	}

	data, err := base64.StdEncoding.DecodeString(info.Value.Data[0])
	if err != nil || len(data) < cpmmMinSlicedLen {
		return
	}

	token0 := encodeBase58(data[0:32])
	token1 := encodeBase58(data[32:64])
	if token0 == "" || token1 == "" {
		return
	}

	dec0 := li.dc.GetOrFetch(token0, li.rpc)
	dec1 := li.dc.GetOrFetch(token1, li.rpc)

	r := PoolRecord{
		PoolAddress: poolAddr,
		Mint0:       token0,
		Mint1:       token1,
		Decimals0:   dec0,
		Decimals1:   dec1,
		Symbol0:     symbolForMint(token0),
		Symbol1:     symbolForMint(token1),
		DexName:     "Raydium CPMM",
		Network:     "solana",
		PoolType:    "amm",
		ProgramID:   ProgramRaydiumCPMM,
	}

	if err := upsertPool(li.db, r); err != nil {
		log.Printf("[live/cpmm] upsert failed for %s: %v", poolAddr[:8], err)
	} else {
		log.Printf("[live/cpmm] ✅ new pool %s — %s/%s dec:%d/%d",
			poolAddr[:8], r.Symbol0, r.Symbol1, dec0, dec1)
	}
}
