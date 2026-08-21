// meteora_damm_indexer.go — Meteora CP-AMM (DAMM V2) pool indexer
//
// Program: cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG
//
// Strategy:
//   Backfill  — getProgramAccounts filtered by Pool account discriminator
//               [241,154,109,4,17,177,109,188] at offset 0.
//               token_a_mint at offset 168, token_b_mint at offset 200
//               (verified from cp_amm.json IDL bytemuck C-repr layout:
//                8 disc + 160 PoolFeesStruct = 168 → token_a_mint).
//
//   Live      — logsSubscribe on the CP-AMM program.
//               EvtInitializePool discriminator: [228,50,246,85,203,66,134,37]
//
//               EvtInitializePool layout (after 8-byte discriminator):
//                 [8:40]   pool         pubkey ← pool address
//                 [40:72]  token_a_mint pubkey ← base token
//                 [72:104] token_b_mint pubkey ← quote token
//                 (remaining fields variable-length, not needed)
//
//               Decimals are fetched once per mint and cached.
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
	ProgramMeteoraDammV2 = "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG"

	// Pool account offsets — bytemuck C-repr, verified from cp_amm.json IDL:
	//   8   discriminator
	//   +160 PoolFeesStruct  → 168
	//   +32  token_a_mint    → 200
	//   +32  token_b_mint
	dammAccountTokenAOffset = 168
	dammAccountTokenBOffset = 200
	dammMinAccountLen       = dammAccountTokenBOffset + 32 // 232

	// EvtInitializePool event offsets (after 8-byte discriminator)
	dammEventPoolOffset   = 8
	dammEventTokenAOffset = 40
	dammEventTokenBOffset = 72
	dammEventMinLen       = 104 // need to read at least token_b_mint end
)

// EvtInitializePool event discriminator — verified from cp_amm.json
var dammInitPoolDisc = [8]byte{228, 50, 246, 85, 203, 66, 134, 37}

// Pool account discriminator — verified from cp_amm.json
var dammPoolAccountDisc = [8]byte{241, 154, 109, 4, 17, 177, 109, 188}

// ── Backfill ──────────────────────────────────────────────────────────────────

func backfillMeteoraDammV2(db *sql.DB, rpc *RPCClient, dc *DecimalCache) {
	log.Println("[backfill/meteora-damm-v2] starting Pool account scan…")

	// Filter by discriminator bytes at offset 0
	discBytes := make([]byte, 8)
	copy(discBytes, dammPoolAccountDisc[:])
	discBase64 := base64.StdEncoding.EncodeToString(discBytes)

	result, err := rpc.call("getProgramAccounts", []interface{}{
		ProgramMeteoraDammV2,
		map[string]interface{}{
			"encoding": "base64",
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
		log.Printf("[backfill/meteora-damm-v2] getProgramAccounts failed: %v", err)
		return
	}

	var accounts []accountEntry
	if err := json.Unmarshal(result, &accounts); err != nil {
		log.Printf("[backfill/meteora-damm-v2] parse failed: %v", err)
		return
	}
	log.Printf("[backfill/meteora-damm-v2] found %d Pool accounts", len(accounts))

	saved, skipped := 0, 0
	for _, acct := range accounts {
		if len(acct.Account.Data) == 0 {
			skipped++
			continue
		}
		data, err := base64.StdEncoding.DecodeString(acct.Account.Data[0])
		if err != nil || len(data) < dammMinAccountLen {
			skipped++
			continue
		}

		tokenA := encodeBase58(data[dammAccountTokenAOffset : dammAccountTokenAOffset+32])
		tokenB := encodeBase58(data[dammAccountTokenBOffset : dammAccountTokenBOffset+32])
		if tokenA == "" || tokenB == "" {
			skipped++
			continue
		}

		dec0 := dc.GetOrFetch(tokenA, rpc)
		dec1 := dc.GetOrFetch(tokenB, rpc)

		r := PoolRecord{
			PoolAddress: acct.Pubkey,
			Mint0:       tokenA,
			Mint1:       tokenB,
			Decimals0:   dec0,
			Decimals1:   dec1,
			Symbol0:     symbolForMint(tokenA),
			Symbol1:     symbolForMint(tokenB),
			DexName:     "Meteora DAMM V2",
			Network:     "solana",
			PoolType:    "amm",
			ProgramID:   ProgramMeteoraDammV2,
		}
		if err := upsertPool(db, r); err != nil {
			log.Printf("[backfill/meteora-damm-v2] upsert failed for %s: %v", acct.Pubkey[:8], err)
			skipped++
		} else {
			saved++
		}
		time.Sleep(5 * time.Millisecond)
	}
	log.Printf("[backfill/meteora-damm-v2] done — saved=%d skipped=%d", saved, skipped)
}

// ── Live indexer ──────────────────────────────────────────────────────────────

type MeteoraDammV2Indexer struct {
	wsEndpoint string
	db         *sql.DB
	rpc        *RPCClient
	dc         *DecimalCache
}

func (li *MeteoraDammV2Indexer) Run() {
	for {
		if err := li.connectAndListen(); err != nil {
			log.Printf("[live/meteora-damm-v2] disconnected: %v — reconnecting in 5s", err)
			time.Sleep(5 * time.Second)
		}
	}
}

func (li *MeteoraDammV2Indexer) connectAndListen() error {
	log.Printf("[live/meteora-damm-v2] connecting to %s", li.wsEndpoint)
	conn, _, err := websocket.DefaultDialer.Dial(li.wsEndpoint, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	sub := map[string]interface{}{
		"jsonrpc": "2.0", "id": 1,
		"method": "logsSubscribe",
		"params": []interface{}{
			map[string]interface{}{"mentions": []string{ProgramMeteoraDammV2}},
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

func (li *MeteoraDammV2Indexer) handleMessage(raw []byte) {
	var msg logsMsg
	if err := json.Unmarshal(raw, &msg); err != nil {
		return
	}
	if msg.ID != nil {
		if len(msg.Error) > 0 {
			log.Printf("[live/meteora-damm-v2] subscription error: %s", msg.Error)
		} else {
			log.Printf("[live/meteora-damm-v2] subscribed to Meteora DAMM V2 program logs")
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
		if err != nil || len(data) < dammEventMinLen {
			continue
		}

		// Check discriminator
		var disc [8]byte
		copy(disc[:], data[0:8])
		if disc != dammInitPoolDisc {
			continue
		}

		// Parse fields — all fixed-offset pubkeys before the variable pool_fees
		pool := encodeBase58(data[dammEventPoolOffset : dammEventPoolOffset+32])
		tokenA := encodeBase58(data[dammEventTokenAOffset : dammEventTokenAOffset+32])
		tokenB := encodeBase58(data[dammEventTokenBOffset : dammEventTokenBOffset+32])

		if pool == "" || tokenA == "" || tokenB == "" {
			continue
		}

		dec0 := li.dc.GetOrFetch(tokenA, li.rpc)
		dec1 := li.dc.GetOrFetch(tokenB, li.rpc)

		r := PoolRecord{
			PoolAddress: pool,
			Mint0:       tokenA,
			Mint1:       tokenB,
			Decimals0:   dec0,
			Decimals1:   dec1,
			Symbol0:     symbolForMint(tokenA),
			Symbol1:     symbolForMint(tokenB),
			DexName:     "Meteora DAMM V2",
			Network:     "solana",
			PoolType:    "amm",
			ProgramID:   ProgramMeteoraDammV2,
		}

		if err := upsertPool(li.db, r); err != nil {
			log.Printf("[live/meteora-damm-v2] upsert failed for %s: %v", pool[:8], err)
		} else {
			log.Printf("[live/meteora-damm-v2] ✅ new pool %s — %s/%s dec:%d/%d tx=%s",
				pool[:8], r.Symbol0, r.Symbol1, dec0, dec1, v.Value.Signature[:8])
		}
		break // one EvtInitializePool per tx
	}
}
