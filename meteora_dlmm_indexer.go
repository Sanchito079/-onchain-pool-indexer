// meteora_dlmm_indexer.go — Meteora DLMM pool indexer
//
// Program: LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo
//
// Strategy:
//   Backfill  — getProgramAccounts with a discriminator memcmp filter
//               (bytes [33,11,49,98,181,101,177,13] at offset 0).
//               token_x_mint at offset 88, token_y_mint at offset 120
//               (verified from dlmm.json IDL bytemuck C-repr layout).
//               bin_step at offset 80 (u16 LE).
//
//   Live      — logsSubscribe on the DLMM program.
//               LbPairCreate discriminator: [185,74,252,125,27,215,188,111]
//
//               LbPairCreate event layout (after 8-byte discriminator):
//                 [8:40]   lb_pair    pubkey
//                 [40:42]  bin_step   u16
//                 [42:74]  token_x    pubkey
//                 [74:106] token_y    pubkey
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
	ProgramMeteoraDLMM = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo"

	// LbPair account offsets — bytemuck C-repr, verified from dlmm.json IDL:
	//   8  (discriminator)
	//   +32 (StaticParameters)  = 40
	//   +32 (VariableParameters) = 72
	//   +1  (bump_seed)         = 73
	//   +2  (bin_step_seed)     = 75
	//   +1  (pair_type)         = 76
	//   +4  (active_id i32)     = 80
	//   +2  (bin_step u16)      = 82  ← bin_step is at 80, 2 bytes
	//   +1  (status)            = 83
	//   +1  (require_base_factor_seed) = 84
	//   +2  (base_factor_seed)  = 86
	//   +1  (activation_type)   = 87
	//   +1  (creator_pool_on_off_control) = 88
	//   +32 (token_x_mint)      → tokenX starts at 88
	//   +32 (token_y_mint)      → tokenY starts at 120
	dlmmAccountTokenXOffset = 88
	dlmmAccountTokenYOffset = 120
	dlmmAccountBinStepOffset = 80 // u16 LE

	dlmmMinAccountLen = dlmmAccountTokenYOffset + 32 // 152

	// LbPairCreate event offsets (after 8-byte discriminator)
	dlmmEventLbPairOffset  = 8
	dlmmEventBinStepOffset = 40
	dlmmEventTokenXOffset  = 42
	dlmmEventTokenYOffset  = 74
	dlmmEventMinLen        = 106
)

// LbPairCreate event discriminator — verified from dlmm.json
var dlmmLbPairCreateDisc = [8]byte{185, 74, 252, 125, 27, 215, 188, 111}

// LbPair account discriminator — verified from dlmm.json
var dlmmLbPairAccountDisc = [8]byte{33, 11, 49, 98, 181, 101, 177, 13}

// ── Backfill ──────────────────────────────────────────────────────────────────

func backfillMeteoraDLMM(db *sql.DB, rpc *RPCClient, dc *DecimalCache) {
	log.Println("[backfill/meteora-dlmm] starting LbPair account scan…")

	// Filter by discriminator bytes at offset 0 — much faster than dataSize
	// because DLMM account sizes vary across versions.
	discBytes := make([]byte, 8)
	copy(discBytes, dlmmLbPairAccountDisc[:])
	discBase64 := base64.StdEncoding.EncodeToString(discBytes)

	result, err := rpc.call("getProgramAccounts", []interface{}{
		ProgramMeteoraDLMM,
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
		log.Printf("[backfill/meteora-dlmm] getProgramAccounts failed: %v", err)
		return
	}

	var accounts []accountEntry
	if err := json.Unmarshal(result, &accounts); err != nil {
		log.Printf("[backfill/meteora-dlmm] parse failed: %v", err)
		return
	}
	log.Printf("[backfill/meteora-dlmm] found %d LbPair accounts", len(accounts))

	saved, skipped := 0, 0
	for _, acct := range accounts {
		if len(acct.Account.Data) == 0 {
			skipped++
			continue
		}
		data, err := base64.StdEncoding.DecodeString(acct.Account.Data[0])
		if err != nil || len(data) < dlmmMinAccountLen {
			skipped++
			continue
		}

		tokenX := encodeBase58(data[dlmmAccountTokenXOffset : dlmmAccountTokenXOffset+32])
		tokenY := encodeBase58(data[dlmmAccountTokenYOffset : dlmmAccountTokenYOffset+32])
		if tokenX == "" || tokenY == "" {
			skipped++
			continue
		}

		// Read bin_step from pool state
		binStep := uint16(0)
		if len(data) >= dlmmAccountBinStepOffset+2 {
			binStep = binary.LittleEndian.Uint16(data[dlmmAccountBinStepOffset : dlmmAccountBinStepOffset+2])
		}

		dec0 := dc.GetOrFetch(tokenX, rpc)
		dec1 := dc.GetOrFetch(tokenY, rpc)

		r := PoolRecord{
			PoolAddress: acct.Pubkey,
			Mint0:       tokenX,
			Mint1:       tokenY,
			Decimals0:   dec0,
			Decimals1:   dec1,
			Symbol0:     symbolForMint(tokenX),
			Symbol1:     symbolForMint(tokenY),
			DexName:     "Meteora DLMM",
			Network:     "solana",
			PoolType:    "dlmm",
			ProgramID:   ProgramMeteoraDLMM,
			BinStep:     int(binStep),
		}
		if err := upsertPool(db, r); err != nil {
			log.Printf("[backfill/meteora-dlmm] upsert failed for %s: %v", acct.Pubkey[:8], err)
			skipped++
		} else {
			saved++
		}
		time.Sleep(5 * time.Millisecond)
	}
	log.Printf("[backfill/meteora-dlmm] done — saved=%d skipped=%d", saved, skipped)
}

// ── Live indexer ──────────────────────────────────────────────────────────────

type MeteoraDLMMIndexer struct {
	wsEndpoint string
	db         *sql.DB
	rpc        *RPCClient
	dc         *DecimalCache
}

func (li *MeteoraDLMMIndexer) Run() {
	for {
		if err := li.connectAndListen(); err != nil {
			log.Printf("[live/meteora-dlmm] disconnected: %v — reconnecting in 5s", err)
			time.Sleep(5 * time.Second)
		}
	}
}

func (li *MeteoraDLMMIndexer) connectAndListen() error {
	log.Printf("[live/meteora-dlmm] connecting to %s", li.wsEndpoint)
	conn, _, err := websocket.DefaultDialer.Dial(li.wsEndpoint, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	sub := map[string]interface{}{
		"jsonrpc": "2.0", "id": 1,
		"method": "logsSubscribe",
		"params": []interface{}{
			map[string]interface{}{"mentions": []string{ProgramMeteoraDLMM}},
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

func (li *MeteoraDLMMIndexer) handleMessage(raw []byte) {
	var msg logsMsg
	if err := json.Unmarshal(raw, &msg); err != nil {
		return
	}
	if msg.ID != nil {
		if len(msg.Error) > 0 {
			log.Printf("[live/meteora-dlmm] subscription error: %s", msg.Error)
		} else {
			log.Printf("[live/meteora-dlmm] subscribed to Meteora DLMM program logs")
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
		if err != nil || len(data) < dlmmEventMinLen {
			continue
		}

		// Check discriminator
		var disc [8]byte
		copy(disc[:], data[0:8])
		if disc != dlmmLbPairCreateDisc {
			continue
		}

		// Parse event fields
		lbPair := encodeBase58(data[dlmmEventLbPairOffset : dlmmEventLbPairOffset+32])
		binStep := binary.LittleEndian.Uint16(data[dlmmEventBinStepOffset : dlmmEventBinStepOffset+2])
		tokenX := encodeBase58(data[dlmmEventTokenXOffset : dlmmEventTokenXOffset+32])
		tokenY := encodeBase58(data[dlmmEventTokenYOffset : dlmmEventTokenYOffset+32])

		if lbPair == "" || tokenX == "" || tokenY == "" {
			continue
		}

		dec0 := li.dc.GetOrFetch(tokenX, li.rpc)
		dec1 := li.dc.GetOrFetch(tokenY, li.rpc)

		r := PoolRecord{
			PoolAddress: lbPair,
			Mint0:       tokenX,
			Mint1:       tokenY,
			Decimals0:   dec0,
			Decimals1:   dec1,
			Symbol0:     symbolForMint(tokenX),
			Symbol1:     symbolForMint(tokenY),
			DexName:     "Meteora DLMM",
			Network:     "solana",
			PoolType:    "dlmm",
			ProgramID:   ProgramMeteoraDLMM,
			BinStep:     int(binStep),
		}

		if err := upsertPool(li.db, r); err != nil {
			log.Printf("[live/meteora-dlmm] upsert failed for %s: %v", lbPair[:8], err)
		} else {
			log.Printf("[live/meteora-dlmm] ✅ new pool %s — %s/%s binStep=%d dec:%d/%d tx=%s",
				lbPair[:8], r.Symbol0, r.Symbol1, binStep, dec0, dec1, v.Value.Signature[:8])
		}
		break // one LbPairCreate per tx
	}
}
