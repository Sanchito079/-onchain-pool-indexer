// orca_indexer.go — Orca Whirlpool pool indexer
//
// Program: whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc
//
// Strategy:
//   Backfill  — getProgramAccounts filtered by Whirlpool account discriminator
//               [63,149,209,12,225,128,99,9] at offset 0.
//               token_mint_a @ 101, token_mint_b @ 181
//               (verified from orca_adapter.go and watcher/solana.go — these
//               offsets are the Orca Whirlpool account layout constants used
//               throughout the price-fetcher codebase).
//               Decimals fetched from SPL mint accounts (cached).
//
//   Live      — logsSubscribe on the Whirlpool program.
//               PoolInitialized discriminator: [100,118,173,87,12,198,254,229]
//
//               PoolInitialized event layout (after 8-byte discriminator):
//                 [8:40]   whirlpool         pubkey  ← pool address
//                 [40:72]  whirlpools_config pubkey
//                 [72:104] token_mint_a      pubkey  ← base token
//                 [104:136] token_mint_b     pubkey  ← quote token
//                 [136:138] tick_spacing     u16
//                 [138:170] token_program_a  pubkey
//                 [170:202] token_program_b  pubkey
//                 [202]    decimals_a        u8  ← embedded!
//                 [203]    decimals_b        u8  ← embedded!
//                 [204:220] initial_sqrt_price u128
//
//               decimals_a and decimals_b are embedded in the event — zero
//               extra RPC calls needed for new pools.
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
	ProgramOrcaWhirlpool = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"

	// Whirlpool account offsets (Anchor account layout, verified in watcher code):
	//   8   discriminator
	//   +32  whirlpools_config  = 40
	//   +32  whirlpool_bump     ... layout continues with padding
	// The confirmed offsets from the existing price-fetcher codebase:
	orcaAccountMintAOffset = 101 // token_mint_a
	orcaAccountMintBOffset = 181 // token_mint_b
	orcaMinAccountLen      = orcaAccountMintBOffset + 32 // 213

	// PoolInitialized event offsets (after 8-byte discriminator)
	orcaEventWhirlpoolOffset  = 8
	orcaEventConfigOffset     = 40
	orcaEventMintAOffset      = 72
	orcaEventMintBOffset      = 104
	orcaEventTickSpacingOffset = 136 // u16
	orcaEventProgAOffset      = 138
	orcaEventProgBOffset      = 170
	orcaEventDecimalsAOffset  = 202 // u8 — embedded decimals!
	orcaEventDecimalsBOffset  = 203 // u8 — embedded decimals!
	orcaEventMinLen           = 204 // need at least decimals_b
)

// PoolInitialized discriminator — verified from orca.json
var orcaPoolInitializedDisc = [8]byte{100, 118, 173, 87, 12, 198, 254, 229}

// Whirlpool account discriminator — verified from orca.json
var orcaWhirlpoolAccountDisc = [8]byte{63, 149, 209, 12, 225, 128, 99, 9}

// ── Backfill ──────────────────────────────────────────────────────────────────

func backfillOrca(db *sql.DB, rpc *RPCClient, dc *DecimalCache) {
	log.Println("[backfill/orca] starting Whirlpool account scan…")

	discBytes := make([]byte, 8)
	copy(discBytes, orcaWhirlpoolAccountDisc[:])
	discBase64 := base64.StdEncoding.EncodeToString(discBytes)

	// Slice: token_mint_a @ 101 (32 bytes), token_mint_b @ 181 (32 bytes)
	// offset=101, length=112 covers both mints in one slice
	const orcaSliceOffset = 101
	const orcaSliceLength = 112 // 181+32 - 101 = 112

	result, err := rpc.call("getProgramAccounts", []interface{}{
		ProgramOrcaWhirlpool,
		map[string]interface{}{
			"encoding": "base64",
			"dataSlice": map[string]interface{}{
				"offset": orcaSliceOffset,
				"length": orcaSliceLength,
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
		log.Printf("[backfill/orca] getProgramAccounts failed: %v", err)
		return
	}

	var accounts []accountEntry
	if err := json.Unmarshal(result, &accounts); err != nil {
		log.Printf("[backfill/orca] parse failed: %v", err)
		return
	}
	log.Printf("[backfill/orca] found %d Whirlpool accounts", len(accounts))

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

		// With dataSlice(offset=101, length=112):
		// data[0:32]   = token_mint_a  (was at account offset 101)
		// data[80:112] = token_mint_b  (was at account offset 181, 181-101=80)
		mintA := encodeBase58(data[0:32])
		mintB := encodeBase58(data[80:112])
		if mintA == "" || mintB == "" {
			skipped++
			continue
		}

		dec0 := dc.GetOrFetch(mintA, rpc)
		dec1 := dc.GetOrFetch(mintB, rpc)

		r := PoolRecord{
			PoolAddress: acct.Pubkey,
			Mint0:       mintA,
			Mint1:       mintB,
			Decimals0:   dec0,
			Decimals1:   dec1,
			Symbol0:     symbolForMint(mintA),
			Symbol1:     symbolForMint(mintB),
			DexName:     "Orca Whirlpool",
			Network:     "solana",
			PoolType:    "clmm",
			ProgramID:   ProgramOrcaWhirlpool,
		}
		if err := upsertPool(db, r); err != nil {
			log.Printf("[backfill/orca] upsert failed for %s: %v", acct.Pubkey[:8], err)
			skipped++
		} else {
			saved++
		}
		time.Sleep(5 * time.Millisecond)
	}
	log.Printf("[backfill/orca] done — saved=%d skipped=%d", saved, skipped)
}

// ── Live indexer ──────────────────────────────────────────────────────────────

type OrcaIndexer struct {
	wsEndpoint string
	db         *sql.DB
	rpc        *RPCClient
	dc         *DecimalCache
}

func (li *OrcaIndexer) Run() {
	for {
		if err := li.connectAndListen(); err != nil {
			log.Printf("[live/orca] disconnected: %v — reconnecting in 5s", err)
			time.Sleep(5 * time.Second)
		}
	}
}

func (li *OrcaIndexer) connectAndListen() error {
	log.Printf("[live/orca] connecting to %s", li.wsEndpoint)
	conn, _, err := websocket.DefaultDialer.Dial(li.wsEndpoint, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	sub := map[string]interface{}{
		"jsonrpc": "2.0", "id": 1,
		"method": "logsSubscribe",
		"params": []interface{}{
			map[string]interface{}{"mentions": []string{ProgramOrcaWhirlpool}},
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

func (li *OrcaIndexer) handleMessage(raw []byte) {
	var msg logsMsg
	if err := json.Unmarshal(raw, &msg); err != nil {
		return
	}
	if msg.ID != nil {
		if len(msg.Error) > 0 {
			log.Printf("[live/orca] subscription error: %s", msg.Error)
		} else {
			log.Printf("[live/orca] subscribed to Orca Whirlpool program logs")
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
		if err != nil || len(data) < orcaEventMinLen {
			continue
		}

		// Check discriminator
		var disc [8]byte
		copy(disc[:], data[0:8])
		if disc != orcaPoolInitializedDisc {
			continue
		}

		// Parse event — decimals are embedded, zero RPC needed
		whirlpool := encodeBase58(data[orcaEventWhirlpoolOffset : orcaEventWhirlpoolOffset+32])
		mintA := encodeBase58(data[orcaEventMintAOffset : orcaEventMintAOffset+32])
		mintB := encodeBase58(data[orcaEventMintBOffset : orcaEventMintBOffset+32])

		if whirlpool == "" || mintA == "" || mintB == "" {
			continue
		}

		// Decimals are directly in the event bytes — no RPC needed
		dec0 := int(data[orcaEventDecimalsAOffset])
		dec1 := int(data[orcaEventDecimalsBOffset])

		// Tick spacing
		tickSpacing := binary.LittleEndian.Uint16(data[orcaEventTickSpacingOffset : orcaEventTickSpacingOffset+2])

		// Sanity check decimals
		if dec0 < 0 || dec0 > 18 || dec1 < 0 || dec1 > 18 {
			log.Printf("[live/orca] suspicious decimals %s: a=%d b=%d — fetching from chain",
				whirlpool[:8], dec0, dec1)
			dec0 = li.dc.GetOrFetch(mintA, li.rpc)
			dec1 = li.dc.GetOrFetch(mintB, li.rpc)
		} else {
			// Cache so future lookups skip RPC
			li.dc.Set(mintA, dec0)
			li.dc.Set(mintB, dec1)
		}

		r := PoolRecord{
			PoolAddress: whirlpool,
			Mint0:       mintA,
			Mint1:       mintB,
			Decimals0:   dec0,
			Decimals1:   dec1,
			Symbol0:     symbolForMint(mintA),
			Symbol1:     symbolForMint(mintB),
			DexName:     "Orca Whirlpool",
			Network:     "solana",
			PoolType:    "clmm",
			ProgramID:   ProgramOrcaWhirlpool,
			BinStep:     int(tickSpacing),
		}

		if err := upsertPool(li.db, r); err != nil {
			log.Printf("[live/orca] upsert failed for %s: %v", whirlpool[:8], err)
		} else {
			log.Printf("[live/orca] ✅ new pool %s — %s/%s tickSpacing=%d dec:%d/%d tx=%s",
				whirlpool[:8], r.Symbol0, r.Symbol1, tickSpacing, dec0, dec1, v.Value.Signature[:8])
		}
		break // one PoolInitialized per tx
	}
}
