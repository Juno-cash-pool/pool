package payout

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"time"
)

const (
	// CoinbaseMaturity is the number of confirmations required before coinbase can be spent
	CoinbaseMaturity = 100
)

// ShieldService automatically shields mature coinbase from transparent to shielded pool address.
type ShieldService struct {
	client        *http.Client
	url           *url.URL
	miningAddress string  // t1 address that receives coinbase
	shieldedPool  string  // j1/orchard address to shield funds to
	minAmount     float64 // minimum amount to shield (avoid dust)
}

// NewShieldService creates a new auto-shielding service.
func NewShieldService(rpcURL string, miningAddress string, shieldedPool string, minAmount float64) (*ShieldService, error) {
	parsed, err := url.Parse(rpcURL)
	if err != nil {
		return nil, fmt.Errorf("parse rpc url: %w", err)
	}
	return &ShieldService{
		client:        &http.Client{Timeout: 60 * time.Second},
		url:           parsed,
		miningAddress: miningAddress,
		shieldedPool:  shieldedPool,
		minAmount:     minAmount,
	}, nil
}

// Start begins periodic shielding checks.
func (s *ShieldService) Start(interval time.Duration) chan struct{} {
	stopCh := make(chan struct{})

	go func() {
		// Run immediately on start
		s.shieldMatureCoins(context.Background())

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
				s.shieldMatureCoins(ctx)
				cancel()
			case <-stopCh:
				return
			}
		}
	}()

	return stopCh
}

// shieldMatureCoins checks for mature transparent balance and shields it.
func (s *ShieldService) shieldMatureCoins(ctx context.Context) {
	if s.miningAddress == "" || s.shieldedPool == "" {
		return
	}

	// Get spendable balance from the mining address (only mature coins)
	balance, err := s.getAddressBalance(ctx, s.miningAddress)
	if err != nil {
		log.Printf("shield: failed to get balance for %s: %v", s.miningAddress[:12], err)
		return
	}

	if balance < s.minAmount {
		return // Nothing to shield or below threshold
	}

	log.Printf("shield: found %.8f JUNO to shield from %s", balance, s.miningAddress[:12])

	// Shield the balance using z_sendmany
	txid, err := s.shieldToOrchard(ctx, balance)
	if err != nil {
		log.Printf("shield: failed to shield %.8f JUNO: %v", balance, err)
		return
	}

	log.Printf("shield: successfully shielded %.8f JUNO to pool (txid: %s)", balance, txid[:16])
}

// getAddressBalance gets the spendable balance for an address.
func (s *ShieldService) getAddressBalance(ctx context.Context, address string) (float64, error) {
	// Use z_getbalanceforaddress or getreceivedbyaddress
	// For transparent addresses, we use listunspent and filter by address
	body, err := json.Marshal(rpcReq{
		JSONRPC: "1.0",
		ID:      "juno-pool-shield",
		Method:  "listunspent",
		Params:  []interface{}{CoinbaseMaturity, 9999999, []string{address}},
	})
	if err != nil {
		return 0, err
	}

	result, err := s.doRPC(ctx, body)
	if err != nil {
		return 0, err
	}

	var utxos []struct {
		Amount float64 `json:"amount"`
	}
	if err := json.Unmarshal(result, &utxos); err != nil {
		return 0, err
	}

	var total float64
	for _, u := range utxos {
		total += u.Amount
	}
	return total, nil
}

// shieldToOrchard uses z_shieldcoinbase to shield mature coinbase to the pool's orchard address.
func (s *ShieldService) shieldToOrchard(ctx context.Context, amount float64) (string, error) {
	// Use z_shieldcoinbase which is the proper way to shield coinbase
	// Params: fromaddress, toaddress, fee (null for ZIP-317), limit
	body, err := json.Marshal(rpcReq{
		JSONRPC: "1.0",
		ID:      "juno-pool-shield",
		Method:  "z_shieldcoinbase",
		Params:  []interface{}{s.miningAddress, s.shieldedPool, nil, 0},
	})
	if err != nil {
		return "", err
	}

	result, err := s.doRPC(ctx, body)
	if err != nil {
		return "", fmt.Errorf("z_shieldcoinbase: %w", err)
	}

	var shieldResult struct {
		RemainingUTXOs int     `json:"remainingUTXOs"`
		RemainingValue float64 `json:"remainingValue"`
		ShieldingUTXOs int     `json:"shieldingUTXOs"`
		ShieldingValue float64 `json:"shieldingValue"`
		OpID           string  `json:"opid"`
	}
	if err := json.Unmarshal(result, &shieldResult); err != nil {
		return "", fmt.Errorf("parse shield result: %w", err)
	}

	if shieldResult.OpID == "" {
		return "", fmt.Errorf("no operation started (nothing to shield)")
	}

	log.Printf("shield: shielding %d UTXOs (%.8f JUNO), %d remaining",
		shieldResult.ShieldingUTXOs, shieldResult.ShieldingValue, shieldResult.RemainingUTXOs)

	// Poll for operation completion
	return s.waitForOperation(ctx, shieldResult.OpID)
}

// waitForOperation polls z_getoperationstatus until the operation completes.
func (s *ShieldService) waitForOperation(ctx context.Context, opid string) (string, error) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-ticker.C:
			body, err := json.Marshal(rpcReq{
				JSONRPC: "1.0",
				ID:      "juno-pool-shield",
				Method:  "z_getoperationstatus",
				Params:  []interface{}{[]string{opid}},
			})
			if err != nil {
				return "", err
			}

			result, err := s.doRPC(ctx, body)
			if err != nil {
				continue // Retry on transient errors
			}

			var ops []struct {
				ID     string `json:"id"`
				Status string `json:"status"`
				Result struct {
					TxID string `json:"txid"`
				} `json:"result"`
				Error struct {
					Message string `json:"message"`
				} `json:"error"`
			}
			if err := json.Unmarshal(result, &ops); err != nil {
				return "", err
			}

			if len(ops) == 0 {
				continue
			}

			switch ops[0].Status {
			case "success":
				return ops[0].Result.TxID, nil
			case "failed":
				return "", fmt.Errorf("operation failed: %s", ops[0].Error.Message)
			case "executing", "queued":
				continue
			}
		}
	}
}

// doRPC performs an RPC call to the node.
func (s *ShieldService) doRPC(ctx context.Context, body []byte) (json.RawMessage, error) {
	req, err := http.NewRequestWithContext(ctx, "POST", s.url.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if s.url.User != nil {
		pass, _ := s.url.User.Password()
		req.SetBasicAuth(s.url.User.Username(), pass)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("rpc status %d", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var rpcResp struct {
		Result json.RawMessage `json:"result"`
		Error  interface{}     `json:"error"`
	}
	if err := json.Unmarshal(data, &rpcResp); err != nil {
		return nil, err
	}
	if rpcResp.Error != nil {
		return nil, fmt.Errorf("rpc error: %v", rpcResp.Error)
	}
	return rpcResp.Result, nil
}
