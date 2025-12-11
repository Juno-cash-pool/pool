package payout

import (
	"context"
	"log"
	"time"

	"juno-pool/internal/db"
)

const (
	// BlockReward is the mining reward per block in JUNO
	BlockReward = 10.0
	// PPLNSWindowMultiplier - look at shares worth N times the network difficulty
	PPLNSWindowMultiplier = 2.0
)

// PPLNSDistributor handles distributing block rewards to miners based on shares.
type PPLNSDistributor struct {
	store              *db.Store
	poolFeeBps         int     // Pool fee in basis points (100 = 1%)
	poolFeeAddress     string  // Address to credit pool fees
	blockConfirmations int     // Required confirmations before distributing
	networkDifficulty  float64 // Current network difficulty for PPLNS window
}

// NewPPLNSDistributor creates a new PPLNS distributor.
func NewPPLNSDistributor(store *db.Store, poolFeeBps int, poolFeeAddress string, blockConfirmations int) *PPLNSDistributor {
	return &PPLNSDistributor{
		store:              store,
		poolFeeBps:         poolFeeBps,
		poolFeeAddress:     poolFeeAddress,
		blockConfirmations: blockConfirmations,
		networkDifficulty:  1000000, // Default, will be updated
	}
}

// SetNetworkDifficulty updates the network difficulty for PPLNS window calculation.
func (p *PPLNSDistributor) SetNetworkDifficulty(diff float64) {
	p.networkDifficulty = diff
}

// DistributeBlockReward distributes rewards for a confirmed block using PPLNS.
// It credits miner balances proportionally based on their share contributions.
func (p *PPLNSDistributor) DistributeBlockReward(ctx context.Context, block db.BlockRow) error {
	// Calculate PPLNS window based on network difficulty
	windowDifficulty := p.networkDifficulty * PPLNSWindowMultiplier

	// Get share contributions within the PPLNS window
	contributions, totalDifficulty, err := p.store.GetPPLNSShares(ctx, windowDifficulty)
	if err != nil {
		return err
	}

	if len(contributions) == 0 || totalDifficulty == 0 {
		log.Printf("PPLNS: no shares found for block %d, skipping distribution", block.Height)
		return p.store.MarkBlockPaid(ctx, block.Hash)
	}

	// Calculate pool fee
	poolFee := BlockReward * float64(p.poolFeeBps) / 10000.0
	distributableReward := BlockReward - poolFee

	log.Printf("PPLNS: distributing %.8f JUNO for block %d (%.8f pool fee)",
		distributableReward, block.Height, poolFee)
	log.Printf("PPLNS: %d miners contributed %.2f total difficulty",
		len(contributions), totalDifficulty)

	// Credit pool fee
	if poolFee > 0 && p.poolFeeAddress != "" {
		// Convert to satoshis for storage
		poolFeeSatoshis := poolFee * satoshisPerCoin
		p.store.CreditBalance(ctx, "__pool_fee__", poolFeeSatoshis)
		log.Printf("PPLNS: credited %.8f JUNO pool fee", poolFee)
	}

	// Distribute to miners proportionally
	for _, c := range contributions {
		proportion := c.Difficulty / totalDifficulty
		reward := distributableReward * proportion
		// Convert to satoshis for storage
		rewardSatoshis := reward * satoshisPerCoin

		p.store.CreditBalance(ctx, c.Username, rewardSatoshis)
		log.Printf("PPLNS: credited %.8f JUNO to %s (%.2f%% of shares)",
			reward, truncateUsername(c.Username), proportion*100)
	}

	// Mark block as paid
	return p.store.MarkBlockPaid(ctx, block.Hash)
}

// ProcessConfirmedBlocks checks for confirmed blocks and distributes rewards.
func (p *PPLNSDistributor) ProcessConfirmedBlocks(ctx context.Context) error {
	blocks, err := p.store.GetUnpaidConfirmedBlocks(ctx, p.blockConfirmations)
	if err != nil {
		return err
	}

	for _, block := range blocks {
		log.Printf("PPLNS: processing confirmed block %d (hash: %s)", block.Height, block.Hash[:16])
		if err := p.DistributeBlockReward(ctx, block); err != nil {
			log.Printf("PPLNS: failed to distribute block %d: %v", block.Height, err)
			continue
		}
	}

	return nil
}

// StartPeriodicCheck starts a goroutine that periodically checks for confirmed blocks.
func (p *PPLNSDistributor) StartPeriodicCheck(interval time.Duration) chan struct{} {
	stopCh := make(chan struct{})

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				if err := p.ProcessConfirmedBlocks(ctx); err != nil {
					log.Printf("PPLNS: periodic check failed: %v", err)
				}
				cancel()
			case <-stopCh:
				return
			}
		}
	}()

	return stopCh
}

// truncateUsername shortens long wallet addresses for logging.
func truncateUsername(username string) string {
	if len(username) > 20 {
		return username[:10] + "..." + username[len(username)-6:]
	}
	return username
}
