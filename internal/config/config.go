package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config holds runtime settings for the stratum daemon and backing services.
type Config struct {
	StratumListen       string  `yaml:"stratum_listen"`
	TLSCertPath         string  `yaml:"tls_cert_path"`
	TLSKeyPath          string  `yaml:"tls_key_path"`
	NodeRPCURL          string  `yaml:"node_rpc_url"`
	MetricsListen       string  `yaml:"metrics_listen"`
	NATSURL             string  `yaml:"nats_url"`
	PostgresDSN         string  `yaml:"postgres_dsn"`
	ClickHouseDSN       string  `yaml:"clickhouse_dsn"`
	PoolFeeBps          int     `yaml:"pool_fee_bps"`
	PoolFeeAddress      string  `yaml:"pool_fee_address"` // address to receive pool fees
	DonationDefault     bool    `yaml:"donation_default"`
	VardiffTargetMS     int     `yaml:"vardiff_target_ms"`
	VardiffRetargetSecs int     `yaml:"vardiff_retarget_secs"`
	DefaultDifficulty   float64 `yaml:"default_difficulty"`
	PPSRewardPerDiff    float64 `yaml:"pps_reward_per_diff"`
	BlockConfirmations  int     `yaml:"block_confirmations"`
	PayoutThreshold     float64 `yaml:"payout_threshold"`
	PayoutBatchCron     string  `yaml:"payout_batch_cron"`
	PayoutFromAddress   string  `yaml:"payout_from_address"` // shielded/unified address to send payouts from
	MiningAddress       string  `yaml:"mining_address"`      // transparent address that receives coinbase (for auto-shielding)
}

// Load reads YAML config from disk.
func Load(path string) (Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}
	return cfg, nil
}

// Validate enforces required fields and basic sanity checks.
func (c Config) Validate() error {
	if c.StratumListen == "" {
		return fmt.Errorf("stratum_listen is required")
	}
	// TLS is optional - if both paths are empty, run without TLS
	if (c.TLSCertPath == "") != (c.TLSKeyPath == "") {
		return fmt.Errorf("tls_cert_path and tls_key_path must both be set or both empty")
	}
	if c.NodeRPCURL == "" {
		return fmt.Errorf("node_rpc_url is required")
	}
	// PostgresDSN is optional for testing
	if c.PoolFeeBps < 0 || c.PoolFeeBps > 1000 {
		return fmt.Errorf("pool_fee_bps must be between 0 and 1000 (0-10%%)")
	}
	if c.PayoutThreshold <= 0 {
		return fmt.Errorf("payout_threshold must be > 0")
	}
	if c.PayoutBatchCron == "" {
		return fmt.Errorf("payout_batch_cron is required (e.g., '@daily')")
	}
	if c.VardiffTargetMS < 0 {
		return fmt.Errorf("vardiff_target_ms must be >= 0")
	}
	if c.VardiffRetargetSecs < 0 {
		return fmt.Errorf("vardiff_retarget_secs must be >= 0")
	}
	if c.DefaultDifficulty <= 0 {
		c.DefaultDifficulty = 1
	}
	if c.PPSRewardPerDiff < 0 {
		c.PPSRewardPerDiff = 0
	}
	if c.BlockConfirmations <= 0 {
		c.BlockConfirmations = 10
	}
	return nil
}
