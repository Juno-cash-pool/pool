package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"juno-pool/internal/api"
	"juno-pool/internal/blockwatch"
	"juno-pool/internal/config"
	"juno-pool/internal/db"
	"juno-pool/internal/metrics"
	"juno-pool/internal/network"
	"juno-pool/internal/payout"
	"juno-pool/internal/stratum"
)

func main() {
	cfgPath := flag.String("config", "config.yaml", "Path to config file")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		log.Fatalf("invalid config: %v", err)
	}

	var store *db.Store
	if cfg.PostgresDSN != "" {
		var err error
		store, err = db.NewStore(cfg.PostgresDSN)
		if err != nil {
			log.Fatalf("init db: %v", err)
		}
		defer store.Close()

		// Ensure pool fee account exists with the configured payout address
		if cfg.PoolFeeAddress != "" {
			store.SetPayoutAddress(context.Background(), "__pool_fee__", cfg.PoolFeeAddress)
			log.Printf("pool fee will be sent to %s", cfg.PoolFeeAddress)
		}
	} else {
		log.Println("WARNING: Running without database - shares will not be persisted")
	}

	prom, err := metrics.NewPromRecorder("juno_pool")
	if err != nil {
		log.Fatalf("init metrics: %v", err)
	}
	metrics.Default = prom

	// Create network stats fetcher
	networkFetcher, err := network.NewFetcher(cfg.NodeRPCURL)
	if err != nil {
		log.Printf("warn: network stats disabled: %v", err)
	} else {
		stopNetworkFetcher := networkFetcher.Start(15 * time.Second)
		defer stopNetworkFetcher()
	}

	// Create API server
	poolFee := float64(cfg.PoolFeeBps) / 100.0
	apiSrv := api.New(store, prom, poolFee, networkFetcher)

	if cfg.MetricsListen != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", prom.Handler())
		// Mount API endpoints
		mux.Handle("/api/", apiSrv.Handler())
		// Serve static web dashboard files
		webDir := "web"
		if _, err := os.Stat(webDir); err == nil {
			fs := http.FileServer(http.Dir(webDir))
			mux.Handle("/", fs)
			log.Printf("serving web dashboard from %s", webDir)
		}
		go func() {
			log.Printf("metrics/api/web listening on %s", cfg.MetricsListen)
			if err := http.ListenAndServe(cfg.MetricsListen, mux); err != nil {
				log.Printf("metrics server error: %v", err)
			}
		}()
	}

	wallet, err := payout.NewWallet(cfg.NodeRPCURL, cfg.PayoutFromAddress)
	if err != nil {
		log.Fatalf("init wallet: %v", err)
	}

	srv := stratum.NewServer(cfg, store)

	// Connect stratum server to API for live stats
	apiSrv.SetStatsProvider(srv)

	var stopBW func()
	if store != nil {
		bw, err := blockwatch.New(store, cfg)
		if err != nil {
			log.Fatalf("init blockwatch: %v", err)
		}
		stopBW = bw.Start()
		defer stopBW()
	}

	// Start payout service (sends to username-as-address and records payouts).
	var stopPayout func()
	var stopPPLNS chan struct{}
	var stopShield chan struct{}
	if store != nil {
		// Start PPLNS reward distributor (credits balances when blocks are confirmed)
		pplns := payout.NewPPLNSDistributor(store, cfg.PoolFeeBps, cfg.PoolFeeAddress, cfg.BlockConfirmations)

		// Update network difficulty from fetcher if available
		if networkFetcher != nil {
			go func() {
				ticker := time.NewTicker(60 * time.Second)
				defer ticker.Stop()
				for range ticker.C {
					if err := networkFetcher.Fetch(context.Background()); err == nil {
						stats := networkFetcher.Get()
						if stats.Difficulty > 0 {
							pplns.SetNetworkDifficulty(stats.Difficulty)
						}
					}
				}
			}()
		}

		stopPPLNS = pplns.StartPeriodicCheck(30 * time.Second)

		// Start auto-shielding service if mining address is configured
		if cfg.MiningAddress != "" && cfg.PayoutFromAddress != "" {
			shieldSvc, err := payout.NewShieldService(cfg.NodeRPCURL, cfg.MiningAddress, cfg.PayoutFromAddress, 0.01)
			if err != nil {
				log.Printf("warn: shield service disabled: %v", err)
			} else {
				stopShield = shieldSvc.Start(5 * time.Minute) // Check every 5 minutes
				log.Printf("auto-shielding enabled: %s -> %s", cfg.MiningAddress[:12], cfg.PayoutFromAddress[:16])
				defer func() { close(stopShield) }()
			}
		}

		paySvc := payout.New(store, wallet, cfg.PayoutThreshold, cfg.PayoutBatchCron)
		stopPayout, err = paySvc.Start()
		if err != nil {
			log.Fatalf("start payout: %v", err)
		}
		defer stopPayout()
		defer func() { close(stopPPLNS) }()

		// Connect payout service to API for manual payout trigger
		apiSrv.SetPayoutRunner(paySvc)
	}
	if err := srv.Start(); err != nil {
		log.Fatalf("start stratum server: %v", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Printf("shutdown signal received, stopping...")

	if err := srv.Stop(); err != nil {
		log.Printf("shutdown error: %v", err)
	}
}
