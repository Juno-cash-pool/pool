# Juno Cash Mining Pool

A stratum mining pool for [Juno Cash](https://juno.cash) — a privacy-focused cryptocurrency using the RandomX (`rx/juno`) algorithm.

**Live Pool:** [https://junocashpool.com](https://junocashpool.com)

## Features

- 🔒 **TLS/SSL Support** — Secure stratum connections
- ⚡ **Variable Difficulty (Vardiff)** — Auto-adjusts to miner hashrate
- 💰 **Automatic Payouts** — Scheduled payouts with configurable thresholds
- 📊 **Web Dashboard** — Real-time stats, blocks, and miner lookup
- 🔧 **XMRig Compatible** — Works with XMRig's Monero-style stratum protocol
- 📈 **Prometheus Metrics** — Built-in metrics endpoint for monitoring

## Quick Start

### Prerequisites

- Go 1.21+
- PostgreSQL
- NATS (optional, for distributed deployments)
- Juno Cash node (junocashd) running with RPC enabled
- TLS certificate (Let's Encrypt recommended)

### Installation

```bash
# Clone the repository
git clone https://github.com/Juno-cash-pool/pool.git
cd pool

# Copy and configure
cp config.example.yaml config.yaml
nano config.yaml  # Edit with your settings

# Build
go mod tidy
go build -o stratumd ./cmd/stratumd

# Run
./stratumd -config config.yaml
```

### Configuration

Edit `config.yaml` with your settings:

```yaml
stratum_listen: ":4444"
tls_cert_path: "/etc/letsencrypt/live/yourdomain.com/fullchain.pem"
tls_key_path: "/etc/letsencrypt/live/yourdomain.com/privkey.pem"
node_rpc_url: "http://rpcuser:rpcpassword@127.0.0.1:8232"
postgres_dsn: "postgres://user:pass@127.0.0.1:5432/juno_pool?sslmode=disable"

pool_fee_bps: 100              # 1% pool fee
default_difficulty: 10000      # Starting difficulty
payout_threshold: 0.001        # Minimum payout in JUNO
```

See [config.example.yaml](config.example.yaml) for all options.

## Mining

### Using XMRig

Download the patched XMRig from [xmrig-junocash releases](https://github.com/Juno-cash-pool/juno-rig/releases).

**Command line:**
```bash
./xmrig \
  -o stratum+ssl://stratum.junocashpool.com:4444 \
  -u YOUR_JUNO_ADDRESS \
  -p x \
  -a rx/juno
```

**Or use config.json:**
```json
{
    "autosave": true,
    "cpu": true,
    "pools": [{
        "url": "stratum+ssl://stratum.junocashpool.com:4444",
        "user": "YOUR_JUNO_ADDRESS",
        "pass": "x",
        "algo": "rx/juno",
        "keepalive": true
    }]
}
```

See [MINERS.md](docs/MINERS.md) for detailed mining instructions.

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Miners    │────▶│  stratumd   │────▶│  junocashd  │
│  (XMRig)    │ TLS │  (Pool)     │ RPC │  (Node)     │
└─────────────┘     └──────┬──────┘     └─────────────┘
                           │
                    ┌──────┴──────┐
                    │  PostgreSQL │
                    │  (Shares,   │
                    │   Balances) │
                    └─────────────┘
```

## Project Structure

```
├── cmd/stratumd/       # Pool server entrypoint
├── internal/
│   ├── api/            # REST API & web dashboard
│   ├── config/         # Configuration loading
│   ├── db/             # PostgreSQL store (shares, balances, payouts)
│   ├── job/            # Block template management
│   ├── metrics/        # Prometheus metrics
│   ├── network/        # Network stats fetcher
│   ├── payout/         # Payout processor
│   └── stratum/        # Stratum protocol (sessions, shares)
├── web/                # Dashboard frontend (HTML/CSS/JS)
├── docs/               # Documentation
│   ├── DEPLOY.md       # Deployment guide
│   └── MINERS.md       # Mining guide
└── config.example.yaml # Example configuration
```

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/pool/stats` | Pool hashrate, miners, blocks found |
| `GET /api/network/stats` | Network hashrate, difficulty, height |
| `GET /api/pool/blocks` | Recent blocks found by pool |
| `GET /api/pool/payments` | Recent payouts |
| `GET /api/miner/:address` | Individual miner stats |

## Deployment

For production deployment with nginx, systemd, and TLS, see [DEPLOY.md](docs/DEPLOY.md).

### Quick Deploy Checklist

1. ✅ Juno node synced and running
2. ✅ PostgreSQL database created
3. ✅ TLS certificate obtained (Let's Encrypt)
4. ✅ config.yaml configured
5. ✅ Firewall: ports 4444 (stratum), 443 (web) open
6. ✅ nginx reverse proxy for web dashboard
7. ✅ systemd service for stratumd

## Development

```bash
# Run tests
go test ./...

# Run with hot reload (using air)
air

# Build for production
CGO_ENABLED=0 go build -o stratumd ./cmd/stratumd
```

## License

MIT License - see [LICENSE](LICENSE) for details.

## Links

- 🌐 **Pool:** [junocashpool.com](https://junocashpool.com)
- 💻 **Juno Cash:** [juno.cash](https://juno.cash)
- 📦 **Juno Node:** [github.com/juno-cash/junocash](https://github.com/juno-cash/junocash)
- ⛏️ **XMRig Miner:** [github.com/Juno-cash-pool/juno-rig](https://github.com/Juno-cash-pool/juno-rig)

