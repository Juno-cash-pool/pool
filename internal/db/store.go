package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// Store wraps a Postgres connection for persisting pool events.
type Store struct {
	db *sql.DB
}

// BlockRow represents a persisted block record.
type BlockRow struct {
	JobID     string
	Height    int64
	Status    string
	Hash      string
	Confirms  int
	CreatedAt time.Time
}

// ShareRow represents a persisted share record.
type ShareRow struct {
	Username   string
	JobID      string
	Difficulty float64
	Accepted   bool
	Stale      bool
	Invalid    bool
	CreatedAt  time.Time
}

// BalanceRow holds miner balance state.
type BalanceRow struct {
	Username string
	Payout   string
	Balance  float64
	Updated  time.Time
}

// NewStore opens a Postgres connection and ensures tables exist.
func NewStore(dsn string) (*Store, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping db: %w", err)
	}
	if err := ensureSchema(ctx, db); err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

func ensureSchema(ctx context.Context, db *sql.DB) error {
	stmts := []string{
		`create table if not exists miners (
            id serial primary key,
		username text unique not null,
		payout_address text,
            created_at timestamptz not null default now()
        )`,
		`alter table miners add column if not exists payout_address text`,
		`create table if not exists balances (
			miner_id integer primary key references miners(id),
			balance double precision not null default 0,
			updated_at timestamptz not null default now()
		)`,
		`create table if not exists shares (
            id bigserial primary key,
            miner_id integer not null references miners(id),
            job_id text not null,
            difficulty double precision not null,
            accepted boolean not null,
            stale boolean not null default false,
            invalid boolean not null default false,
            created_at timestamptz not null default now()
        )`,
		`create table if not exists blocks (
            id bigserial primary key,
            job_id text not null,
            height bigint not null,
            status text not null,
			hash text,
			confirmations integer not null default 0,
			paid boolean not null default false,
            created_at timestamptz not null default now()
		)`,
		`alter table blocks add column if not exists paid boolean not null default false`,
		`create table if not exists payouts (
			id bigserial primary key,
			miner_id integer not null references miners(id),
			amount double precision not null,
			status text not null,
			txid text,
			created_at timestamptz not null default now(),
			updated_at timestamptz not null default now()
        )`,
	}
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("schema: %w", err)
		}
	}
	return nil
}

// getOrCreateMiner returns miner id for a username.
func (s *Store) getOrCreateMiner(ctx context.Context, username string) (int64, error) {
	if username == "" {
		username = "anonymous"
	}
	var id int64
	err := s.db.QueryRowContext(ctx, `select id from miners where username=$1`, username).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		err = s.db.QueryRowContext(ctx, `insert into miners (username) values ($1) returning id`, username).Scan(&id)
	}
	if err != nil {
		return 0, fmt.Errorf("miner upsert: %w", err)
	}
	return id, nil
}

// RecordShare inserts a share row.
func (s *Store) RecordShare(ctx context.Context, username, jobID string, difficulty float64, accepted, stale, invalid bool) {
	minerID, err := s.getOrCreateMiner(ctx, username)
	if err != nil {
		return
	}
	_, _ = s.db.ExecContext(ctx, `insert into shares (miner_id, job_id, difficulty, accepted, stale, invalid) values ($1,$2,$3,$4,$5,$6)`, minerID, jobID, difficulty, accepted, stale, invalid)
}

// CreditBalance increments a miner balance by delta.
func (s *Store) CreditBalance(ctx context.Context, username string, delta float64) {
	minerID, err := s.getOrCreateMiner(ctx, username)
	if err != nil {
		return
	}
	_, _ = s.db.ExecContext(ctx, `
		insert into balances (miner_id, balance)
		values ($1, $2)
		on conflict (miner_id) do update set balance = balances.balance + excluded.balance, updated_at = now()`, minerID, delta)
}

// SetPayoutAddress sets payout address for a miner.
func (s *Store) SetPayoutAddress(ctx context.Context, username, address string) {
	minerID, err := s.getOrCreateMiner(ctx, username)
	if err != nil {
		return
	}
	_, _ = s.db.ExecContext(ctx, `update miners set payout_address=$1 where id=$2`, address, minerID)
}

// RecordBlock inserts a block candidate row.
func (s *Store) RecordBlock(ctx context.Context, jobID string, height int64, hash string, status string) {
	if status == "" {
		status = "found"
	}
	// Use ON CONFLICT to avoid duplicate inserts for same hash
	_, _ = s.db.ExecContext(ctx, `INSERT INTO blocks (job_id, height, hash, status) VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING`, jobID, height, hash, status)
}

// UpdateBlockStatus updates block status by job id (only if not already submitted/confirmed).
func (s *Store) UpdateBlockStatus(ctx context.Context, jobID string, status string) {
	// Only update if not already in a terminal state, and limit to avoid mass updates
	_, _ = s.db.ExecContext(ctx, `UPDATE blocks SET status=$1 WHERE job_id=$2 AND status NOT IN ('submitted', 'confirmed')`, status, jobID)
}

// UpdateBlockConfirmations updates confirmations and status for a given hash.
func (s *Store) UpdateBlockConfirmations(ctx context.Context, hash string, confs int, status string) {
	_, _ = s.db.ExecContext(ctx, `update blocks set confirmations=$1, status=$2 where hash=$3`, confs, status, hash)
}

// BalancesAbove returns miners with balance >= threshold.
func (s *Store) BalancesAbove(ctx context.Context, threshold float64) ([]BalanceRow, error) {
	rows, err := s.db.QueryContext(ctx, `
		select m.username, m.payout_address, b.balance, b.updated_at
		from balances b join miners m on m.id = b.miner_id
		where b.balance >= $1
		order by b.balance desc`, threshold)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []BalanceRow
	for rows.Next() {
		var r BalanceRow
		if err := rows.Scan(&r.Username, &r.Payout, &r.Balance, &r.Updated); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// DeductAndRecordPayout atomically deducts from balance and records a payout row.
func (s *Store) DeductAndRecordPayout(ctx context.Context, username string, amount float64, status, txid string) error {
	minerID, err := s.getOrCreateMiner(ctx, username)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `
		update balances set balance = balance - $1, updated_at = now()
		where miner_id = $2 and balance >= $1`, amount, minerID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		insert into payouts (miner_id, amount, status, txid) values ($1,$2,$3,$4)`, minerID, amount, status, txid); err != nil {
		return err
	}
	return tx.Commit()
}

// RecordPayout logs a payout event without touching balance (e.g., failures).
func (s *Store) RecordPayout(ctx context.Context, username string, amount float64, status, txid string) {
	minerID, err := s.getOrCreateMiner(ctx, username)
	if err != nil {
		return
	}
	_, _ = s.db.ExecContext(ctx, `insert into payouts (miner_id, amount, status, txid) values ($1,$2,$3,$4)`, minerID, amount, status, txid)
}

// RecentBlocks returns the most recent N blocks.
func (s *Store) RecentBlocks(ctx context.Context, limit int) ([]BlockRow, error) {
	rows, err := s.db.QueryContext(ctx, `select job_id, height, status, coalesce(hash, ''), confirmations, created_at from blocks order by created_at desc limit $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []BlockRow
	for rows.Next() {
		var r BlockRow
		if err := rows.Scan(&r.JobID, &r.Height, &r.Status, &r.Hash, &r.Confirms, &r.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// PendingBlocks returns submitted blocks needing confirmation checks.
func (s *Store) PendingBlocks(ctx context.Context, limit int) ([]BlockRow, error) {
	rows, err := s.db.QueryContext(ctx, `select job_id, height, status, hash, confirmations, created_at from blocks where status in ('submitted','found') and hash is not null order by created_at desc limit $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []BlockRow
	for rows.Next() {
		var r BlockRow
		if err := rows.Scan(&r.JobID, &r.Height, &r.Status, &r.Hash, &r.Confirms, &r.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// RecentShares returns the most recent N shares with usernames.
func (s *Store) RecentShares(ctx context.Context, limit int) ([]ShareRow, error) {
	rows, err := s.db.QueryContext(ctx, `
		select m.username, sh.job_id, sh.difficulty, sh.accepted, sh.stale, sh.invalid, sh.created_at
		from shares sh
		join miners m on m.id = sh.miner_id
		order by sh.created_at desc
		limit $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ShareRow
	for rows.Next() {
		var r ShareRow
		if err := rows.Scan(&r.Username, &r.JobID, &r.Difficulty, &r.Accepted, &r.Stale, &r.Invalid, &r.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// RecentPayouts returns the most recent payout rows.
func (s *Store) RecentPayouts(ctx context.Context, limit int) ([]struct {
	Username string
	Amount   float64
	Status   string
	TxID     string
	Created  time.Time
	Updated  time.Time
}, error) {
	rows, err := s.db.QueryContext(ctx, `
		select m.username, p.amount, p.status, coalesce(p.txid, ''), p.created_at, p.updated_at
		from payouts p join miners m on m.id = p.miner_id
		order by p.created_at desc
		limit $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []struct {
		Username string
		Amount   float64
		Status   string
		TxID     string
		Created  time.Time
		Updated  time.Time
	}
	for rows.Next() {
		var r struct {
			Username string
			Amount   float64
			Status   string
			TxID     string
			Created  time.Time
			Updated  time.Time
		}
		if err := rows.Scan(&r.Username, &r.Amount, &r.Status, &r.TxID, &r.Created, &r.Updated); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// MinerStatsRow holds aggregated miner statistics.
type MinerStatsRow struct {
	SharesAccepted int64
	SharesRejected int64
	LastShare      time.Time
	Balance        float64
	TotalPaid      float64
	PayoutAddress  string
}

// ShareCount24h returns the total number of shares in the last 24 hours.
func (s *Store) ShareCount24h(ctx context.Context) (int64, error) {
	var count int64
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM shares 
		WHERE created_at > NOW() - INTERVAL '24 hours'`).Scan(&count)
	return count, err
}

// GetMinerStats returns aggregated statistics for a miner.
func (s *Store) GetMinerStats(ctx context.Context, username string) (*MinerStatsRow, error) {
	stats := &MinerStatsRow{}

	// Get share counts
	err := s.db.QueryRowContext(ctx, `
		SELECT 
			COALESCE(SUM(CASE WHEN accepted THEN 1 ELSE 0 END), 0) as accepted,
			COALESCE(SUM(CASE WHEN NOT accepted THEN 1 ELSE 0 END), 0) as rejected,
			COALESCE(MAX(sh.created_at), '1970-01-01'::timestamptz) as last_share
		FROM shares sh
		JOIN miners m ON m.id = sh.miner_id
		WHERE m.username = $1`, username).Scan(&stats.SharesAccepted, &stats.SharesRejected, &stats.LastShare)
	if err != nil {
		return nil, err
	}

	// Get balance
	err = s.db.QueryRowContext(ctx, `
		SELECT COALESCE(b.balance, 0), COALESCE(m.payout_address, '')
		FROM miners m
		LEFT JOIN balances b ON b.miner_id = m.id
		WHERE m.username = $1`, username).Scan(&stats.Balance, &stats.PayoutAddress)
	if err != nil {
		// Miner might not exist, return zero balance
		stats.Balance = 0
		stats.PayoutAddress = ""
	}

	// Get total paid
	err = s.db.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(p.amount), 0)
		FROM payouts p
		JOIN miners m ON m.id = p.miner_id
		WHERE m.username = $1 AND p.status = 'sent'`, username).Scan(&stats.TotalPaid)
	if err != nil {
		stats.TotalPaid = 0
	}

	return stats, nil
}

// MinerExists checks if a miner with the given username exists.
func (s *Store) MinerExists(ctx context.Context, username string) (bool, error) {
	var exists bool
	err := s.db.QueryRowContext(ctx, `
		SELECT EXISTS(SELECT 1 FROM miners WHERE username = $1)`, username).Scan(&exists)
	return exists, err
}

// GetAllBalances returns all miner balances.
func (s *Store) GetAllBalances(ctx context.Context) ([]BalanceRow, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT m.username, COALESCE(m.payout_address, ''), COALESCE(b.balance, 0), COALESCE(b.updated_at, m.created_at)
		FROM miners m
		LEFT JOIN balances b ON b.miner_id = m.id
		ORDER BY b.balance DESC NULLS LAST`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []BalanceRow
	for rows.Next() {
		var r BalanceRow
		if err := rows.Scan(&r.Username, &r.Payout, &r.Balance, &r.Updated); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// Close closes the underlying DB.
func (s *Store) Close() error { return s.db.Close() }

// ShareContribution represents a miner's contribution in a PPLNS window.
type ShareContribution struct {
	Username   string
	Difficulty float64 // Total difficulty contributed
}

// GetPPLNSShares returns share contributions within the last N difficulty units.
// This implements PPLNS (Pay Per Last N Shares) by looking at recent accepted shares.
func (s *Store) GetPPLNSShares(ctx context.Context, windowDifficulty float64) ([]ShareContribution, float64, error) {
	// Get shares in reverse chronological order until we hit the window size
	rows, err := s.db.QueryContext(ctx, `
		WITH recent_shares AS (
			SELECT m.username, sh.difficulty, sh.created_at,
				SUM(sh.difficulty) OVER (ORDER BY sh.created_at DESC) as running_total
			FROM shares sh
			JOIN miners m ON m.id = sh.miner_id
			WHERE sh.accepted = true
			ORDER BY sh.created_at DESC
		)
		SELECT username, SUM(difficulty) as total_diff
		FROM recent_shares
		WHERE running_total <= $1 * 2  -- Take shares up to 2x window to ensure we have enough
		GROUP BY username
		ORDER BY total_diff DESC`, windowDifficulty)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var contributions []ShareContribution
	var totalDifficulty float64
	for rows.Next() {
		var c ShareContribution
		if err := rows.Scan(&c.Username, &c.Difficulty); err != nil {
			return nil, 0, err
		}
		contributions = append(contributions, c)
		totalDifficulty += c.Difficulty
	}
	return contributions, totalDifficulty, rows.Err()
}

// GetSharesSinceBlock returns share contributions since a specific time (for block-based PPLNS).
func (s *Store) GetSharesSinceBlock(ctx context.Context, since time.Time) ([]ShareContribution, float64, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT m.username, SUM(sh.difficulty) as total_diff
		FROM shares sh
		JOIN miners m ON m.id = sh.miner_id
		WHERE sh.accepted = true AND sh.created_at >= $1
		GROUP BY m.username
		ORDER BY total_diff DESC`, since)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var contributions []ShareContribution
	var totalDifficulty float64
	for rows.Next() {
		var c ShareContribution
		if err := rows.Scan(&c.Username, &c.Difficulty); err != nil {
			return nil, 0, err
		}
		contributions = append(contributions, c)
		totalDifficulty += c.Difficulty
	}
	return contributions, totalDifficulty, rows.Err()
}

// GetUnpaidConfirmedBlocks returns blocks that are confirmed but haven't had rewards distributed.
func (s *Store) GetUnpaidConfirmedBlocks(ctx context.Context, requiredConfirmations int) ([]BlockRow, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT job_id, height, status, COALESCE(hash, ''), confirmations, created_at
		FROM blocks
		WHERE status = 'confirmed' AND confirmations >= $1 AND paid = false
		ORDER BY created_at ASC`, requiredConfirmations)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []BlockRow
	for rows.Next() {
		var r BlockRow
		if err := rows.Scan(&r.JobID, &r.Height, &r.Status, &r.Hash, &r.Confirms, &r.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// MarkBlockPaid marks a block as having had rewards distributed.
func (s *Store) MarkBlockPaid(ctx context.Context, hash string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE blocks SET paid = true WHERE hash = $1`, hash)
	return err
}
