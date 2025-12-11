package stratum

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"strconv"
	"sync"
	"time"

	"juno-pool/internal/config"
	"juno-pool/internal/db"
	"juno-pool/internal/job"
	"juno-pool/internal/metrics"
)

// Session handles a single stratum TCP/TLS connection.
type Session struct {
	cfg         config.Config
	conn        net.Conn
	rw          *bufio.ReadWriter
	metrics     metrics.Recorder
	extranonce1 string
	submitter   *job.Submitter
	store       *db.Store
	subscribed  bool
	authorized  bool
	username    string
	getTemplate func() *job.Template
	getJob      func(string) *job.Template
	difficulty  float64
	lastShare   time.Time
	lastRetgt   time.Time
	invalidCnt  int
	isXmrig     bool // true if client uses xmrig/Monero stratum protocol
	// Share tracking for hashrate estimation
	shareCount int64
	shareStart time.Time
	mu         sync.Mutex
}

func NewSession(cfg config.Config, conn net.Conn, extranonce1 string, getTemplate func() *job.Template, getJob func(string) *job.Template, submitter *job.Submitter, store *db.Store) *Session {
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	return &Session{
		cfg:         cfg,
		conn:        conn,
		rw:          rw,
		metrics:     metrics.Default,
		extranonce1: extranonce1,
		submitter:   submitter,
		store:       store,
		getTemplate: getTemplate,
		getJob:      getJob,
		difficulty:  cfg.DefaultDifficulty,
		shareStart:  time.Now(),
	}
}

// Hashrate returns estimated hashrate based on shares submitted and difficulty.
func (s *Session) Hashrate() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	elapsed := time.Since(s.shareStart).Seconds()
	if elapsed < 1 || s.shareCount == 0 {
		return 0
	}
	// Hashrate = (shares * difficulty) / time
	// For RandomX: difficulty represents the expected number of hashes per share
	return float64(s.shareCount) * s.difficulty / elapsed
}

// Username returns the authorized username for this session.
func (s *Session) Username() string {
	return s.username
}

// Serve reads stratum JSON-RPC requests line-by-line and writes responses.
func (s *Session) Serve() {
	log.Printf("conn %s opened", s.conn.RemoteAddr())
	s.metrics.ConnOpened()
	defer log.Printf("conn %s closed", s.conn.RemoteAddr())
	defer s.metrics.ConnClosed()

	scanner := bufio.NewScanner(s.rw.Reader)
	for scanner.Scan() {
		line := scanner.Bytes()
		var req Request
		if err := json.Unmarshal(line, &req); err != nil {
			s.writeError(nil, -32700, fmt.Sprintf("parse error: %v", err))
			continue
		}
		if err := s.handle(req); err != nil {
			s.writeError(req.ID, -32603, err.Error())
		}
	}
	if err := scanner.Err(); err != nil && err != io.EOF {
		log.Printf("conn %s read error: %v", s.conn.RemoteAddr(), err)
	}
}

func (s *Session) handle(req Request) error {
	switch req.Method {
	case "mining.subscribe":
		return s.handleSubscribe(req)
	case "mining.authorize":
		return s.handleAuthorize(req)
	case "mining.set_difficulty":
		// Client-initiated set_difficulty is uncommon; acknowledge.
		return s.writeResult(req.ID, true)
	case "mining.submit":
		return s.handleSubmit(req)
	case "login":
		// Monero/xmrig-style stratum login
		return s.handleLogin(req)
	case "submit":
		// Monero/xmrig-style stratum submit
		return s.handleXmrigSubmit(req)
	default:
		return fmt.Errorf("unknown method: %s", req.Method)
	}
}

func (s *Session) handleSubscribe(req Request) error {
	// Stratum V1 subscribe response structure: [[sub_details], extranonce1, extranonce2_size]
	result := []any{
		[]any{
			[]any{"mining.set_difficulty", "1"},
			[]any{"mining.notify", "1"},
		},
		s.extranonce1,
		job.ExtraNonce2Size,
	}
	s.subscribed = true
	if err := s.writeResult(req.ID, result); err != nil {
		return err
	}
	// Send initial difficulty if set.
	if s.difficulty > 0 {
		_ = s.sendSetDifficulty(s.difficulty)
	}
	// Send initial job so miner can start hashing.
	return s.sendNotify()
}

func (s *Session) handleAuthorize(req Request) error {
	// Accept all authorizations; capture username for accounting.
	var params []any
	if err := json.Unmarshal(req.Params, &params); err == nil && len(params) > 0 {
		if u, ok := params[0].(string); ok {
			s.username = u
		}
		// Optional second param as payout address.
		if len(params) > 1 {
			if addr, ok := params[1].(string); ok && addr != "" && s.store != nil {
				go s.store.SetPayoutAddress(context.Background(), s.username, addr)
			}
		}
	}
	if s.username == "" {
		s.username = "anonymous"
	}
	s.authorized = true
	return s.writeResult(req.ID, true)
}

// handleLogin handles Monero/xmrig-style login requests
func (s *Session) handleLogin(req Request) error {
	var params map[string]any
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return fmt.Errorf("bad login params: %v", err)
	}

	// Extract login (username) - for xmrig this is typically the wallet address
	if login, ok := params["login"].(string); ok && login != "" {
		s.username = login
	} else {
		s.username = "anonymous"
	}

	s.subscribed = true
	s.authorized = true
	s.isXmrig = true // Mark as xmrig client

	// For xmrig, the login IS the payout address - store it
	if s.store != nil && s.username != "anonymous" {
		go s.store.SetPayoutAddress(context.Background(), s.username, s.username)
	}

	// Generate response with job
	tmpl := s.getTemplate()
	if tmpl == nil {
		return fmt.Errorf("no template available")
	}

	// Build xmrig job response
	jobObj, err := s.buildXmrigJob(tmpl)
	if err != nil {
		return fmt.Errorf("build job: %v", err)
	}

	result := map[string]any{
		"id":     s.extranonce1, // Use extranonce1 as miner ID
		"job":    jobObj,
		"status": "OK",
	}

	return s.writeResult(req.ID, result)
}

// buildXmrigJob creates a job object for xmrig/Monero stratum
func (s *Session) buildXmrigJob(tmpl *job.Template) (map[string]any, error) {
	// For Juno with RandomX:
	// - blob = header without solution (version|prevhash|merkleroot|blockcommit|time|bits|nonce_placeholder)
	// - seed_hash = RandomX seed from template
	// - target = difficulty target as hex
	//
	// NOTE: For Zcash/Juno v5 transactions, the txid is computed via ZIP-244 using BLAKE2b,
	// not double-SHA256. Rather than implement ZIP-244 txid computation, we use the
	// pre-computed merkle root from getblocktemplate and DON'T modify the coinbase.
	// The 32-byte nonce field provides ample space for miner differentiation.
	merkleRoot := tmpl.MerkleRoot

	// Build header without nonce (nonce will be set by miner)
	// Format: version(4) + prevhash(32) + merkleroot(32) + blockcommit(32) + time(4) + bits(4) + nonce(32)
	blob, err := job.BuildHeaderBlobJuno(tmpl.Version, tmpl.PrevHash, merkleRoot, tmpl.BlockCommit, tmpl.CurTime, tmpl.Bits)
	if err != nil {
		return nil, err
	}

	// Compute target from difficulty
	target := job.TargetToCompact(s.difficulty, tmpl.Bits)

	jobObj := map[string]any{
		"job_id":    tmpl.JobID,
		"blob":      blob,
		"target":    target,
		"algo":      "rx/juno",
		"height":    tmpl.Height,
		"seed_hash": tmpl.RandomXSeed,
	}

	return jobObj, nil
}

// handleXmrigSubmit handles Monero/xmrig-style share submissions
func (s *Session) handleXmrigSubmit(req Request) error {
	if !s.subscribed || !s.authorized {
		return s.fail(fmt.Errorf("unauthorized"))
	}

	var params map[string]any
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return s.fail(fmt.Errorf("bad params: %v", err))
	}

	jobID, ok := params["job_id"].(string)
	if !ok {
		return s.fail(fmt.Errorf("missing job_id"))
	}

	nonceHex, ok := params["nonce"].(string)
	if !ok {
		return s.fail(fmt.Errorf("missing nonce"))
	}

	resultHex, ok := params["result"].(string)
	if !ok {
		return s.fail(fmt.Errorf("missing result"))
	}

	// Juno uses 32-byte nonce (64 hex chars)
	if len(nonceHex) != 64 {
		return s.fail(fmt.Errorf("wrong nonce size: expected 64 hex chars, got %d", len(nonceHex)))
	}

	nonceBin, err := hex.DecodeString(nonceHex)
	if err != nil {
		return s.fail(fmt.Errorf("bad nonce hex: %v", err))
	}

	resultBin, err := hex.DecodeString(resultHex)
	if err != nil {
		return s.fail(fmt.Errorf("bad result hex: %v", err))
	}

	if len(resultBin) != 32 {
		return s.fail(fmt.Errorf("wrong result size"))
	}

	jobTmpl := s.getJob(jobID)
	if jobTmpl == nil {
		return s.fail(fmt.Errorf("stale job"))
	}

	// Use the pre-computed merkle root from the template (ZIP-244 compatible)
	// We don't modify the coinbase, so the original merkle root is correct
	merkleRoot := jobTmpl.MerkleRoot
	coinbaseHex := jobTmpl.CoinbaseTxn

	// The solution for Juno is the 32-byte RandomX hash result
	solution := resultBin

	header, err := job.BuildHeaderJuno(jobTmpl.Version, jobTmpl.PrevHash, merkleRoot, jobTmpl.BlockCommit, uint32(jobTmpl.CurTime), jobTmpl.Bits, nonceHex, solution)
	if err != nil {
		return s.fail(fmt.Errorf("build header: %v", err))
	}

	// Verify the RandomX hash
	headerBlob, err := job.GetHeaderInputJuno(jobTmpl.Version, jobTmpl.PrevHash, merkleRoot, jobTmpl.BlockCommit, jobTmpl.CurTime, jobTmpl.Bits, nonceBin)
	if err != nil {
		return s.fail(fmt.Errorf("header input: %v", err))
	}

	// Verify RandomX hash matches (need to compute RandomX hash)
	// For now, trust the miner's result and check if it meets target
	// TODO: Actually verify RandomX hash

	// Check share difficulty using the last 8 bytes of the result hash (little-endian uint64)
	// This matches how xmrig compares: value = *(uint64_t*)(hash + 24)
	if len(resultBin) < 32 {
		return s.fail(fmt.Errorf("result hash too short"))
	}
	// Last 8 bytes as little-endian uint64
	shareValue := binary.LittleEndian.Uint64(resultBin[24:32])

	// Target for difficulty 1 is max uint64
	// For session difficulty d, target = max_uint64 / d
	maxTarget := uint64(0xFFFFFFFFFFFFFFFF)
	sessTarget := maxTarget
	if s.difficulty > 1 {
		sessTarget = uint64(float64(maxTarget) / s.difficulty)
	}

	if shareValue >= sessTarget {
		return s.fail(fmt.Errorf("low difficulty share"))
	}

	// Check if this is a valid block (meets network target)
	target, err := job.TargetFromBits(jobTmpl.Bits)
	if err != nil {
		return fmt.Errorf("target parse: %v", err)
	}

	// Compare the full 32-byte hash against the full 256-bit target
	// The hash from RandomX is in little-endian, convert to big.Int for comparison
	// Reverse the result bytes to get big-endian for big.Int
	resultLE := make([]byte, 32)
	copy(resultLE, resultBin)
	for i := 0; i < 16; i++ {
		resultLE[i], resultLE[31-i] = resultLE[31-i], resultLE[i]
	}
	hashValue := new(big.Int).SetBytes(resultLE)

	// Debug: log share and target values
	log.Printf("share: hashValue=%064x target=%064x (bits=%s)",
		hashValue, target, jobTmpl.Bits)

	// A share is a block if hashValue < target
	isBlock := hashValue.Cmp(target) < 0

	if isBlock {
		log.Printf("BLOCK FOUND! hash=%064x < target=%064x", hashValue, target)
	}

	s.adjustDifficulty()

	// Record share for PPLNS tracking (no balance credit until block found)
	if s.store != nil {
		go s.store.RecordShare(context.Background(), s.username, jobID, s.difficulty, true, false, false)
	}

	// PPS mode: credit balance immediately per share (only if PPSRewardPerDiff > 0)
	if s.store != nil && s.cfg.PPSRewardPerDiff > 0 {
		credit := s.difficulty * s.cfg.PPSRewardPerDiff
		var poolFee float64
		if s.cfg.PoolFeeBps > 0 {
			poolFee = credit * float64(s.cfg.PoolFeeBps) / 10000.0
			credit = credit - poolFee
		}
		go func(username string, credit, poolFee float64, feeAddr string) {
			ctx := context.Background()
			s.store.CreditBalance(ctx, username, credit)
			if poolFee > 0 && feeAddr != "" {
				s.store.CreditBalance(ctx, "__pool_fee__", poolFee)
			}
		}(s.username, credit, poolFee, s.cfg.PoolFeeAddress)
	}

	if isBlock {
		go s.submitBlock(jobTmpl, merkleRoot, coinbaseHex, uint32(jobTmpl.CurTime), nonceHex, solution)
	}

	s.metrics.ShareAccepted()
	s.mu.Lock()
	s.shareCount++
	s.lastShare = time.Now()
	s.mu.Unlock()

	// Ignore headerBlob for now to avoid unused variable error
	_ = headerBlob
	_ = header

	return s.writeResult(req.ID, map[string]string{"status": "OK"})
}

func (s *Session) handleSubmit(req Request) error {
	if !s.subscribed || !s.authorized {
		return s.fail(fmt.Errorf("unauthorized"))
	}

	var params []any
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return s.fail(fmt.Errorf("bad params: %v", err))
	}
	if len(params) < 6 {
		return s.fail(fmt.Errorf("missing params"))
	}
	jobID, ok := params[1].(string)
	if !ok {
		return s.fail(fmt.Errorf("invalid job id"))
	}
	extranonce2, ok := params[2].(string)
	if !ok {
		return s.fail(fmt.Errorf("invalid extranonce2"))
	}
	ntimeHex, ok := params[3].(string)
	if !ok {
		return s.fail(fmt.Errorf("invalid ntime"))
	}
	nonce, ok := params[4].(string)
	if !ok {
		return s.fail(fmt.Errorf("invalid nonce"))
	}
	solutionHex, ok := params[5].(string)
	if !ok {
		return s.fail(fmt.Errorf("invalid solution"))
	}

	jobTmpl := s.getJob(jobID)
	if jobTmpl == nil {
		return s.fail(fmt.Errorf("stale job"))
	}

	if len(extranonce2) != job.ExtraNonce2Size*2 {
		return s.fail(fmt.Errorf("wrong extranonce2 size"))
	}
	if _, err := hex.DecodeString(extranonce2); err != nil {
		return s.fail(fmt.Errorf("bad extranonce2 hex"))
	}
	if len(ntimeHex) != 8 {
		return s.fail(fmt.Errorf("wrong ntime size"))
	}
	ntimeVal, err := strconv.ParseUint(ntimeHex, 16, 32)
	if err != nil {
		return s.fail(fmt.Errorf("bad ntime"))
	}
	if ntimeVal < uint64(jobTmpl.Mintime) || ntimeVal > uint64(jobTmpl.CurTime)+900 {
		return s.fail(fmt.Errorf("ntime out of range"))
	}
	if _, err := hex.DecodeString(nonce); err != nil {
		return s.fail(fmt.Errorf("bad nonce hex"))
	}
	if len(nonce) != 64 {
		return s.fail(fmt.Errorf("wrong nonce size"))
	}
	solution, err := hex.DecodeString(solutionHex)
	if err != nil {
		return s.fail(fmt.Errorf("bad solution hex"))
	}
	if len(solution) == 0 {
		return s.fail(fmt.Errorf("empty solution"))
	}

	// Use the pre-computed merkle root from the template (ZIP-244 compatible)
	// We don't modify the coinbase, so the original merkle root is correct
	merkleRoot := jobTmpl.MerkleRoot
	coinbaseHex := jobTmpl.CoinbaseTxn

	header, err := job.BuildHeaderJuno(jobTmpl.Version, jobTmpl.PrevHash, merkleRoot, jobTmpl.BlockCommit, uint32(ntimeVal), jobTmpl.Bits, nonce, solution)
	if err != nil {
		return s.fail(fmt.Errorf("build header: %v", err))
	}
	hash := job.HashHeader(header)

	target, err := job.TargetFromHex(jobTmpl.Target)
	if err != nil || target == nil {
		target, err = job.TargetFromBits(jobTmpl.Bits)
		if err != nil {
			return fmt.Errorf("target parse: %v", err)
		}
	}
	share := new(big.Int).SetBytes(hash)
	// Apply session difficulty: target /= difficulty
	sessTarget := new(big.Float).SetInt(target)
	if s.difficulty > 0 {
		sessTarget = sessTarget.Quo(sessTarget, big.NewFloat(s.difficulty))
	}
	adjTarget, _ := sessTarget.Int(nil)
	if adjTarget.Sign() == 0 {
		adjTarget = big.NewInt(1)
	}
	if share.Cmp(adjTarget) > 0 {
		return s.fail(fmt.Errorf("low difficulty share"))
	}
	isBlock := target != nil && share.Cmp(target) <= 0

	s.adjustDifficulty()

	// Record share for PPLNS tracking (no balance credit until block found)
	if s.store != nil {
		go s.store.RecordShare(context.Background(), s.username, jobID, s.difficulty, true, false, false)
	}

	// PPS mode: credit balance immediately per share (only if PPSRewardPerDiff > 0)
	if s.store != nil && s.cfg.PPSRewardPerDiff > 0 {
		credit := s.difficulty * s.cfg.PPSRewardPerDiff
		var poolFee float64
		if s.cfg.PoolFeeBps > 0 {
			poolFee = credit * float64(s.cfg.PoolFeeBps) / 10000.0
			credit = credit - poolFee
		}
		go func(username string, credit, poolFee float64, feeAddr string) {
			ctx := context.Background()
			s.store.CreditBalance(ctx, username, credit)
			if poolFee > 0 && feeAddr != "" {
				s.store.CreditBalance(ctx, "__pool_fee__", poolFee)
			}
		}(s.username, credit, poolFee, s.cfg.PoolFeeAddress)
	}

	if isBlock {
		go s.submitBlock(jobTmpl, merkleRoot, coinbaseHex, uint32(ntimeVal), nonce, solution)
	}

	s.metrics.ShareAccepted()
	s.mu.Lock()
	s.shareCount++
	s.lastShare = time.Now()
	s.mu.Unlock()
	return s.writeResult(req.ID, true)
}

func (s *Session) submitBlock(tmpl *job.Template, merkleRoot string, coinbaseHex string, ntime uint32, nonceHex string, solution []byte) {
	if s.submitter == nil {
		log.Printf("block found but submitter unavailable")
		return
	}
	s.metrics.BlockFound(tmpl.Height, tmpl.JobID)

	// Build the block hash for recording
	var blockHash string
	if header, err := job.BuildHeaderJuno(tmpl.Version, tmpl.PrevHash, merkleRoot, tmpl.BlockCommit, ntime, tmpl.Bits, nonceHex, solution); err == nil {
		hash := job.HashHeader(header)
		blockHash = job.ReverseHex(hash)
	}

	blockHex, err := job.BuildBlockJuno(tmpl.Version, tmpl.PrevHash, merkleRoot, tmpl.BlockCommit, ntime, tmpl.Bits, nonceHex, solution, coinbaseHex, tmpl.Transactions)
	if err != nil {
		log.Printf("block build failed: %v", err)
		s.metrics.BlockSubmitted(false)
		return
	}
	log.Printf("DEBUG block submission: height=%d nonce=%s solution=%x", tmpl.Height, nonceHex, solution)
	log.Printf("DEBUG block hex (first 500 chars): %s", blockHex[:min(500, len(blockHex))])
	log.Printf("DEBUG block hex length: %d", len(blockHex))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.submitter.SubmitBlock(ctx, blockHex); err != nil {
		log.Printf("block submit failed: %v", err)
		s.metrics.BlockSubmitted(false)
		return
	}
	s.metrics.BlockSubmitted(true)
	// Only record blocks that were successfully submitted
	if s.store != nil && blockHash != "" {
		go s.store.RecordBlock(context.Background(), tmpl.JobID, tmpl.Height, blockHash, "submitted")
	}
	log.Printf("block submitted height=%d job=%s", tmpl.Height, tmpl.JobID)
}

func (s *Session) adjustDifficulty() {
	if s.cfg.VardiffTargetMS <= 0 || s.cfg.VardiffRetargetSecs <= 0 {
		return
	}
	now := time.Now()
	if s.lastShare.IsZero() {
		s.lastShare = now
		s.lastRetgt = now
		return
	}
	gap := now.Sub(s.lastShare)
	s.lastShare = now
	if now.Sub(s.lastRetgt) < time.Duration(s.cfg.VardiffRetargetSecs)*time.Second {
		return
	}
	target := time.Duration(s.cfg.VardiffTargetMS) * time.Millisecond
	if gap < target/2 {
		s.difficulty *= 1.5
		s.lastRetgt = now
		_ = s.sendSetDifficulty(s.difficulty)
	} else if gap > target*2 {
		s.difficulty /= 1.5
		if s.difficulty < 0.01 {
			s.difficulty = 0.01
		}
		s.lastRetgt = now
		_ = s.sendSetDifficulty(s.difficulty)
	}
}

// sendNotify emits a mining.notify payload using the latest template (if any).
func (s *Session) sendNotify() error {
	tmpl := s.getTemplate()
	if tmpl == nil {
		return fmt.Errorf("no template available")
	}
	return s.sendNotifyWithTemplate(tmpl)
}

// PushTemplate sends a notify for the given template if subscribed.
func (s *Session) PushTemplate(tmpl *job.Template) error {
	if !s.subscribed {
		return nil
	}
	return s.sendNotifyWithTemplate(tmpl)
}

func (s *Session) sendNotifyWithTemplate(tmpl *job.Template) error {
	// If xmrig client, send job notification in Monero stratum format
	if s.isXmrig {
		return s.sendXmrigJob(tmpl)
	}

	// Standard Zcash-style stratum notify
	coinb1, coinb2, err := job.SplitCoinbase(tmpl.CoinbaseTxn, s.extranonce1, job.ExtraNonce2Size)
	if err != nil {
		return err
	}
	notify := []any{
		tmpl.JobID,
		tmpl.PrevHash,
		coinb1,
		coinb2,
		tmpl.MerkleBranches,
		fmt.Sprintf("%08x", tmpl.Version),
		tmpl.Bits,
		fmt.Sprintf("%x", tmpl.CurTime),
		true,
	}
	resp := Response{ID: nil, Result: notify, Error: nil, Method: "mining.notify"}
	return s.write(resp)
}

// sendXmrigJob sends a job notification in Monero/xmrig stratum format
func (s *Session) sendXmrigJob(tmpl *job.Template) error {
	jobObj, err := s.buildXmrigJob(tmpl)
	if err != nil {
		return err
	}
	// xmrig expects: {"jsonrpc":"2.0", "method":"job", "params": {...}}
	// We need to write this directly since Response.Params is []any but we need a map
	msg := map[string]any{
		"jsonrpc": "2.0",
		"method":  "job",
		"params":  jobObj,
	}
	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	if _, err := s.rw.Write(append(b, '\n')); err != nil {
		return err
	}
	return s.rw.Flush()
}

func (s *Session) sendSetDifficulty(diff float64) error {
	resp := Response{ID: nil, Method: "mining.set_difficulty", Params: []any{diff}}
	return s.write(resp)
}

func (s *Session) writeResult(id any, result any) error {
	resp := Response{ID: id, Result: result}
	return s.write(resp)
}

func (s *Session) writeError(id any, code int, msg string) error {
	resp := Response{ID: id, Error: &RespError{Code: code, Message: msg}}
	return s.write(resp)
}

func (s *Session) write(resp Response) error {
	b, err := json.Marshal(resp)
	if err != nil {
		return err
	}
	if _, err := s.rw.Write(append(b, '\n')); err != nil {
		return err
	}
	return s.rw.Flush()
}

func (s *Session) fail(err error) error {
	s.metrics.ShareInvalid()
	s.invalidCnt++
	if s.invalidCnt >= 20 {
		_ = s.conn.Close()
	}
	return err
}

// Request represents a minimal Stratum V1 JSON-RPC request.
type Request struct {
	ID     any             `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

// Response represents a minimal Stratum V1 JSON-RPC response.
type Response struct {
	ID     any        `json:"id"`
	Result any        `json:"result,omitempty"`
	Error  *RespError `json:"error,omitempty"`
	Method string     `json:"method,omitempty"`
	Params []any      `json:"params,omitempty"`
}

// RespError matches JSON-RPC error shape.
type RespError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}
