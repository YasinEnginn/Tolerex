// ===================================================================================
// TOLEREX – LEADER SERVER (CONTROL & DATA PLANE ORCHESTRATION)
// ===================================================================================
//
// This file defines the Leader node in the Tolerex fault-tolerant storage cluster.
//
// Responsibilities of Leader:
// - Manage membership (register new members, track heartbeat/liveness).
// - Decide replication targets for storing messages (prefer least-loaded members).
// - Coordinate Store and Retrieve operations via gRPC with cluster members.
// - Maintain cluster state (members, message replicas) and persist it to disk for recovery.
// - Provide a local TCP control interface for operators (simple SET/GET commands).
//
// Concurrency Model:
// - A single mutex `s.stateMu` protects all shared state (Members, MemberLog, MsgMap).
// - Network calls and disk I/O are performed outside the lock by snapshotting state.
//
// Security:
// - All Leader-to-Member gRPC calls use mutual TLS for authentication.
// ===================================================================================

package leader

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"tolerex/internal/config"
	"tolerex/internal/logger"
	"tolerex/internal/security"
	"tolerex/internal/server/balancer"
	"tolerex/internal/server/shared"
	pb "tolerex/proto/gen"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// MemberInfo is now in tolerex/internal/server/shared

// --- LEADER SERVER ---
// Implements the gRPC service and holds cluster coordination state.
type LeaderServer struct {
	pb.UnimplementedStorageServiceServer

	// --- CLUSTER MEMBERSHIP ---
	Members   []string
	Tolerance int

	// --- MESSAGE → REPLICA ADDRESSES MAP ---
	MsgMap map[int][]string

	// --- MEMBER METADATA REGISTRY ---
	MemberLog map[string]*shared.MemberInfo

	// --- LOAD BALANCER STRATEGY ---
	Balancer balancer.LoadBalancer

	// --- SHARED STATE LOCK ---
	stateMu sync.Mutex

	// --- mTLS DIAL OPTION FOR LEADER→MEMBER RPC ---
	memberDialOpt grpc.DialOption

	// Optional function hooks for testing overrides.
	dialFn      func(addr string) (*grpc.ClientConn, error)
	replicateFn func(ctx context.Context, addr string, msg *pb.StoredMessage) bool

	// --- CONNECTION POOL ---
	conns   map[string]*grpc.ClientConn
	connsMu sync.Mutex

	// --- RUNTIME LISTENERS ---
	TcpListener  net.Listener // Operator TCP console
	GrpcListener net.Listener // gRPC service listener

	// --- HEARTBEAT WATCHER CONFIG (TEST OVERRIDES) ---
	heartbeatTimeout  time.Duration
	heartbeatInterval time.Duration
	heartbeatStop     chan struct{}
	heartbeatDone     chan struct{}
	heartbeatMu       sync.Mutex
}

// --- PERSISTED LEADER STATE ---
// Struct for JSON serialization of Leader state (Members, MemberLog, MsgMap).
type LeaderState struct {
	Members   []string
	MemberLog map[string]*shared.MemberInfo
	MsgMap    map[int][]string
}

func stateFilePath() string {
	if os.Getenv("TOLEREX_TEST_MODE") == "1" {
		if d := os.Getenv("TOLEREX_TEST_DIR"); d != "" {
			return filepath.Join(d, "leader_state.json")
		}
	}
	return "internal/data/leader_state.json"
}

const (
	defaultHeartbeatTimeout  = 15 * time.Second
	defaultHeartbeatInterval = 5 * time.Second
	rpcTimeout               = 2 * time.Second
)

// ===================================================================================
// gRPC: HEARTBEAT
// ===================================================================================

// --- HEARTBEAT RPC ---
// Updates a Member's liveness timestamp and marks it alive.
func (s *LeaderServer) Heartbeat(ctx context.Context, hb *pb.HeartbeatRequest) (*emptypb.Empty, error) {
	log := logger.WithContext(ctx, logger.Leader)

	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	member, ok := s.MemberLog[hb.Address]
	if !ok {
		logger.Warn(log, "Heartbeat from unknown member: %s", hb.Address)
		return &emptypb.Empty{}, nil
	}

	member.LastSeen = time.Now()
	member.Alive = true
	logger.Debug(log, "Heartbeat received from %s", hb.Address)
	return &emptypb.Empty{}, nil
}

// ===================================================================================
// LIVENESS SUPERVISION
// ===================================================================================

// --- HEARTBEAT WATCHER ---
// Periodically checks liveness and marks Members DOWN on timeout.
func (s *LeaderServer) StartHeartbeatWatcher() {
	timeout := s.heartbeatTimeout
	if timeout <= 0 {
		timeout = defaultHeartbeatTimeout
	}
	interval := s.heartbeatInterval
	if interval <= 0 {
		interval = defaultHeartbeatInterval
	}

	s.heartbeatMu.Lock()
	if s.heartbeatStop != nil {
		s.heartbeatMu.Unlock()
		logger.Warn(logger.Leader, "Heartbeat watcher already running")
		return
	}
	stopCh := make(chan struct{})
	doneCh := make(chan struct{})
	s.heartbeatStop = stopCh
	s.heartbeatDone = doneCh
	s.heartbeatMu.Unlock()

	logger.Info(logger.Leader, "Heartbeat watcher started (timeout=%s, interval=%s)", timeout, interval)

	go func() {
		defer close(doneCh)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				logger.Debug(logger.Leader, "Heartbeat watcher tick")

				s.stateMu.Lock()
				now := time.Now()
				changed := false

				for _, m := range s.MemberLog {
					if m.LastSeen.IsZero() {
						m.LastSeen = m.AddedAt
					}
					if now.Sub(m.LastSeen) > timeout && m.Alive {
						logger.Warn(logger.Leader,
							"Member marked DOWN: addr=%s lastSeen=%s addedAt=%s",
							m.Address,
							m.LastSeen.Format(time.RFC3339),
							m.AddedAt.Format(time.RFC3339),
						)
						m.Alive = false
						changed = true
					}
				}

				s.stateMu.Unlock()

				if changed {
					_ = s.saveState()
				}
			case <-stopCh:
				logger.Info(logger.Leader, "Heartbeat watcher stopped")
				return
			}
		}
	}()
}

func (s *LeaderServer) StopHeartbeatWatcher() {
	s.heartbeatMu.Lock()
	stopCh := s.heartbeatStop
	doneCh := s.heartbeatDone
	s.heartbeatStop = nil
	s.heartbeatDone = nil
	s.heartbeatMu.Unlock()

	if stopCh != nil {
		close(stopCh)
	}
	if doneCh != nil {
		<-doneCh
	}
}

// ===================================================================================
// LEADER INITIALIZATION & SECURITY
// ===================================================================================

// --- NEW LEADER SERVER ---
// Loads config, initializes registries and mTLS, and restores any persisted state.
func NewLeaderServer(members []string, toleranceFile string) (*LeaderServer, error) {
	logger.Info(logger.Leader, "LeaderServer initialization started")
	logger.Info(logger.Leader, "Tolerance file path: %s", toleranceFile)

	tolerance, err := config.ReadTolerance(toleranceFile)
	if err != nil {
		logger.Error(logger.Leader, "Failed to read tolerance config: %v", err)
		return nil, err
	}

	memberLog := make(map[string]*shared.MemberInfo)
	now := time.Now()
	for _, addr := range members {
		memberLog[addr] = &shared.MemberInfo{
			Address:    addr,
			AddedAt:    now,
			MessageCnt: 0,
			LastSeen:   now,
			Alive:      true,
		}
	}

	leader := &LeaderServer{
		Members:   members,
		Tolerance: tolerance,
		MsgMap:    make(map[int][]string),
		MemberLog: memberLog,
		Balancer:  &balancer.LeastLoadedBalancer{},
		conns:     make(map[string]*grpc.ClientConn),
	}

	// Default load balancer
	strategy := os.Getenv("BALANCER_STRATEGY")
	if strategy == shared.StrategyP2C {
		logger.Info(logger.Leader, "Using Load Balancer: Power of Two Choices (P2C)")
		leader.Balancer = &balancer.PowerOfTwoChoicesBalancer{}
	} else {
		logger.Info(logger.Leader, "Using Load Balancer: Least Loaded (Default)")
		leader.Balancer = &balancer.LeastLoadedBalancer{}
	}

	// Prepare mTLS credentials for Leader→Member RPC (if not in test mode).
	if os.Getenv("TOLEREX_TEST_MODE") != "1" {
		creds, err := security.NewMTLSClientCreds(
			"config/tls/leader.crt",
			"config/tls/leader.key",
			"config/tls/ca.crt",
			"member",
		)
		if err != nil {
			logger.Error(logger.Leader, "Failed to initialize mTLS creds: %v", err)
			return nil, fmt.Errorf("failed to init leader->member mTLS creds: %w", err)
		}
		leader.memberDialOpt = grpc.WithTransportCredentials(creds)
	}

	// Load persisted state from disk (best-effort).
	if err := leader.loadState(); err != nil {
		logger.Warn(logger.Leader, "Failed to load leader state: %v", err)
	}

	return leader, nil
}

// ===================================================================================
// gRPC: MEMBER REGISTRATION
// ===================================================================================

func (s *LeaderServer) RegisterMember(ctx context.Context, req *pb.MemberInfo) (*pb.RegisterReply, error) {
	log := logger.WithContext(ctx, logger.Leader)

	s.stateMu.Lock()
	addr := req.Address

	// Re-register / recovery
	if m, exists := s.MemberLog[addr]; exists {
		if !m.Alive {
			logger.Info(log, "Member recovered: %s", addr)
		} else {
			logger.Debug(log, "Member already registered and alive: %s", addr)
		}
		m.Alive = true
		m.LastSeen = time.Now()
		s.stateMu.Unlock()
		return &pb.RegisterReply{Ok: true}, nil
	}

	// New member
	now := time.Now()
	s.Members = append(s.Members, addr)
	s.MemberLog[addr] = &shared.MemberInfo{
		Address:    addr,
		AddedAt:    now,
		MessageCnt: 0,
		LastSeen:   now,
		Alive:      true,
	}
	s.stateMu.Unlock()

	if err := s.saveState(); err != nil {
		logger.Warn(log, "Failed to persist new member state: %v", err)
	}

	logger.Info(log, "New member registered: %s", addr)
	return &pb.RegisterReply{Ok: true}, nil
}

// ===================================================================================
// LEADER → MEMBER CONNECTIVITY
// ===================================================================================

func (s *LeaderServer) dialMember(addr string) (*grpc.ClientConn, error) {
	if s.dialFn != nil {
		return s.dialFn(addr)
	}

	s.connsMu.Lock()
	defer s.connsMu.Unlock()

	// 1. Check if a valid connection exists
	if conn, ok := s.conns[addr]; ok {
		// Verify if state is shutdown/transient failure?
		// gRPC usually handles transient reconnection automatically.
		// Only remove if it's explicitly Shutdown or closed.
		if conn.GetState() != connectivity.Shutdown {
			return conn, nil
		}
		// If shutdown, fallthrough to dial new one
		delete(s.conns, addr)
	}

	logger.Debug(logger.Leader, "Dialing member %s with mTLS (New Connection)", addr)
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		addr,
		s.memberDialOpt,
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(),
	)

	if err != nil {
		return nil, err
	}

	// 2. Add to pool
	s.conns[addr] = conn
	return conn, nil
}

// ===================================================================================
// gRPC: STORE (REPLICATION COORDINATION)
// ===================================================================================

func (s *LeaderServer) Store(ctx context.Context, msg *pb.StoredMessage) (*pb.StoreResult, error) {
	log := logger.WithContext(ctx, logger.Leader)

	logger.Info(log,
		"Store request received: msg_id=%d payload_bytes=%d tolerance=%d",
		msg.Id,
		len(msg.Text),
		s.Tolerance,
	)

	if len(msg.Text) > 1_000_000 {
		logger.Warn(log, "Large payload detected: %d bytes", len(msg.Text))
	}

	// Snapshot candidates under lock (Alive + MessageCnt)
	s.stateMu.Lock()
	candidates := make([]balancer.MemberLoad, 0, len(s.Members))
	for _, addr := range s.Members {
		info := s.MemberLog[addr]
		if info == nil || !info.Alive {
			logger.Debug(log, "Skipping DOWN member for replication: %s", addr)
			continue
		}
		candidates = append(candidates, balancer.MemberLoad{
			Addr:  addr,
			Count: info.MessageCnt,
		})
	}
	tol := s.Tolerance
	lb := s.Balancer
	s.stateMu.Unlock()

	if lb == nil {
		lb = &balancer.LeastLoadedBalancer{}
	}

	// Pick replication targets via balancer
	picked := lb.Pick(candidates, tol)

	selected := make([]string, 0, len(picked))
	for _, p := range picked {
		selected = append(selected, p.Addr)
		logger.Debug(log, "Selected member for replication: %s (msg_count=%d)", p.Addr, p.Count)
	}

	if len(selected) < tol {
		logger.Error(log, "Not enough alive members for replication: have=%d need=%d", len(selected), tol)
		return &pb.StoreResult{Ok: false, Err: "Not enough alive members"}, nil
	}

	logger.Debug(log, "Replication targets selected: %v", selected)

	// --- CONCURRENT REPLICATION ---
	var wg sync.WaitGroup
	results := make(chan string, len(selected))

	// We need 'tol' successes. If we get them, we are good.
	// We'll launch all attempts in parallel.

	for _, addr := range selected {
		wg.Add(1)
		go func(targetAddr string) {
			defer wg.Done()

			var success bool
			if s.replicateFn != nil {
				logger.Debug(log, "Using test replicateFn for member %s", targetAddr)
				success = s.replicateFn(ctx, targetAddr, msg)
			} else {
				// Dial (reuses connection)
				conn, err := s.dialMember(targetAddr)
				if err != nil {
					logger.Warn(log, "Connection error to %s: %v", targetAddr, err)
					return
				}

				// Do NOT close conn here, it is pooled.

				client := pb.NewStorageServiceClient(conn)
				ctx2, cancel := context.WithTimeout(ctx, rpcTimeout)
				res, callErr := client.Store(ctx2, msg)
				cancel()

				if callErr == nil && res != nil && res.Ok {
					logger.Debug(log, "Replication succeeded: msg_id=%d member=%s", msg.Id, targetAddr)
					success = true
				} else {
					logger.Error(log, "Replication failed: msg_id=%d member=%s err=%v", msg.Id, targetAddr, callErr)
				}
			}

			if success {
				results <- targetAddr
			}
		}(addr)
	}

	// Close results channel once all goroutines are done
	go func() {
		wg.Wait()
		close(results)
	}()

	successful := make([]string, 0, tol)
	for addr := range results {
		successful = append(successful, addr)
		// Optimization: If we have enough, we could break?
		// But we need to drain channel or wait for wg to avoid leaks if we stop early?
		// Actually, since we close channel after wg.Wait(), reading until close is safe and correct.
		// It waits for ALL attempts to finish.
		// If we want to return EARLY (latency optimization), we'd need a more complex context cancel pattern.
		// For now, let's wait for all (robustness) but execute in PARALLEL (throughput).
	}

	// Update state based on replication results.
	if len(successful) > 0 {
		s.stateMu.Lock()

		for _, addr := range successful {
			if s.MemberLog[addr] != nil {
				s.MemberLog[addr].MessageCnt++
			}
		}

		full := len(successful) >= tol
		if full {
			logger.Debug(log, "Updating MsgMap for msg_id=%d replicas=%v", msg.Id, successful)
			s.MsgMap[int(msg.Id)] = append([]string(nil), successful...)
		}

		s.stateMu.Unlock()

		if full {
			logger.Info(log, "Store replication successful: msg_id=%d replicas=%v", msg.Id, successful)
			_ = s.saveState()
			return &pb.StoreResult{Ok: true}, nil
		}
	}

	return &pb.StoreResult{Ok: false, Err: "Replication incomplete"}, nil
}

// ===================================================================================
// gRPC: RETRIEVE (READ PATH)
// ===================================================================================

func (s *LeaderServer) Retrieve(ctx context.Context, req *pb.MessageID) (*pb.StoredMessage, error) {
	// WARNING: test-only shortcut.
	// If TOLEREX_TEST_MODE=1 is enabled outside tests, Retrieve will bypass
	// member lookup and always return a dummy payload.
	if os.Getenv("TOLEREX_TEST_MODE") == "1" {
		return &pb.StoredMessage{Id: req.Id, Text: "dummy"}, nil
	}

	log := logger.WithContext(ctx, logger.Leader)
	logger.Info(log, "Retrieve request received: msg_id=%d", req.Id)

	s.stateMu.Lock()
	replicas, ok := s.MsgMap[int(req.Id)]
	members := append([]string(nil), s.Members...)
	s.stateMu.Unlock()

	var targets []string
	if ok && len(replicas) > 0 {
		targets = replicas
	} else {
		logger.Warn(log, "Replica map empty for msg_id=%d, trying all members", req.Id)
		targets = members
	}

	for _, addr := range targets {
		s.stateMu.Lock()
		info := s.MemberLog[addr]
		alive := info != nil && info.Alive
		s.stateMu.Unlock()

		if !alive {
			continue
		}

		conn, err := s.dialMember(addr)
		if err != nil {
			logger.Warn(log, "Retrieve connect failed to %s: %v", addr, err)
			continue
		}

		client := pb.NewStorageServiceClient(conn)
		ctx2, cancel := context.WithTimeout(ctx, rpcTimeout)
		res, callErr := client.Retrieve(ctx2, req)
		cancel()

		if callErr == nil && res != nil {
			logger.Info(log, "Retrieve succeeded from %s: msg_id=%d", addr, req.Id)
			return res, nil
		}
	}

	return nil, status.Error(codes.NotFound, "message not found")
}

// ===================================================================================
// TCP CONTROL PLANE (LOCAL OPERATOR INTERFACE)
// ===================================================================================

func (s *LeaderServer) HandleClient(conn net.Conn) {
	defer conn.Close()

	reqID := fmt.Sprintf("tcp-%d", time.Now().UnixNano())
	ctx := context.WithValue(context.Background(), logger.RequestIDKey, reqID)
	log := logger.WithContext(ctx, logger.Leader)
	logger.Info(log, "TCP client connected: %s", conn.RemoteAddr())

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	banner := []string{
		"--------------------------------------------------",
		"TOLEREX - Leader Node (Control Plane)",
		"--------------------------------------------------",
		"",
		"Basic Commands:",
		"  SET <id> <message>        Store a message",
		"  <N> SET <message>         Bulk store N messages",
		"  GET <id>                  Retrieve a message",
		"",
		"Utility:",
		"  HELP                      Show this help",
		"  QUIT | EXIT               Disconnect client",
		"",
		"Examples:",
		"  SET 10 HELLO",
		"  1000 SET HELLO",
		"--------------------------------------------------",
	}

	for _, line := range banner {
		fmt.Fprint(writer, line+"\r\n")
	}
	fmt.Fprint(writer, "tolerex> ")
	_ = writer.Flush()

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				logger.Warn(log, "TCP client error: %v", err)
			}
			return
		}

		line = strings.TrimSpace(line)
		if line == "" {
			fmt.Fprint(writer, "tolerex> ")
			_ = writer.Flush()
			continue
		}

		logger.Debug(log, "TCP command received: %s", line)

		parts := strings.Fields(line)
		cmd := strings.ToUpper(parts[0])

		switch cmd {
		case "SET":
			if len(parts) < 3 {
				fmt.Fprint(writer, "ERROR: SET <id> <message>\r\n")
				logger.Warn(log, "Invalid SET command format")
				break
			}

			id, err := strconv.Atoi(parts[1])
			if err != nil {
				fmt.Fprint(writer, "ERROR: id must be numeric\r\n")
				logger.Warn(log, "Non-numeric id in SET command: %s", parts[1])
				break
			}

			msg := strings.Join(parts[2:], " ")
			res, _ := s.Store(ctx, &pb.StoredMessage{Id: int32(id), Text: msg})
			if res != nil && res.Ok {
				fmt.Fprint(writer, "OK\r\n")
				logger.Info(log, "SET command succeeded: id=%d", id)
			} else {
				fmt.Fprint(writer, "ERROR: store failed\r\n")
				if res != nil {
					logger.Error(log, "SET command failed: id=%d err=%v", id, res.Err)
				} else {
					logger.Error(log, "SET command failed: id=%d err=nil result", id)
				}
			}

		case "GET":
			if len(parts) != 2 {
				fmt.Fprint(writer, "ERROR: GET <id>\r\n")
				logger.Warn(log, "Invalid GET command format")
				break
			}

			id, err := strconv.Atoi(parts[1])
			if err != nil {
				fmt.Fprint(writer, "ERROR: id must be numeric\r\n")
				logger.Warn(log, "Non-numeric id in GET command: %s", parts[1])
				break
			}

			res, err := s.Retrieve(ctx, &pb.MessageID{Id: int32(id)})
			if err != nil {
				st, ok := status.FromError(err)
				if ok && st.Code() == codes.NotFound {
					fmt.Fprint(writer, "NOT_FOUND\r\n")
					logger.Info(log, "GET command: message not found id=%d", id)
				} else {
					fmt.Fprint(writer, "ERROR: retrieve failed\r\n")
					logger.Error(log, "GET command failed: id=%d err=%v", id, err)
				}
				break
			}

			fmt.Fprint(writer, res.Text+"\r\n")

		case "HELP":
			for _, line := range banner {
				fmt.Fprint(writer, line+"\r\n")
			}

		case "QUIT", "EXIT":
			fmt.Fprint(writer, "Bye\r\n")
			_ = writer.Flush()
			logger.Info(log, "TCP client disconnected: %s", conn.RemoteAddr())
			return

		default:
			fmt.Fprint(writer, "ERROR: unknown command\r\n")
		}

		fmt.Fprint(writer, "tolerex> ")
		_ = writer.Flush()
	}
}

// ===================================================================================
// STATE PERSISTENCE (SAVE/LOAD)
// ===================================================================================

func (s *LeaderServer) saveState() error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.saveStateUnsafe()
}

func (s *LeaderServer) saveStateUnsafe() error {
	members := append([]string(nil), s.Members...)

	memberLog := make(map[string]*shared.MemberInfo, len(s.MemberLog))
	for k, v := range s.MemberLog {
		if v == nil {
			continue
		}
		cp := *v
		memberLog[k] = &cp
	}

	msgMap := make(map[int][]string, len(s.MsgMap))
	for id, addrs := range s.MsgMap {
		msgMap[id] = append([]string(nil), addrs...)
	}

	logger.Info(logger.Leader, "Persisting Leader state: members=%d msgMap entries=%d", len(members), len(s.MsgMap))

	data, err := json.MarshalIndent(LeaderState{
		Members:   members,
		MemberLog: memberLog,
		MsgMap:    msgMap,
	}, "", "  ")
	if err != nil {
		logger.Error(logger.Leader, "Failed to serialize Leader state: %v", err)
		return err
	}

	path := stateFilePath()
	_ = os.MkdirAll(filepath.Dir(path), 0755)

	err = os.WriteFile(path, data, 0644)
	if err == nil {
		logger.Info(logger.Leader, "Leader state persisted successfully")
	}
	return err
}

func (s *LeaderServer) loadState() error {
	data, err := os.ReadFile(stateFilePath())
	if err != nil {
		if os.IsNotExist(err) {
			logger.Info(logger.Leader, "No persisted Leader state found, starting fresh")
			return nil
		}
		return err
	}

	var state LeaderState
	if err := json.Unmarshal(data, &state); err != nil {
		logger.Error(logger.Leader, "Failed to deserialize Leader state: %v", err)
		return err
	}

	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	if state.MemberLog == nil {
		logger.Warn(logger.Leader, "Persisted Leader state has nil MemberLog, initializing empty map")
		state.MemberLog = make(map[string]*shared.MemberInfo)
	}
	if state.MsgMap == nil {
		logger.Warn(logger.Leader, "Persisted Leader state has nil MsgMap, initializing empty map")
		state.MsgMap = make(map[int][]string)
	}

	s.Members = state.Members
	s.MemberLog = state.MemberLog
	s.MsgMap = state.MsgMap

	logger.Info(logger.Leader, "Leader state loaded: members=%d, msgMap entries=%d", len(s.Members), len(s.MsgMap))
	return nil
}

// ===================================================================================
// OPERATOR VISIBILITY
// ===================================================================================

func (s *LeaderServer) PrintMemberStats() {
	s.stateMu.Lock()
	snapshot := make([]shared.MemberInfo, 0, len(s.MemberLog))
	for _, info := range s.MemberLog {
		if info != nil {
			snapshot = append(snapshot, *info)
		}
	}
	s.stateMu.Unlock()

	fmt.Print("\033[2J\033[H")
	fmt.Println("==== MEMBER STATUS (LIVE VIEW) ====")
	for _, info := range snapshot {
		fmt.Printf("Addr=%s | Alive=%v | LastSeen=%s | MsgCnt=%d\n",
			info.Address, info.Alive, info.LastSeen.Format("15:04:05"), info.MessageCnt)
	}
	fmt.Println("==================================")
}
func (s *LeaderServer) CloseAllConns() {
	s.connsMu.Lock()
	defer s.connsMu.Unlock()
	for addr, c := range s.conns {
		_ = c.Close()
		delete(s.conns, addr)
	}
}
