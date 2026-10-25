package leader

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"tolerex/internal/config"
	"tolerex/internal/logger"
	"tolerex/internal/server/balancer"
	"tolerex/internal/server/shared"
	pb "tolerex/proto/gen"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// LeaderServer coordinates membership, replicas, persistence, and client requests.
type LeaderServer struct {
	pb.UnimplementedStorageServiceServer

	Members   []string
	Tolerance int
	MsgMap    map[int][]string
	MemberLog map[string]*shared.MemberInfo

	Balancer  balancer.LoadBalancer
	stateMu   sync.Mutex
	persistMu sync.Mutex

	persistLoopMu sync.Mutex
	persistSignal chan struct{}
	persistStop   chan struct{}
	persistDone   chan error

	memberDialOpt grpc.DialOption

	dialFn      func(addr string) (*grpc.ClientConn, error)
	replicateFn func(ctx context.Context, addr string, msg *pb.StoredMessage) bool
	retrieveFn  func(ctx context.Context, addr string, req *pb.MessageID) (*pb.StoredMessage, error)

	conns   map[string]*grpc.ClientConn
	connsMu sync.Mutex

	TcpListener  net.Listener
	GrpcListener net.Listener
	clientsMu    sync.Mutex
	clients      map[net.Conn]struct{}
	clientsWG    sync.WaitGroup
	clientsClose bool

	heartbeatTimeout  time.Duration
	heartbeatInterval time.Duration
	heartbeatStop     chan struct{}
	heartbeatDone     chan struct{}
	heartbeatMu       sync.Mutex
}

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
	defaultPersistInterval   = 100 * time.Millisecond
	rpcTimeout               = 2 * time.Second
	maxMessageBytes          = 4 << 20
	maxCommandBytes          = maxMessageBytes + 128
	defaultExpectedMemberCN  = "member"
)

var errCommandTooLong = errors.New("command exceeds size limit")

func expectedMemberCN() string {
	if commonName := os.Getenv("LEADER_EXPECTED_MEMBER_CN"); commonName != "" {
		return commonName
	}
	return defaultExpectedMemberCN
}

func authorizeMember(ctx context.Context) error {
	peerInfo, ok := peer.FromContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing peer info")
	}
	tlsInfo, ok := peerInfo.AuthInfo.(credentials.TLSInfo)
	if !ok || len(tlsInfo.State.VerifiedChains) == 0 || len(tlsInfo.State.VerifiedChains[0]) == 0 {
		return status.Error(codes.Unauthenticated, "unverified peer certificate")
	}
	if commonName := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName; commonName != expectedMemberCN() {
		logger.Warn(logger.Leader, "Unauthorized member caller CN=%s", commonName)
		return status.Error(codes.PermissionDenied, "unauthorized caller")
	}
	return nil
}

func parseSetCommand(line string) (idText, message string, ok bool) {
	commandEnd := strings.IndexFunc(line, unicode.IsSpace)
	if commandEnd < 0 || !strings.EqualFold(line[:commandEnd], "SET") {
		return "", "", false
	}
	rest := strings.TrimLeftFunc(line[commandEnd:], unicode.IsSpace)
	idEnd := strings.IndexFunc(rest, unicode.IsSpace)
	if idEnd < 0 {
		return "", "", false
	}
	idText = rest[:idEnd]
	message = strings.TrimLeftFunc(rest[idEnd:], unicode.IsSpace)
	return idText, message, message != ""
}

func validateMemberAddress(address string) error {
	host, portText, err := net.SplitHostPort(address)
	if err != nil || host == "" {
		return fmt.Errorf("member address must use host:port format")
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("member address contains an invalid port")
	}
	if host == "0.0.0.0" || host == "::" {
		return fmt.Errorf("member address must be dialable")
	}
	return nil
}

func (s *LeaderServer) markMemberUnavailable(address string, cause error) {
	s.stateMu.Lock()
	member := s.MemberLog[address]
	changed := member != nil && member.Alive
	if changed {
		member.Alive = false
	}
	s.stateMu.Unlock()
	if changed {
		s.dropMemberConn(address)
		logger.Warn(logger.Leader, "Member marked unavailable after RPC failure: addr=%s err=%v", address, cause)
		s.requestStateSave()
	}
}

func (s *LeaderServer) dropMemberConn(address string) {
	s.connsMu.Lock()
	conn := s.conns[address]
	delete(s.conns, address)
	s.connsMu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

func readCommandLine(reader *bufio.Reader) (string, error) {
	var line strings.Builder
	for {
		fragment, more, err := reader.ReadLine()
		if line.Len()+len(fragment) > maxCommandBytes {
			for more && err == nil {
				_, more, err = reader.ReadLine()
			}
			return "", errCommandTooLong
		}
		_, _ = line.Write(fragment)
		if err != nil {
			return "", err
		}
		if !more {
			return line.String(), nil
		}
	}
}

// Heartbeat updates a member's liveness timestamp.
func (s *LeaderServer) Heartbeat(ctx context.Context, hb *pb.HeartbeatRequest) (*emptypb.Empty, error) {
	log := logger.WithContext(ctx, logger.Leader)
	if err := authorizeMember(ctx); err != nil {
		return nil, err
	}
	if hb == nil {
		return nil, status.Error(codes.InvalidArgument, "heartbeat is required")
	}

	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	member, ok := s.MemberLog[hb.Address]
	if !ok {
		logger.Warn(log, "Heartbeat from unknown member: %s", hb.Address)
		return nil, status.Error(codes.NotFound, "member is not registered")
	}

	member.LastSeen = time.Now()
	member.Alive = true
	logger.Debug(log, "Heartbeat received from %s", hb.Address)
	return &emptypb.Empty{}, nil
}

// StartHeartbeatWatcher marks silent members unavailable.
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
					if err := s.saveState(); err != nil {
						logger.Error(logger.Leader, "Failed to persist liveness state: %v", err)
					}
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

type Option func(*LeaderServer)

func WithMemberDialOption(dialOption grpc.DialOption) Option {
	return func(server *LeaderServer) {
		server.memberDialOpt = dialOption
	}
}

func NewLeaderServer(members []string, toleranceFile string, options ...Option) (*LeaderServer, error) {
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
	for _, option := range options {
		option(leader)
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

	if err := leader.loadState(); err != nil {
		return nil, fmt.Errorf("load leader state: %w", err)
	}

	return leader, nil
}

func (s *LeaderServer) RegisterMember(ctx context.Context, req *pb.MemberInfo) (*pb.RegisterReply, error) {
	log := logger.WithContext(ctx, logger.Leader)
	if err := authorizeMember(ctx); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "member info is required")
	}
	if err := validateMemberAddress(req.Address); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	s.stateMu.Lock()
	addr := req.Address

	// Re-register / recovery
	if m, exists := s.MemberLog[addr]; exists {
		recovered := !m.Alive
		if recovered {
			logger.Info(log, "Member recovered: %s", addr)
		} else {
			logger.Debug(log, "Member already registered and alive: %s", addr)
		}
		m.Alive = true
		m.LastSeen = time.Now()
		s.stateMu.Unlock()
		s.dropMemberConn(addr)
		if err := s.saveState(); err != nil {
			return nil, status.Error(codes.Internal, "member state could not be persisted")
		}
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
		return nil, status.Error(codes.Internal, "member state could not be persisted")
	}

	logger.Info(log, "New member registered: %s", addr)
	return &pb.RegisterReply{Ok: true}, nil
}

func (s *LeaderServer) dialMember(addr string) (*grpc.ClientConn, error) {
	if s.dialFn != nil {
		return s.dialFn(addr)
	}
	if s.memberDialOpt == nil {
		return nil, fmt.Errorf("member dial credentials are not configured")
	}

	s.connsMu.Lock()
	if conn, ok := s.conns[addr]; ok {
		if conn.GetState() != connectivity.Shutdown {
			s.connsMu.Unlock()
			return conn, nil
		}
		delete(s.conns, addr)
	}
	s.connsMu.Unlock()

	logger.Debug(logger.Leader, "Dialing member %s with mTLS (New Connection)", addr)
	conn, err := grpc.NewClient(
		addr,
		s.memberDialOpt,
	)

	if err != nil {
		return nil, err
	}

	s.connsMu.Lock()
	defer s.connsMu.Unlock()
	if existing, ok := s.conns[addr]; ok && existing.GetState() != connectivity.Shutdown {
		_ = conn.Close()
		return existing, nil
	}
	s.conns[addr] = conn
	return conn, nil
}

func (s *LeaderServer) Store(ctx context.Context, msg *pb.StoredMessage) (*pb.StoreResult, error) {
	log := logger.WithContext(ctx, logger.Leader)
	if msg == nil {
		return nil, status.Error(codes.InvalidArgument, "message is required")
	}
	if msg.Id < 0 {
		return nil, status.Error(codes.InvalidArgument, "message id must be non-negative")
	}
	if len(msg.Text) > maxMessageBytes {
		return nil, status.Error(codes.ResourceExhausted, "message exceeds 4 MiB limit")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if s.Tolerance <= 0 {
		return nil, status.Error(codes.FailedPrecondition, "leader replication configuration is invalid")
	}

	logger.Debug(log,
		"Store request received: msg_id=%d payload_bytes=%d tolerance=%d",
		msg.Id,
		len(msg.Text),
		s.Tolerance,
	)

	if len(msg.Text) > 1_000_000 {
		logger.Debug(log, "Large payload detected: %d bytes", len(msg.Text))
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

	// Order all viable targets so failed first choices can be replaced.
	picked := lb.Pick(candidates, len(candidates))

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

	successful := make([]string, 0, tol)
	for next := 0; next < len(selected) && len(successful) < tol; {
		needed := tol - len(successful)
		end := next + needed
		if end > len(selected) {
			end = len(selected)
		}
		batch := selected[next:end]
		next = end

		var wg sync.WaitGroup
		results := make(chan string, len(batch))
		for _, addr := range batch {
			wg.Add(1)
			go func(targetAddr string) {
				defer wg.Done()

				success := false
				var failure error
				if s.replicateFn != nil {
					success = s.replicateFn(ctx, targetAddr, msg)
					if !success {
						failure = errors.New("replication failed")
					}
				} else {
					conn, err := s.dialMember(targetAddr)
					if err != nil {
						failure = err
					} else {
						client := pb.NewStorageServiceClient(conn)
						ctx2, cancel := context.WithTimeout(ctx, rpcTimeout)
						res, callErr := client.Store(ctx2, msg)
						cancel()
						if callErr == nil && res != nil && res.Ok {
							success = true
						} else if callErr != nil {
							failure = callErr
						} else {
							failure = errors.New("member rejected replication")
						}
					}
				}
				if !success && ctx.Err() == nil {
					s.markMemberUnavailable(targetAddr, failure)
				}
				if success {
					results <- targetAddr
				}
			}(addr)
		}
		wg.Wait()
		close(results)
		for addr := range results {
			successful = append(successful, addr)
		}
	}

	if len(successful) >= tol {
		s.stateMu.Lock()
		previous := make(map[string]struct{}, len(s.MsgMap[int(msg.Id)]))
		for _, addr := range s.MsgMap[int(msg.Id)] {
			previous[addr] = struct{}{}
		}
		current := make(map[string]struct{}, len(successful))
		for _, addr := range successful {
			current[addr] = struct{}{}
			if _, existed := previous[addr]; !existed && s.MemberLog[addr] != nil {
				s.MemberLog[addr].MessageCnt++
			}
		}
		for addr := range previous {
			if _, retained := current[addr]; !retained && s.MemberLog[addr] != nil && s.MemberLog[addr].MessageCnt > 0 {
				s.MemberLog[addr].MessageCnt--
			}
		}
		s.MsgMap[int(msg.Id)] = append([]string(nil), successful...)
		s.stateMu.Unlock()

		logger.Debug(log, "Store replication successful: msg_id=%d replicas=%v", msg.Id, successful)
		s.requestStateSave()
		return &pb.StoreResult{Ok: true}, nil
	}

	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	return &pb.StoreResult{Ok: false, Err: "Replication incomplete"}, nil
}

func (s *LeaderServer) Retrieve(ctx context.Context, req *pb.MessageID) (*pb.StoredMessage, error) {
	log := logger.WithContext(ctx, logger.Leader)
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "message id is required")
	}
	if req.Id < 0 {
		return nil, status.Error(codes.InvalidArgument, "message id must be non-negative")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	logger.Debug(log, "Retrieve request received: msg_id=%d", req.Id)

	s.stateMu.Lock()
	replicas, ok := s.MsgMap[int(req.Id)]
	members := append([]string(nil), s.Members...)
	s.stateMu.Unlock()

	var targets []string
	if ok && len(replicas) > 0 {
		targets = replicas
	} else {
		logger.Debug(log, "Replica map empty for msg_id=%d, trying all members", req.Id)
		targets = members
	}
	skippedUnavailable := false
	sawUnavailable := false
	sawDataLoss := false

	for _, addr := range targets {
		s.stateMu.Lock()
		info := s.MemberLog[addr]
		alive := info != nil && info.Alive
		s.stateMu.Unlock()

		if !alive {
			skippedUnavailable = true
			continue
		}

		var res *pb.StoredMessage
		var callErr error
		if s.retrieveFn != nil {
			res, callErr = s.retrieveFn(ctx, addr, req)
		} else {
			conn, err := s.dialMember(addr)
			if err != nil {
				logger.Warn(log, "Retrieve connect failed to %s: %v", addr, err)
				sawUnavailable = true
				if ctx.Err() == nil {
					s.markMemberUnavailable(addr, err)
				}
				continue
			}
			client := pb.NewStorageServiceClient(conn)
			ctx2, cancel := context.WithTimeout(ctx, rpcTimeout)
			res, callErr = client.Retrieve(ctx2, req)
			cancel()
		}

		if callErr == nil && res != nil && res.Id == req.Id {
			logger.Debug(log, "Retrieve succeeded from %s: msg_id=%d", addr, req.Id)
			return res, nil
		}
		if callErr == nil {
			callErr = status.Error(codes.DataLoss, "member returned an invalid response")
			sawDataLoss = true
		}
		if callErr != nil && status.Code(callErr) != codes.NotFound && ctx.Err() == nil {
			if status.Code(callErr) == codes.DataLoss {
				sawDataLoss = true
			} else {
				sawUnavailable = true
			}
			s.markMemberUnavailable(addr, callErr)
		}
	}

	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if sawDataLoss {
		return nil, status.Error(codes.DataLoss, "replica returned inconsistent data")
	}
	if sawUnavailable || skippedUnavailable {
		return nil, status.Error(codes.Unavailable, "one or more replicas are unavailable")
	}
	return nil, status.Error(codes.NotFound, "message not found")
}

// ServeClient registers a TCP session before starting its handler.
func (s *LeaderServer) ServeClient(conn net.Conn) {
	s.clientsMu.Lock()
	if s.clientsClose {
		s.clientsMu.Unlock()
		_ = conn.Close()
		return
	}
	if s.clients == nil {
		s.clients = make(map[net.Conn]struct{})
	}
	s.clients[conn] = struct{}{}
	s.clientsWG.Add(1)
	s.clientsMu.Unlock()

	go func() {
		defer func() {
			s.clientsMu.Lock()
			delete(s.clients, conn)
			s.clientsMu.Unlock()
			s.clientsWG.Done()
		}()
		s.HandleClient(conn)
	}()
}

// CloseClients stops existing sessions and waits for their handlers.
func (s *LeaderServer) CloseClients() {
	s.clientsMu.Lock()
	s.clientsClose = true
	connections := make([]net.Conn, 0, len(s.clients))
	for conn := range s.clients {
		connections = append(connections, conn)
	}
	s.clientsMu.Unlock()
	for _, conn := range connections {
		_ = conn.Close()
	}
	s.clientsWG.Wait()
}

func (s *LeaderServer) HandleClient(conn net.Conn) {
	defer conn.Close()

	reqID := fmt.Sprintf("tcp-%d", time.Now().UnixNano())
	ctx := context.WithValue(context.Background(), logger.RequestIDKey, reqID)
	log := logger.WithContext(ctx, logger.Leader)
	logger.Info(log, "TCP client connected: %s", conn.RemoteAddr())

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	fmt.Fprint(writer, "Tolerex ready\r\n")
	_ = writer.Flush()

	for {
		line, err := readCommandLine(reader)
		if err != nil {
			if errors.Is(err, errCommandTooLong) {
				fmt.Fprint(writer, "ERROR: command exceeds 4 MiB limit\r\n")
				_ = writer.Flush()
				continue
			}
			if err != io.EOF {
				logger.Debug(log, "TCP client closed: %v", err)
			}
			return
		}

		line = strings.TrimSpace(line)
		if line == "" {
			fmt.Fprint(writer, "ERROR: empty command\r\n")
			_ = writer.Flush()
			continue
		}

		parts := strings.Fields(line)
		cmd := strings.ToUpper(parts[0])
		logger.Debug(log, "TCP command received: command=%s bytes=%d", cmd, len(line))

		switch cmd {
		case "SET":
			idText, msg, ok := parseSetCommand(line)
			if !ok {
				fmt.Fprint(writer, "ERROR: SET <id> <message>\r\n")
				logger.Warn(log, "Invalid SET command format")
				break
			}

			id, err := strconv.ParseInt(idText, 10, 32)
			if err != nil || id < 0 {
				fmt.Fprint(writer, "ERROR: id must be a non-negative 32-bit integer\r\n")
				logger.Warn(log, "Invalid id in SET command: %s", idText)
				break
			}

			res, _ := s.Store(ctx, &pb.StoredMessage{Id: int32(id), Text: msg})
			if res != nil && res.Ok {
				fmt.Fprint(writer, "OK\r\n")
				logger.Debug(log, "SET command succeeded: id=%d", id)
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

			id, err := strconv.ParseInt(parts[1], 10, 32)
			if err != nil || id < 0 {
				fmt.Fprint(writer, "ERROR: id must be a non-negative 32-bit integer\r\n")
				logger.Warn(log, "Invalid id in GET command: %s", parts[1])
				break
			}

			res, err := s.Retrieve(ctx, &pb.MessageID{Id: int32(id)})
			if err != nil {
				st, ok := status.FromError(err)
				if ok && st.Code() == codes.NotFound {
					fmt.Fprint(writer, "NOT_FOUND\r\n")
					logger.Debug(log, "GET command: message not found id=%d", id)
				} else {
					fmt.Fprint(writer, "ERROR: retrieve failed\r\n")
					logger.Error(log, "GET command failed: id=%d err=%v", id, err)
				}
				break
			}

			fmt.Fprint(writer, "VALUE "+res.Text+"\r\n")

		case "HELP":
			fmt.Fprint(writer, "HELP SET <id> <message> | GET <id> | STATUS | HELP | QUIT\r\n")

		case "STATUS":
			if len(parts) != 1 {
				fmt.Fprint(writer, "ERROR: STATUS takes no arguments\r\n")
				break
			}
			s.stateMu.Lock()
			alive := 0
			for _, member := range s.MemberLog {
				if member.Alive {
					alive++
				}
			}
			memberCount := len(s.MemberLog)
			messageCount := len(s.MsgMap)
			tolerance := s.Tolerance
			s.stateMu.Unlock()
			fmt.Fprintf(writer, "STATUS members=%d alive=%d tolerance=%d messages=%d\r\n", memberCount, alive, tolerance, messageCount)

		case "QUIT", "EXIT":
			fmt.Fprint(writer, "Bye\r\n")
			_ = writer.Flush()
			logger.Info(log, "TCP client disconnected: %s", conn.RemoteAddr())
			return

		default:
			fmt.Fprint(writer, "ERROR: unknown command\r\n")
		}

		_ = writer.Flush()
	}
}

// StartStatePersister batches frequent Store metadata updates. Registration and
// liveness changes remain synchronous because they are infrequent.
func (s *LeaderServer) StartStatePersister(interval time.Duration) {
	if interval <= 0 {
		interval = defaultPersistInterval
	}

	s.persistLoopMu.Lock()
	if s.persistSignal != nil {
		s.persistLoopMu.Unlock()
		return
	}
	signal := make(chan struct{}, 1)
	stop := make(chan struct{})
	done := make(chan error, 1)
	s.persistSignal = signal
	s.persistStop = stop
	s.persistDone = done
	s.persistLoopMu.Unlock()

	go func() {
		for {
			select {
			case <-signal:
				timer := time.NewTimer(interval)
				select {
				case <-timer.C:
					if err := s.saveState(); err != nil {
						logger.Error(logger.Leader, "Failed to persist batched leader state: %v", err)
					}
				case <-stop:
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					done <- s.saveState()
					return
				}
			case <-stop:
				done <- s.saveState()
				return
			}
		}
	}()
}

func (s *LeaderServer) requestStateSave() {
	s.persistLoopMu.Lock()
	signal := s.persistSignal
	s.persistLoopMu.Unlock()

	if signal == nil {
		if err := s.saveState(); err != nil {
			logger.Error(logger.Leader, "Failed to persist leader state: %v", err)
		}
		return
	}

	select {
	case signal <- struct{}{}:
	default:
	}
}

// StopStatePersister flushes the latest snapshot before returning.
func (s *LeaderServer) StopStatePersister() error {
	s.persistLoopMu.Lock()
	if s.persistStop == nil {
		s.persistLoopMu.Unlock()
		return nil
	}
	stop := s.persistStop
	done := s.persistDone
	s.persistSignal = nil
	s.persistStop = nil
	s.persistDone = nil
	close(stop)
	s.persistLoopMu.Unlock()

	return <-done
}

func (s *LeaderServer) saveState() error {
	s.persistMu.Lock()
	defer s.persistMu.Unlock()

	s.stateMu.Lock()
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
	s.stateMu.Unlock()

	logger.Debug(logger.Leader, "Persisting Leader state: members=%d msgMap entries=%d", len(members), len(msgMap))

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
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	temp, err := os.CreateTemp(dir, ".leader-state-*.tmp")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	cleanup := func() {
		_ = temp.Close()
		_ = os.Remove(tempPath)
	}
	if err := temp.Chmod(0o600); err != nil {
		cleanup()
		return err
	}
	if _, err := temp.Write(data); err != nil {
		cleanup()
		return err
	}
	if err := temp.Sync(); err != nil {
		cleanup()
		return err
	}
	if err := temp.Close(); err != nil {
		cleanup()
		return err
	}
	if err := os.Rename(tempPath, path); err != nil {
		cleanup()
		return err
	}

	logger.Debug(logger.Leader, "Leader state persisted successfully")
	return nil
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
	connections := make([]*grpc.ClientConn, 0, len(s.conns))
	for addr, conn := range s.conns {
		connections = append(connections, conn)
		delete(s.conns, addr)
	}
	s.connsMu.Unlock()
	for _, conn := range connections {
		_ = conn.Close()
	}
}
