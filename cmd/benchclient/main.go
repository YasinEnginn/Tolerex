package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	serverReadyLine = "Tolerex ready"
	requestTimeout  = 20 * time.Second
	payloadVariants = 64
	maxInt32        = int64(1<<31 - 1)
)

var (
	addrFlag         = flag.String("addr", "127.0.0.1:6666", "leader TCP address")
	clientsFlag      = flag.Int("clients", 1, "number of concurrent closed-loop clients")
	readRatioFlag    = flag.Int("read-ratio", 80, "percentage of GET operations")
	payloadBytesFlag = flag.Int("payload-bytes", 1024, "exact message payload size")
	workingSetFlag   = flag.Int("working-set", 1024, "keys owned by each client")
	warmupFlag       = flag.Duration("warmup", 2*time.Minute, "unmeasured warm-up duration")
	durationFlag     = flag.Duration("duration", 10*time.Minute, "measurement duration")
	distributionFlag = flag.String("distribution", "zipf", "key distribution: zipf or uniform")
	zipfSFlag        = flag.Float64("zipf-s", 1.1, "Zipf exponent; must be greater than 1")
	seedFlag         = flag.Int64("seed", 20261025, "base pseudo-random seed")
	baseIDFlag       = flag.Int("base-id", 1, "first generated message ID")
	profileFlag      = flag.String("profile", "read-heavy-80-20", "profile label written to JSON")
)

type session struct {
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
}

type worker struct {
	index        int
	baseID       int
	workingSet   int
	readRatio    int
	distribution string
	rng          *rand.Rand
	zipf         *rand.Zipf
	payloads     []string
	current      []uint8
	sequence     uint64
	session      *session
}

type workerStats struct {
	reads        int64
	writes       int64
	successes    int64
	failures     int64
	allLatency   []int64
	readLatency  []int64
	writeLatency []int64
	fatal        error
}

type latencySummary struct {
	Count  int     `json:"count"`
	MinMS  float64 `json:"min_ms"`
	MeanMS float64 `json:"mean_ms"`
	P50MS  float64 `json:"p50_ms"`
	P95MS  float64 `json:"p95_ms"`
	P99MS  float64 `json:"p99_ms"`
	MaxMS  float64 `json:"max_ms"`
}

type benchmarkResult struct {
	SchemaVersion       int            `json:"schema_version"`
	TimestampUTC        string         `json:"timestamp_utc"`
	Profile             string         `json:"profile"`
	Address             string         `json:"address"`
	Clients             int            `json:"clients"`
	ReadRatioTarget     int            `json:"read_ratio_target_percent"`
	ReadRatioActual     float64        `json:"read_ratio_actual_percent"`
	PayloadBytes        int            `json:"payload_bytes"`
	WorkingSetPerClient int            `json:"working_set_per_client"`
	Distribution        string         `json:"distribution"`
	ZipfS               float64        `json:"zipf_s,omitempty"`
	Seed                int64          `json:"seed"`
	WarmupSeconds       float64        `json:"warmup_seconds"`
	RequestedSeconds    float64        `json:"requested_measurement_seconds"`
	MeasuredSeconds     float64        `json:"measured_seconds"`
	Reads               int64          `json:"reads"`
	Writes              int64          `json:"writes"`
	SuccessfulOps       int64          `json:"successful_operations"`
	FailedOps           int64          `json:"failed_operations"`
	ThroughputOPS       float64        `json:"throughput_ops_per_second"`
	ErrorRatePercent    float64        `json:"error_rate_percent"`
	Latency             latencySummary `json:"latency"`
	ReadLatency         latencySummary `json:"read_latency"`
	WriteLatency        latencySummary `json:"write_latency"`
	FatalErrors         []string       `json:"fatal_errors,omitempty"`
}

func openSession(address string) (*session, error) {
	conn, err := (&net.Dialer{Timeout: 5 * time.Second}).Dial("tcp", address)
	if err != nil {
		return nil, err
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
	}
	s := &session{
		conn:   conn,
		reader: bufio.NewReaderSize(conn, 64*1024),
		writer: bufio.NewWriterSize(conn, 64*1024),
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	greeting, readErr := s.reader.ReadString('\n')
	_ = conn.SetReadDeadline(time.Time{})
	if readErr != nil || strings.TrimSpace(greeting) != serverReadyLine {
		_ = conn.Close()
		if readErr != nil {
			return nil, readErr
		}
		return nil, fmt.Errorf("unexpected server greeting %q", strings.TrimSpace(greeting))
	}
	return s, nil
}

func (s *session) execute(command string) (string, time.Duration, error) {
	start := time.Now()
	_ = s.conn.SetDeadline(start.Add(requestTimeout))
	if _, err := s.writer.WriteString(command + "\r\n"); err != nil {
		return "", time.Since(start), err
	}
	if err := s.writer.Flush(); err != nil {
		return "", time.Since(start), err
	}
	response, err := s.reader.ReadString('\n')
	_ = s.conn.SetDeadline(time.Time{})
	return strings.TrimSpace(response), time.Since(start), err
}

func buildPayload(size, workerIndex, variant int) string {
	prefix := fmt.Sprintf("w%02d-v%02d-", workerIndex, variant)
	if len(prefix) >= size {
		return prefix[:size]
	}
	return prefix + strings.Repeat("x", size-len(prefix))
}

func newWorker(index int) (*worker, error) {
	rng := rand.New(rand.NewSource(*seedFlag + int64(index)*104729))
	w := &worker{
		index:        index,
		baseID:       *baseIDFlag + index**workingSetFlag,
		workingSet:   *workingSetFlag,
		readRatio:    *readRatioFlag,
		distribution: *distributionFlag,
		rng:          rng,
		payloads:     make([]string, payloadVariants),
		current:      make([]uint8, *workingSetFlag),
	}
	if w.distribution == "zipf" {
		w.zipf = rand.NewZipf(rng, *zipfSFlag, 1, uint64(w.workingSet-1))
	}
	for variant := range w.payloads {
		w.payloads[variant] = buildPayload(*payloadBytesFlag, index, variant)
	}
	s, err := openSession(*addrFlag)
	if err != nil {
		return nil, err
	}
	w.session = s
	return w, nil
}

func (w *worker) pickKey() int {
	if w.zipf != nil {
		return int(w.zipf.Uint64())
	}
	return w.rng.Intn(w.workingSet)
}

func (w *worker) store(key, variant int) (time.Duration, bool, error) {
	id := w.baseID + key
	response, latency, err := w.session.execute(fmt.Sprintf("SET %d %s", id, w.payloads[variant]))
	if err != nil {
		return latency, false, err
	}
	if response != "OK" {
		return latency, false, nil
	}
	w.current[key] = uint8(variant)
	return latency, true, nil
}

func (w *worker) retrieve(key int) (time.Duration, bool, error) {
	id := w.baseID + key
	response, latency, err := w.session.execute(fmt.Sprintf("GET %d", id))
	if err != nil {
		return latency, false, err
	}
	want := "VALUE " + w.payloads[w.current[key]]
	return latency, response == want, nil
}

func (w *worker) prefill() error {
	for key := 0; key < w.workingSet; key++ {
		_, ok, err := w.store(key, 0)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("prefill SET failed for id %d", w.baseID+key)
		}
	}
	return nil
}

func (w *worker) runOne() (operation string, latency time.Duration, ok bool, err error) {
	key := w.pickKey()
	if w.rng.Intn(100) < w.readRatio {
		latency, ok, err = w.retrieve(key)
		return "GET", latency, ok, err
	}
	w.sequence++
	variant := int(w.sequence % payloadVariants)
	latency, ok, err = w.store(key, variant)
	return "SET", latency, ok, err
}

func runPrefill(workers []*worker) error {
	var wg sync.WaitGroup
	errorsByWorker := make([]error, len(workers))
	for index, current := range workers {
		wg.Add(1)
		go func(i int, w *worker) {
			defer wg.Done()
			errorsByWorker[i] = w.prefill()
		}(index, current)
	}
	wg.Wait()
	for index, err := range errorsByWorker {
		if err != nil {
			return fmt.Errorf("worker %d prefill: %w", index, err)
		}
	}
	return nil
}

func runWarmup(workers []*worker, duration time.Duration) error {
	if duration <= 0 {
		return nil
	}
	deadline := time.Now().Add(duration)
	var wg sync.WaitGroup
	errorsByWorker := make([]error, len(workers))
	for index, current := range workers {
		wg.Add(1)
		go func(i int, w *worker) {
			defer wg.Done()
			for time.Now().Before(deadline) {
				_, _, ok, err := w.runOne()
				if err != nil {
					errorsByWorker[i] = err
					return
				}
				if !ok {
					errorsByWorker[i] = errors.New("warm-up operation returned an invalid response")
					return
				}
			}
		}(index, current)
	}
	wg.Wait()
	for index, err := range errorsByWorker {
		if err != nil {
			return fmt.Errorf("worker %d warm-up: %w", index, err)
		}
	}
	return nil
}

func runMeasurement(workers []*worker, duration time.Duration) ([]workerStats, time.Duration) {
	stats := make([]workerStats, len(workers))
	start := time.Now()
	deadline := start.Add(duration)
	var wg sync.WaitGroup
	for index, current := range workers {
		wg.Add(1)
		go func(i int, w *worker) {
			defer wg.Done()
			currentStats := &stats[i]
			for time.Now().Before(deadline) {
				operation, latency, ok, err := w.runOne()
				if err != nil {
					currentStats.fatal = err
					return
				}
				micros := latency.Microseconds()
				if operation == "GET" {
					currentStats.reads++
				} else {
					currentStats.writes++
				}
				if !ok {
					currentStats.failures++
					continue
				}
				currentStats.successes++
				currentStats.allLatency = append(currentStats.allLatency, micros)
				if operation == "GET" {
					currentStats.readLatency = append(currentStats.readLatency, micros)
				} else {
					currentStats.writeLatency = append(currentStats.writeLatency, micros)
				}
			}
		}(index, current)
	}
	wg.Wait()
	return stats, time.Since(start)
}

func summarize(values []int64) latencySummary {
	if len(values) == 0 {
		return latencySummary{}
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	var sum float64
	for _, value := range values {
		sum += float64(value)
	}
	valueAt := func(percentile float64) float64 {
		index := int(math.Ceil(percentile*float64(len(values)))) - 1
		if index < 0 {
			index = 0
		}
		if index >= len(values) {
			index = len(values) - 1
		}
		return float64(values[index]) / 1000
	}
	return latencySummary{
		Count:  len(values),
		MinMS:  float64(values[0]) / 1000,
		MeanMS: sum / float64(len(values)) / 1000,
		P50MS:  valueAt(0.50),
		P95MS:  valueAt(0.95),
		P99MS:  valueAt(0.99),
		MaxMS:  float64(values[len(values)-1]) / 1000,
	}
}

func validateFlags() error {
	if strings.TrimSpace(*addrFlag) == "" {
		return errors.New("addr must not be empty")
	}
	if *clientsFlag < 1 || *clientsFlag > 128 {
		return errors.New("clients must be between 1 and 128")
	}
	if *readRatioFlag < 0 || *readRatioFlag > 100 {
		return errors.New("read-ratio must be between 0 and 100")
	}
	if *payloadBytesFlag < 32 || *payloadBytesFlag > 4<<20 {
		return errors.New("payload-bytes must be between 32 and 4194304")
	}
	if *workingSetFlag < 1 {
		return errors.New("working-set must be greater than 0")
	}
	if *warmupFlag < 0 || *durationFlag <= 0 {
		return errors.New("warmup must be non-negative and duration must be positive")
	}
	if *distributionFlag != "zipf" && *distributionFlag != "uniform" {
		return errors.New("distribution must be zipf or uniform")
	}
	if *distributionFlag == "zipf" && *zipfSFlag <= 1 {
		return errors.New("zipf-s must be greater than 1")
	}
	lastID := int64(*baseIDFlag) + int64(*clientsFlag**workingSetFlag) - 1
	if *baseIDFlag < 0 || lastID > maxInt32 {
		return errors.New("generated IDs exceed the non-negative 32-bit range")
	}
	return nil
}

func main() {
	flag.Parse()
	if err := validateFlags(); err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(2)
	}

	workers := make([]*worker, 0, *clientsFlag)
	for index := 0; index < *clientsFlag; index++ {
		current, err := newWorker(index)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: connect worker %d: %v\n", index, err)
			os.Exit(1)
		}
		workers = append(workers, current)
	}
	defer func() {
		for _, current := range workers {
			_ = current.session.conn.Close()
		}
	}()

	fmt.Fprintf(os.Stderr, "Prefilling %d keys (%d clients x %d keys)...\n", *clientsFlag**workingSetFlag, *clientsFlag, *workingSetFlag)
	if err := runPrefill(workers); err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "Warm-up: %s\n", warmupFlag.String())
	if err := runWarmup(workers, *warmupFlag); err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "Measurement: %s\n", durationFlag.String())
	workerResults, elapsed := runMeasurement(workers, *durationFlag)

	var reads, writes, successes, failures int64
	var allLatency, readLatency, writeLatency []int64
	var fatalErrors []string
	for index, current := range workerResults {
		reads += current.reads
		writes += current.writes
		successes += current.successes
		failures += current.failures
		allLatency = append(allLatency, current.allLatency...)
		readLatency = append(readLatency, current.readLatency...)
		writeLatency = append(writeLatency, current.writeLatency...)
		if current.fatal != nil {
			fatalErrors = append(fatalErrors, fmt.Sprintf("worker %d: %v", index, current.fatal))
		}
	}
	attempted := reads + writes
	actualReadRatio := 0.0
	errorRate := 0.0
	if attempted > 0 {
		actualReadRatio = float64(reads) * 100 / float64(attempted)
		errorRate = float64(failures) * 100 / float64(attempted)
	}
	result := benchmarkResult{
		SchemaVersion:       1,
		TimestampUTC:        time.Now().UTC().Format(time.RFC3339),
		Profile:             *profileFlag,
		Address:             *addrFlag,
		Clients:             *clientsFlag,
		ReadRatioTarget:     *readRatioFlag,
		ReadRatioActual:     actualReadRatio,
		PayloadBytes:        *payloadBytesFlag,
		WorkingSetPerClient: *workingSetFlag,
		Distribution:        *distributionFlag,
		ZipfS:               *zipfSFlag,
		Seed:                *seedFlag,
		WarmupSeconds:       warmupFlag.Seconds(),
		RequestedSeconds:    durationFlag.Seconds(),
		MeasuredSeconds:     elapsed.Seconds(),
		Reads:               reads,
		Writes:              writes,
		SuccessfulOps:       successes,
		FailedOps:           failures,
		ThroughputOPS:       float64(successes) / elapsed.Seconds(),
		ErrorRatePercent:    errorRate,
		Latency:             summarize(allLatency),
		ReadLatency:         summarize(readLatency),
		WriteLatency:        summarize(writeLatency),
		FatalErrors:         fatalErrors,
	}

	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		fmt.Fprintln(os.Stderr, "ERROR: encode result:", err)
		os.Exit(1)
	}
	if failures > 0 || len(fatalErrors) > 0 {
		os.Exit(1)
	}
}
