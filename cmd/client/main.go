package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	DEFAULT_ADDR     = "127.0.0.1:6666"
	DIAL_TIMEOUT     = 3 * time.Second
	RETRY_DELAY      = 2 * time.Second
	BULK_FLUSH_EVERY = 1000

	// Leader prompt (server prints this WITHOUT newline)
	SERVER_PROMPT = "tolerex> "
)

var bannerLines = map[string]struct{}{
	"--------------------------------------------------": {},
	"TOLEREX - Leader Node (Control Plane)":              {},
	"Basic Commands:":                                    {},
	"Utility:":                                           {},
	"Examples:":                                          {},
	"SET <id> <message>        Store a message":          {},
	"<N> SET <message>         Bulk store N messages":    {},
	"GET <id>                  Retrieve a message":       {},
	"HELP                      Show this help":           {},
	"QUIT | EXIT               Disconnect client":        {},
	"SET 10 HELLO":                                       {},
	"1000 SET HELLO":                                     {},
}

var (
	addrStr    = flag.String("addr", DEFAULT_ADDR, "Leader TCP address or comma-separated cluster addresses (e.g., 127.0.0.1:6666,127.0.0.1:6667)")
	measure    = flag.Bool("measure", false, "Measure RTT per command using a separate TCP connection")
	csv        = flag.Bool("csv", false, "Output measurements as CSV to stdout (Timestamp,ClientID,Operation,Count,Bytes,RTT_us,Mode,Status,PayloadSize). All logs/UI go to stderr.")
	flushEvery = flag.Int("flush", BULK_FLUSH_EVERY, "Flush buffer after every N operations (default 1000). Set to 1 for unbuffered.")
	offset     = flag.Int("offset", 0, "Starting ID offset for BULK operations (default 0)")
	clientID   = flag.String("id", "client", "Client Identifier for CSV output (default 'client')")
	stressMins = flag.Int("stress", 0, "Run stress test for N minutes. If 0, runs interactive/stdin mode.")
)

func rotateAddresses(addrs []string) []string {
	if len(addrs) <= 1 {
		return addrs
	}
	res := make([]string, len(addrs))
	copy(res, addrs)
	rand.Shuffle(len(res), func(i, j int) {
		res[i], res[j] = res[j], res[i]
	})
	return res
}

func connectLoop(addresses []string) net.Conn {
	for {
		currentAddrs := rotateAddresses(addresses)
		for _, a := range currentAddrs {
			dialer := net.Dialer{Timeout: DIAL_TIMEOUT}
			conn, err := dialer.Dial("tcp", a)
			if err == nil {
				if tcp, ok := conn.(*net.TCPConn); ok {
					_ = tcp.SetNoDelay(true)
				}
				fmt.Fprintf(os.Stderr, "Connected to %s\n", a)
				return conn
			}
		}
		fmt.Fprintf(os.Stderr, "Cluster unreachable, retrying in %v...\n", RETRY_DELAY)
		time.Sleep(RETRY_DELAY)
	}
}

func drainMeasureStartup(mconn net.Conn, r *bufio.Reader, maxDur time.Duration) error {
	deadline := time.Now().Add(maxDur)
	_ = mconn.SetReadDeadline(deadline)
	defer mconn.SetReadDeadline(time.Time{})

	p := []byte(SERVER_PROMPT)
	window := make([]byte, 0, len(p))

	for {
		b, err := r.ReadByte()
		if err != nil {
			return fmt.Errorf("startup sync failed (waiting for prompt): %w", err)
		}

		if len(window) < len(p) {
			window = append(window, b)
		} else {
			copy(window, window[1:])
			window[len(p)-1] = b
		}

		if len(window) == len(p) && bytes.Equal(window, p) {
			return nil
		}
	}
}

func setupMeasureChannel(addrs []string) (net.Conn, *bufio.Reader, *bufio.Writer, error) {
	const maxAttempts = 2

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		mconn := connectLoop(addrs)
		mreader := bufio.NewReader(mconn)
		mwriter := bufio.NewWriterSize(mconn, 64*1024)

		timeout := 5 * time.Second
		if attempt == maxAttempts {
			timeout = 15 * time.Second
		}

		if err := drainMeasureStartup(mconn, mreader, timeout); err == nil {
			return mconn, mreader, mwriter, nil
		} else {
			fmt.Fprintf(
				os.Stderr,
				"Measure channel sync failed (attempt %d/%d): %v\n",
				attempt,
				maxAttempts,
				err,
			)
		}

		_ = mconn.Close()
		time.Sleep(500 * time.Millisecond)
	}

	return nil, nil, nil, fmt.Errorf("startup sync failed after retries")
}

func consumePrompt(mconn net.Conn, r *bufio.Reader) {
	// Optimization: Reduce deadline calls if possible, use faster peek
	_ = mconn.SetReadDeadline(time.Now().Add(5 * time.Millisecond))
	defer mconn.SetReadDeadline(time.Time{})

	for i := 0; i < 5; i++ {
		b, err := r.Peek(len(SERVER_PROMPT))
		if err == nil && string(b) == SERVER_PROMPT {
			_, _ = r.Discard(len(SERVER_PROMPT))
			return
		}
		one, err2 := r.Peek(1)
		if err2 == nil && (one[0] == '\r' || one[0] == '\n' || one[0] == ' ') {
			_, _ = r.Discard(1)
			continue
		}
		return
	}
}

func isControlBannerLine(line string) bool {
	if strings.Contains(line, "TOLEREX - Leader Node") {
		return true
	}
	_, ok := bannerLines[line]
	return ok
}

func readSemanticLine(mconn net.Conn, r *bufio.Reader, timeout time.Duration) (string, error) {
	_ = mconn.SetReadDeadline(time.Now().Add(timeout))
	defer mconn.SetReadDeadline(time.Time{})

	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return "", err
		}

		// Optimization: Avoid heavy TrimSpace if not needed
		t := strings.TrimSpace(line)
		if len(t) == 0 {
			continue
		}

		// Fast path for common responses
		if t == "OK" || t == "NOT_FOUND" || t == "Bye" {
			return t, nil
		}

		// Skip only exact banner/help lines, never data payloads.
		if isControlBannerLine(t) {
			continue
		}
		return t, nil
	}
}

func listTxtFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".txt") {
			files = append(files, e.Name())
		}
	}
	return files, nil
}

func selectRandomTestFile() (string, error) {
	const baseDir = "test_data"
	files, err := listTxtFiles(baseDir)
	if err != nil || len(files) == 0 {
		return "", fmt.Errorf("no txt files found in %s", baseDir)
	}
	return filepath.Join(baseDir, files[rand.Intn(len(files))]), nil
}

func selectTestFileInteractive() (string, error) {
	const baseDir = "test_data"
	files, err := listTxtFiles(baseDir)
	if err != nil || len(files) == 0 {
		return "", fmt.Errorf("no txt files found in %s", baseDir)
	}
	fmt.Fprintf(os.Stderr, "\nAvailable files:\n")
	for i, f := range files {
		fmt.Fprintf(os.Stderr, "  %d) %s\n", i+1, f)
	}
	var fileChoice int
	fmt.Fprint(os.Stderr, "Select file: ")
	fmt.Scanln(&fileChoice)
	if fileChoice < 1 || fileChoice > len(files) {
		return "", fmt.Errorf("invalid file choice")
	}
	return filepath.Join(baseDir, files[fileChoice-1]), nil
}

func readWholeFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	content := string(data)
	content = strings.ReplaceAll(content, "\r\n", " ")
	content = strings.ReplaceAll(content, "\n", " ")
	content = strings.Join(strings.Fields(content), " ")
	return content, nil
}

type rttStats struct {
	count int64
	sum   time.Duration
	min   time.Duration
	max   time.Duration
}

func (s *rttStats) add(rtt time.Duration) {
	s.count++
	s.sum += rtt
	if rtt < s.min || s.min == 0 {
		s.min = rtt
	}
	if rtt > s.max {
		s.max = rtt
	}
}

func emitStat(csvEnabled bool, csvOut *bufio.Writer, op string, count int, bytesSent int64, rtt time.Duration, status string, payloadSize int) {
	mode := "buffered"
	if *flushEvery == 1 {
		mode = "unbuffered"
	}
	if csvEnabled && csvOut != nil {
		ts := time.Now().Format(time.RFC3339Nano)
		fmt.Fprintf(csvOut, "%s,%s,%s,%d,%d,%d,%s,%s,%d\n", ts, *clientID, op, count, bytesSent, rtt.Microseconds(), mode, status, payloadSize)
		_ = csvOut.Flush()
		return
	}
	fmt.Fprintf(os.Stderr, "RTT (%s): %d us | count=%d | bytes=%d | mode=%s | status=%s\n", op, rtt.Microseconds(), count, bytesSent, mode, status)
}

func main() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())

	addrs := strings.Split(*addrStr, ",")
	for i := range addrs {
		addrs[i] = strings.TrimSpace(addrs[i])
	}

	if *csv && !*measure {
		fmt.Fprintln(os.Stderr, "ERROR: -csv requires -measure. Run with: -measure -csv")
		os.Exit(2)
	}

	var csvOut *bufio.Writer
	if *csv {
		csvOut = bufio.NewWriterSize(os.Stdout, 64*1024)
		fmt.Fprintln(csvOut, "Timestamp,ClientID,Operation,Count,Bytes,RTT_us,Mode,Status,PayloadSize")
		_ = csvOut.Flush()
	}

	cliConn := connectLoop(addrs)
	defer cliConn.Close()

	var cliOut io.Writer = os.Stdout
	if *stressMins > 0 {
		// In stress mode, discarding control-plane responses avoids
		// background-job output buffering stalls.
		cliOut = io.Discard
	} else if *csv {
		cliOut = os.Stderr
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = io.Copy(cliOut, cliConn)
	}()

	cliWriter := bufio.NewWriterSize(cliConn, 64*1024)

	var (
		mconn   net.Conn
		mreader *bufio.Reader
		mwriter *bufio.Writer
	)
	stats := rttStats{}

	if *measure {
		var err error
		mconn, mreader, mwriter, err = setupMeasureChannel(addrs)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		defer mconn.Close()
	}

	if *stressMins > 0 {
		runStressTest(addrs, cliWriter, mconn, mreader, mwriter, csvOut, &stats)
		return
	}

	stdin := bufio.NewScanner(os.Stdin)
	stdin.Buffer(make([]byte, 1024), 10*1024*1024)

	for {
		fmt.Fprint(os.Stderr, SERVER_PROMPT)
		if !stdin.Scan() {
			break
		}
		line := strings.TrimSpace(stdin.Text())
		if line == "" {
			continue
		}

		if strings.EqualFold(line, "EXIT") || strings.EqualFold(line, "QUIT") {
			_, _ = cliWriter.WriteString("QUIT\r\n")
			_ = cliWriter.Flush()
			if *measure && mwriter != nil {
				_, _ = mwriter.WriteString("QUIT\r\n")
				_ = mwriter.Flush()
			}
			break
		}

		fields := strings.Fields(line)
		if len(fields) == 2 && strings.EqualFold(fields[1], "FILE") {
			if bulkCount, err := strconv.Atoi(fields[0]); err == nil {
				path, _ := selectTestFileInteractive()
				if path != "" {
					handleBulkFile(bulkCount, path, cliWriter, mconn, mreader, mwriter, csvOut, &stats)
				}
				continue
			}
		}

		if strings.EqualFold(line, "FILE") {
			path, _ := selectTestFileInteractive()
			if path != "" {
				handleSingleFile(path, cliWriter, mconn, mreader, mwriter, csvOut, &stats)
			}
			continue
		}

		if len(fields) >= 3 && strings.EqualFold(fields[1], "SET") {
			if bulkCount, err := strconv.Atoi(fields[0]); err == nil {
				handleBulkSet(bulkCount, strings.Join(fields[2:], " "), cliWriter, mconn, mreader, mwriter, csvOut, &stats)
				continue
			}
		}

		// Normal operation
		_, _ = cliWriter.WriteString(line + "\r\n")
		_ = cliWriter.Flush()

		if *measure && mwriter != nil {
			start := time.Now()
			_, _ = mwriter.WriteString(line + "\r\n")
			_ = mwriter.Flush()
			_, err := readSemanticLine(mconn, mreader, 5*time.Second)
			rtt := time.Since(start)
			consumePrompt(mconn, mreader)
			status := "OK"
			if err != nil {
				status = "ERROR"
			}
			stats.add(rtt)
			emitStat(*csv, csvOut, strings.ToUpper(fields[0]), 1, int64(len(line)+2), rtt, status, 0)
		}
	}

	if *measure && stats.count > 0 {
		fmt.Fprintf(os.Stderr, "\n=== RTT SUMMARY ===\nSamples: %d | Avg: %v | Min: %v | Max: %v\n",
			stats.count, stats.sum/time.Duration(stats.count), stats.min, stats.max)
	}
	<-done
}

func handleSingleFile(path string, cliWriter *bufio.Writer, mconn net.Conn, mreader *bufio.Reader, mwriter *bufio.Writer, csvOut *bufio.Writer, stats *rttStats) {
	content, _ := readWholeFile(path)
	id := rand.Intn(1000000)
	cmd := fmt.Sprintf("SET %d %s\r\n", id, content)
	_, _ = cliWriter.WriteString(cmd)
	_ = cliWriter.Flush()

	if *measure && mwriter != nil {
		start := time.Now()
		_, _ = mwriter.WriteString(cmd)
		_ = mwriter.Flush()
		_, err := readSemanticLine(mconn, mreader, 10*time.Second)
		rtt := time.Since(start)
		consumePrompt(mconn, mreader)
		status := "OK"
		if err != nil {
			status = "ERROR"
		}
		stats.add(rtt)
		emitStat(*csv, csvOut, "FILE_SET", 1, int64(len(cmd)), rtt, status, len(content))
	}
}

func handleBulkFile(count int, path string, cliWriter *bufio.Writer, mconn net.Conn, mreader *bufio.Reader, mwriter *bufio.Writer, csvOut *bufio.Writer, stats *rttStats) {
	content, _ := readWholeFile(path)
	baseID := rand.Intn(1000000)
	var totalBytes int64
	var totalRTT time.Duration
	var samples int

	for i := 0; i < count; i++ {
		cmd := fmt.Sprintf("SET %d %s\r\n", baseID+i, content)
		totalBytes += int64(len(cmd))
		_, _ = cliWriter.WriteString(cmd)
		if (i+1)%*flushEvery == 0 {
			_ = cliWriter.Flush()
		}

		if *measure && mwriter != nil && (i == 0 || i == count-1) {
			start := time.Now()
			_, _ = mwriter.WriteString(cmd)
			_ = mwriter.Flush()
			_, err := readSemanticLine(mconn, mreader, 10*time.Second)
			if err == nil {
				totalRTT += time.Since(start)
				samples++
			}
			consumePrompt(mconn, mreader)
		}
	}
	_ = cliWriter.Flush()

	if samples > 0 {
		avgRTT := totalRTT / time.Duration(samples)
		stats.add(avgRTT)
		emitStat(*csv, csvOut, "BULK_FILE", count, totalBytes, avgRTT, "OK", len(content))
	}
}

func handleBulkSet(count int, msg string, cliWriter *bufio.Writer, mconn net.Conn, mreader *bufio.Reader, mwriter *bufio.Writer, csvOut *bufio.Writer, stats *rttStats) {
	var totalBytes int64
	var totalRTT time.Duration
	var samples int
	baseID := *offset + rand.Intn(1000000)

	for i := 0; i < count; i++ {
		cmd := fmt.Sprintf("SET %d %s_%d\r\n", baseID+i, msg, baseID+i)
		totalBytes += int64(len(cmd))
		_, _ = cliWriter.WriteString(cmd)
		if (i+1)%*flushEvery == 0 {
			_ = cliWriter.Flush()
		}

		if *measure && mwriter != nil && (i == 0 || i == count-1) {
			start := time.Now()
			_, _ = mwriter.WriteString(cmd)
			_ = mwriter.Flush()
			_, err := readSemanticLine(mconn, mreader, 10*time.Second)
			if err == nil {
				totalRTT += time.Since(start)
				samples++
			}
			consumePrompt(mconn, mreader)
		}
	}
	_ = cliWriter.Flush()

	if samples > 0 {
		avgRTT := totalRTT / time.Duration(samples)
		stats.add(avgRTT)
		emitStat(*csv, csvOut, "BULK_SET", count, totalBytes, avgRTT, "OK", len(msg))
	}
}

func runStressTest(addrs []string, cliWriter *bufio.Writer, mconn net.Conn, mreader *bufio.Reader, mwriter *bufio.Writer, csvOut *bufio.Writer, stats *rttStats) {
	endTime := time.Now().Add(time.Duration(*stressMins) * time.Minute)
	fmt.Fprintf(os.Stderr, "Starting stress test for %d minutes...\n", *stressMins)

	ops := []string{"SET", "GET", "FILE", "BULK_SET", "BULK_FILE"}

	for time.Now().Before(endTime) {
		op := ops[rand.Intn(len(ops))]
		switch op {
		case "SET":
			id := rand.Intn(1000000)
			val := fmt.Sprintf("stress_val_%d", rand.Intn(1000))
			cmd := fmt.Sprintf("SET %d %s\r\n", id, val)
			sendStressCmd(cmd, "SET", 1, len(val), cliWriter, mconn, mreader, mwriter, csvOut, stats)
		case "GET":
			id := rand.Intn(1000000)
			cmd := fmt.Sprintf("GET %d\r\n", id)
			sendStressCmd(cmd, "GET", 1, 0, cliWriter, mconn, mreader, mwriter, csvOut, stats)
		case "FILE":
			path, _ := selectRandomTestFile()
			if path != "" {
				content, _ := readWholeFile(path)
				id := rand.Intn(1000000)
				cmd := fmt.Sprintf("SET %d %s\r\n", id, content)
				sendStressCmd(cmd, "STRESS_FILE", 1, len(content), cliWriter, mconn, mreader, mwriter, csvOut, stats)
			}
		case "BULK_SET":
			count := 10 + rand.Intn(90)
			handleBulkSet(count, "stress_bulk", cliWriter, mconn, mreader, mwriter, csvOut, stats)
		case "BULK_FILE":
			count := 5 + rand.Intn(20)
			path, _ := selectRandomTestFile()
			if path != "" {
				handleBulkFile(count, path, cliWriter, mconn, mreader, mwriter, csvOut, stats)
			}
		}
		// Removed artificial sleep for maximum throughput
	}

	fmt.Fprintln(os.Stderr, "Stress test completed.")
	if stats.count > 0 {
		fmt.Fprintf(os.Stderr, "Summary: %d ops | Avg RTT: %v\n", stats.count, stats.sum/time.Duration(stats.count))
	}
}

func sendStressCmd(cmd, op string, count, payload int, cliWriter *bufio.Writer, mconn net.Conn, mreader *bufio.Reader, mwriter *bufio.Writer, csvOut *bufio.Writer, stats *rttStats) {
	_, _ = cliWriter.WriteString(cmd)
	_ = cliWriter.Flush()

	if *measure && mwriter != nil {
		start := time.Now()
		_, _ = mwriter.WriteString(cmd)
		_ = mwriter.Flush()
		_, err := readSemanticLine(mconn, mreader, 5*time.Second)
		rtt := time.Since(start)
		consumePrompt(mconn, mreader)
		status := "OK"
		if err != nil {
			status = "ERROR"
		}
		stats.add(rtt)
		emitStat(*csv, csvOut, op, count, int64(len(cmd)), rtt, status, payload)
	}
}
