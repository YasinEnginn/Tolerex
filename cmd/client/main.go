package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	defaultAddress    = "127.0.0.1:6666"
	dialTimeout       = 3 * time.Second
	retryDelay        = 2 * time.Second
	defaultBatchSize  = 1000
	serverReadyLine   = "Tolerex ready"
	interactivePrompt = "tolerex> "
	maxResponseBytes  = (4 << 20) + 1024
)

var (
	addrFlag       = flag.String("addr", defaultAddress, "Leader address or comma-separated addresses")
	measureFlag    = flag.Bool("measure", false, "Measure round-trip time for each command")
	csvFlag        = flag.Bool("csv", false, "Write measurements as CSV to stdout; requires -measure")
	batchFlag      = flag.Int("flush", defaultBatchSize, "Bulk batch size; measurement mode always uses 1")
	offsetFlag     = flag.Int("offset", 0, "Starting ID offset for generated bulk commands")
	clientIDFlag   = flag.String("id", "client", "Client identifier in CSV output")
	stressMinsFlag = flag.Int("stress", 0, "Stress duration in minutes")
	durationFlag   = flag.Duration("duration", 0, "Stress duration such as 30s or 5m; overrides -stress")
)

type session struct {
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
}

func parseAddresses(value string) []string {
	var addresses []string
	for _, address := range strings.Split(value, ",") {
		if address = strings.TrimSpace(address); address != "" {
			addresses = append(addresses, address)
		}
	}
	return addresses
}

func openSession(addresses []string) *session {
	for {
		current := append([]string(nil), addresses...)
		rand.Shuffle(len(current), func(i, j int) { current[i], current[j] = current[j], current[i] })
		for _, address := range current {
			conn, err := (&net.Dialer{Timeout: dialTimeout}).Dial("tcp", address)
			if err != nil {
				continue
			}
			if tcp, ok := conn.(*net.TCPConn); ok {
				_ = tcp.SetNoDelay(true)
			}
			client := &session{
				conn:   conn,
				reader: bufio.NewReaderSize(conn, 64*1024),
				writer: bufio.NewWriterSize(conn, 64*1024),
			}
			if err := client.readReady(5 * time.Second); err == nil {
				fmt.Fprintf(os.Stderr, "Connected to %s\n", address)
				return client
			}
			_ = conn.Close()
		}
		fmt.Fprintf(os.Stderr, "Cluster unreachable; retrying in %v\n", retryDelay)
		time.Sleep(retryDelay)
	}
}

func (s *session) readReady(timeout time.Duration) error {
	_ = s.conn.SetReadDeadline(time.Now().Add(timeout))
	defer s.conn.SetReadDeadline(time.Time{})
	line, err := s.reader.ReadString('\n')
	if err != nil {
		return err
	}
	if strings.TrimSpace(line) != serverReadyLine {
		return fmt.Errorf("unexpected server greeting %q", strings.TrimSpace(line))
	}
	return nil
}

func (s *session) write(command string) error {
	if _, err := s.writer.WriteString(command + "\r\n"); err != nil {
		return err
	}
	return nil
}

func (s *session) flush() error {
	return s.writer.Flush()
}

func (s *session) readResponse(timeout time.Duration) (string, error) {
	_ = s.conn.SetReadDeadline(time.Now().Add(timeout))
	defer s.conn.SetReadDeadline(time.Time{})

	var response strings.Builder
	for {
		fragment, err := s.reader.ReadSlice('\n')
		if response.Len()+len(fragment) > maxResponseBytes {
			return "", errors.New("response exceeds 4 MiB limit")
		}
		_, _ = response.Write(fragment)
		if err == nil {
			line := strings.TrimSuffix(response.String(), "\n")
			return strings.TrimSuffix(line, "\r"), nil
		}
		if !errors.Is(err, bufio.ErrBufferFull) {
			return "", err
		}
	}
}

func (s *session) execute(command string, timeout time.Duration) (string, time.Duration, error) {
	start := time.Now()
	if err := s.write(command); err != nil {
		return "", time.Since(start), err
	}
	if err := s.flush(); err != nil {
		return "", time.Since(start), err
	}
	response, err := s.readResponse(timeout)
	return response, time.Since(start), err
}

func responseStatus(response string, err error) string {
	if err != nil || strings.HasPrefix(response, "ERROR:") {
		return "ERROR"
	}
	if response == "NOT_FOUND" {
		return "NOT_FOUND"
	}
	return "OK"
}

func displayResponse(response string) string {
	if value, ok := strings.CutPrefix(response, "VALUE "); ok {
		return value
	}
	if help, ok := strings.CutPrefix(response, "HELP "); ok {
		return help
	}
	return response
}

func listTxtFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var files []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.EqualFold(filepath.Ext(entry.Name()), ".txt") {
			files = append(files, entry.Name())
		}
	}
	sort.Strings(files)
	return files, nil
}

func selectTestFileInteractive(stdin *bufio.Scanner) (string, error) {
	return selectTestFileInteractiveFrom("test_data", stdin)
}

func selectTestFileInteractiveFrom(dir string, stdin *bufio.Scanner) (string, error) {
	files, err := listTxtFiles(dir)
	if err != nil || len(files) == 0 {
		return "", fmt.Errorf("no .txt files found in %s", dir)
	}
	fmt.Fprintln(os.Stderr, "Available files:")
	for index, name := range files {
		fmt.Fprintf(os.Stderr, "  %d) %s\n", index+1, name)
	}
	fmt.Fprint(os.Stderr, "Select file: ")
	if !stdin.Scan() {
		if err := stdin.Err(); err != nil {
			return "", err
		}
		return "", io.EOF
	}
	choice, err := strconv.Atoi(strings.TrimSpace(stdin.Text()))
	if err != nil || choice < 1 || choice > len(files) {
		return "", errors.New("invalid file choice")
	}
	return filepath.Join(dir, files[choice-1]), nil
}

func readWholeFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	content := strings.TrimSpace(string(data))
	content = strings.NewReplacer("\r\n", " ", "\n", " ", "\r", " ", "\t", " ").Replace(content)
	return content, nil
}

func loadTestPayloads(dir string) ([]string, error) {
	files, err := listTxtFiles(dir)
	if err != nil || len(files) == 0 {
		return nil, fmt.Errorf("no .txt files found in %s", dir)
	}
	payloads := make([]string, 0, len(files))
	for _, name := range files {
		content, readErr := readWholeFile(filepath.Join(dir, name))
		if readErr != nil {
			return nil, readErr
		}
		payloads = append(payloads, content)
	}
	return payloads, nil
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
	if s.min == 0 || rtt < s.min {
		s.min = rtt
	}
	if rtt > s.max {
		s.max = rtt
	}
}

func emitStat(csvOut *bufio.Writer, operation string, count int, bytesSent int64, rtt time.Duration, status string, payloadSize int) {
	mode := "batched"
	if *batchFlag == 1 || *measureFlag {
		mode = "immediate"
	}
	if *csvFlag {
		fmt.Fprintf(csvOut, "%s,%s,%s,%d,%d,%d,%s,%s,%d\n",
			time.Now().Format(time.RFC3339Nano), *clientIDFlag, operation, count,
			bytesSent, rtt.Microseconds(), mode, status, payloadSize)
		_ = csvOut.Flush()
		return
	}
	fmt.Fprintf(os.Stderr, "RTT %s: %v | count=%d | bytes=%d | status=%s\n", operation, rtt, count, bytesSent, status)
}

func runBulk(client *session, count int, operation string, payloadSize int, build func(int) string, csvOut *bufio.Writer, stats *rttStats, report bool) error {
	if count <= 0 {
		return errors.New("bulk count must be greater than 0")
	}
	batchSize := *batchFlag
	if *measureFlag {
		batchSize = 1
	}
	var totalBytes int64
	var totalRTT time.Duration
	overallStatus := "OK"

	for start := 0; start < count; start += batchSize {
		end := min(start+batchSize, count)
		written := 0
		var started time.Time
		for index := start; index < end; index++ {
			command := build(index)
			if *measureFlag {
				started = time.Now()
			}
			totalBytes += int64(len(command) + 2)
			if err := client.write(command); err != nil {
				return err
			}
			written++
		}
		if err := client.flush(); err != nil {
			return err
		}
		for index := 0; index < written; index++ {
			response, readErr := client.readResponse(15 * time.Second)
			var rtt time.Duration
			if *measureFlag {
				rtt = time.Since(started)
			}
			status := responseStatus(response, readErr)
			if status != "OK" {
				overallStatus = status
			}
			if *measureFlag {
				totalRTT += rtt
				stats.add(rtt)
			}
			if readErr != nil {
				return readErr
			}
		}
	}

	if *measureFlag {
		emitStat(csvOut, operation, count, totalBytes, totalRTT/time.Duration(count), overallStatus, payloadSize)
	} else if report {
		fmt.Fprintf(os.Stderr, "%s completed: %d operations | status=%s\n", operation, count, overallStatus)
	}
	return nil
}

func runFile(client *session, path string, csvOut *bufio.Writer, stats *rttStats) {
	content, err := readWholeFile(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		return
	}
	id := *offsetFlag + rand.Intn(1_000_000)
	command := fmt.Sprintf("SET %d %s", id, content)
	response, rtt, err := client.execute(command, 15*time.Second)
	status := responseStatus(response, err)
	if *measureFlag {
		stats.add(rtt)
		emitStat(csvOut, "FILE_SET", 1, int64(len(command)+2), rtt, status, len(content))
	} else {
		fmt.Fprintln(os.Stdout, displayResponse(response))
	}
}

func runStress(client *session, duration time.Duration, csvOut *bufio.Writer, stats *rttStats) error {
	deadline := time.Now().Add(duration)
	payloads, payloadErr := loadTestPayloads("test_data")
	if payloadErr != nil {
		fmt.Fprintln(os.Stderr, "File payloads disabled:", payloadErr)
	}
	fmt.Fprintf(os.Stderr, "Stress test running for %s\n", duration)
	for time.Now().Before(deadline) {
		operationCount := 5
		if len(payloads) == 0 {
			operationCount = 3
		}
		switch rand.Intn(operationCount) {
		case 0:
			id := *offsetFlag + rand.Intn(1_000_000)
			if err := sendStress(client, fmt.Sprintf("SET %d stress_%d", id, id), "SET", csvOut, stats); err != nil {
				return err
			}
		case 1:
			id := *offsetFlag + rand.Intn(1_000_000)
			if err := sendStress(client, fmt.Sprintf("GET %d", id), "GET", csvOut, stats); err != nil {
				return err
			}
		case 2:
			base := *offsetFlag + rand.Intn(1_000_000)
			if err := runBulk(client, 10+rand.Intn(90), "BULK_SET", 11, func(index int) string {
				id := base + index
				return fmt.Sprintf("SET %d stress_bulk_%d", id, id)
			}, csvOut, stats, false); err != nil {
				return err
			}
		case 3:
			content := payloads[rand.Intn(len(payloads))]
			id := *offsetFlag + rand.Intn(1_000_000)
			if err := sendStress(client, fmt.Sprintf("SET %d %s", id, content), "FILE_SET", csvOut, stats); err != nil {
				return err
			}
		case 4:
			content := payloads[rand.Intn(len(payloads))]
			base := *offsetFlag + rand.Intn(1_000_000)
			if err := runBulk(client, 5+rand.Intn(20), "BULK_FILE", len(content), func(index int) string {
				return fmt.Sprintf("SET %d %s", base+index, content)
			}, csvOut, stats, false); err != nil {
				return err
			}
		}
	}
	fmt.Fprintln(os.Stderr, "Stress test completed")
	return nil
}

func sendStress(client *session, command, operation string, csvOut *bufio.Writer, stats *rttStats) error {
	response, rtt, err := client.execute(command, 15*time.Second)
	if *measureFlag {
		stats.add(rtt)
		emitStat(csvOut, operation, 1, int64(len(command)+2), rtt, responseStatus(response, err), 0)
	}
	return err
}

func main() {
	flag.Parse()
	addresses := parseAddresses(*addrFlag)
	if len(addresses) == 0 || *batchFlag < 1 || *stressMinsFlag < 0 || *durationFlag < 0 {
		fmt.Fprintln(os.Stderr, "ERROR: provide an address, -flush >= 1, and a non-negative stress duration")
		os.Exit(2)
	}
	if *csvFlag && !*measureFlag {
		fmt.Fprintln(os.Stderr, "ERROR: -csv requires -measure")
		os.Exit(2)
	}

	var csvOut *bufio.Writer
	if *csvFlag {
		csvOut = bufio.NewWriterSize(os.Stdout, 64*1024)
		fmt.Fprintln(csvOut, "Timestamp,ClientID,Operation,Count,Bytes,RTT_us,Mode,Status,PayloadSize")
		_ = csvOut.Flush()
	}

	client := openSession(addresses)
	defer client.conn.Close()
	stats := &rttStats{}

	stressDuration := *durationFlag
	if stressDuration == 0 && *stressMinsFlag > 0 {
		stressDuration = time.Duration(*stressMinsFlag) * time.Minute
	}
	if stressDuration > 0 {
		if err := runStress(client, stressDuration, csvOut, stats); err != nil {
			fmt.Fprintln(os.Stderr, "ERROR: stress test stopped:", err)
			os.Exit(1)
		}
		if stats.count > 0 {
			fmt.Fprintf(os.Stderr, "RTT summary: samples=%d avg=%v min=%v max=%v\n", stats.count, stats.sum/time.Duration(stats.count), stats.min, stats.max)
		}
		return
	}

	stdin := bufio.NewScanner(os.Stdin)
	stdin.Buffer(make([]byte, 1024), 10*1024*1024)
	stdinInfo, _ := os.Stdin.Stat()
	showPrompt := stdinInfo != nil && stdinInfo.Mode()&os.ModeCharDevice != 0
	for {
		if showPrompt {
			fmt.Fprint(os.Stderr, interactivePrompt)
		}
		if !stdin.Scan() {
			break
		}
		line := strings.TrimSpace(stdin.Text())
		if line == "" {
			continue
		}

		if strings.EqualFold(line, "FILE") || strings.HasSuffix(strings.ToUpper(line), " FILE") {
			path, err := selectTestFileInteractive(stdin)
			if err != nil {
				fmt.Fprintln(os.Stderr, "ERROR:", err)
				continue
			}
			fields := strings.Fields(line)
			if len(fields) == 2 {
				count, err := strconv.Atoi(fields[0])
				if err == nil {
					content, readErr := readWholeFile(path)
					if readErr != nil {
						fmt.Fprintln(os.Stderr, "ERROR:", readErr)
						continue
					}
					base := *offsetFlag + rand.Intn(1_000_000)
					if err := runBulk(client, count, "BULK_FILE", len(content), func(index int) string {
						return fmt.Sprintf("SET %d %s", base+index, content)
					}, csvOut, stats, true); err != nil {
						fmt.Fprintln(os.Stderr, "ERROR:", err)
					}
					continue
				}
			}
			runFile(client, path, csvOut, stats)
			continue
		}

		fields := strings.Fields(line)
		if len(fields) >= 3 && strings.EqualFold(fields[1], "SET") {
			if count, err := strconv.Atoi(fields[0]); err == nil {
				message := strings.Join(fields[2:], " ")
				base := *offsetFlag + rand.Intn(1_000_000)
				if err := runBulk(client, count, "BULK_SET", len(message), func(index int) string {
					id := base + index
					return fmt.Sprintf("SET %d %s_%d", id, message, id)
				}, csvOut, stats, true); err != nil {
					fmt.Fprintln(os.Stderr, "ERROR:", err)
				}
				continue
			}
		}

		response, rtt, err := client.execute(line, 15*time.Second)
		status := responseStatus(response, err)
		if *measureFlag {
			stats.add(rtt)
			emitStat(csvOut, strings.ToUpper(fields[0]), 1, int64(len(line)+2), rtt, status, 0)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "ERROR:", err)
			break
		}
		if !*csvFlag {
			fmt.Fprintln(os.Stdout, displayResponse(response))
		}
		if strings.EqualFold(fields[0], "QUIT") || strings.EqualFold(fields[0], "EXIT") {
			break
		}
	}

	if stats.count > 0 {
		fmt.Fprintf(os.Stderr, "RTT summary: samples=%d avg=%v min=%v max=%v\n", stats.count, stats.sum/time.Duration(stats.count), stats.min, stats.max)
	}
}
