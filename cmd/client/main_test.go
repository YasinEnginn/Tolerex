package main

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSessionUsesLineFraming(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()

	go func() {
		defer serverConn.Close()
		reader := bufio.NewReader(serverConn)
		writer := bufio.NewWriter(serverConn)
		_, _ = writer.WriteString(serverReadyLine + "\r\n")
		_ = writer.Flush()
		_, _ = reader.ReadString('\n')
		_, _ = writer.WriteString("VALUE prompt tolerex> remains data\r\n")
		_ = writer.Flush()
	}()

	client := &session{
		conn:   clientConn,
		reader: bufio.NewReader(clientConn),
		writer: bufio.NewWriter(clientConn),
	}
	if err := client.readReady(time.Second); err != nil {
		t.Fatal(err)
	}
	response, _, err := client.execute("GET 1", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got := displayResponse(response); got != "prompt tolerex> remains data" {
		t.Fatalf("unexpected response %q", got)
	}
}

func TestResponseStatus(t *testing.T) {
	for _, test := range []struct {
		response string
		want     string
	}{
		{response: "OK", want: "OK"},
		{response: "VALUE ERROR: stored payload", want: "OK"},
		{response: "NOT_FOUND", want: "NOT_FOUND"},
		{response: "ERROR: store failed", want: "ERROR"},
	} {
		if got := responseStatus(test.response, nil); got != test.want {
			t.Fatalf("responseStatus(%q)=%q, want %q", test.response, got, test.want)
		}
	}
}

func TestParseAddresses(t *testing.T) {
	got := parseAddresses(" 127.0.0.1:1, ,localhost:2 ")
	if len(got) != 2 || got[0] != "127.0.0.1:1" || got[1] != "localhost:2" {
		t.Fatalf("unexpected addresses: %v", got)
	}
}

func TestSelectTestFileInteractiveUsesProvidedInput(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "payload.txt"), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	stdin := bufio.NewScanner(strings.NewReader("1\n"))
	path, err := selectTestFileInteractiveFrom(dir, stdin)
	if err != nil {
		t.Fatal(err)
	}
	if path == "" {
		t.Fatal("expected a selected test file")
	}
}

func TestReadWholeFileMakesPayloadSingleLineWithoutCollapsingSpaces(t *testing.T) {
	path := filepath.Join(t.TempDir(), "payload.txt")
	if err := os.WriteFile(path, []byte("  first  value\r\nsecond\tvalue  "), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := readWholeFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if want := "first  value second value"; got != want {
		t.Fatalf("readWholeFile()=%q, want %q", got, want)
	}
}

func TestRunBulkFlushAndMeasurementCombinationMatrix(t *testing.T) {
	oldBatch, oldMeasure, oldCSV := *batchFlag, *measureFlag, *csvFlag
	defer func() {
		*batchFlag, *measureFlag, *csvFlag = oldBatch, oldMeasure, oldCSV
	}()

	for _, measured := range []bool{false, true} {
		for _, batchSize := range []int{1, 2, 8, 32} {
			for _, count := range []int{1, 2, 9, 17} {
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				commands := make(chan string, count)
				serverDone := make(chan error, 1)
				go func() {
					conn, acceptErr := listener.Accept()
					if acceptErr != nil {
						serverDone <- acceptErr
						return
					}
					defer conn.Close()
					reader := bufio.NewReader(conn)
					writer := bufio.NewWriter(conn)
					for range count {
						line, readErr := reader.ReadString('\n')
						if readErr != nil {
							serverDone <- readErr
							return
						}
						commands <- strings.TrimSpace(line)
						_, _ = writer.WriteString("OK\r\n")
						if flushErr := writer.Flush(); flushErr != nil {
							serverDone <- flushErr
							return
						}
					}
					serverDone <- nil
				}()

				conn, err := net.Dial("tcp", listener.Addr().String())
				if err != nil {
					t.Fatal(err)
				}
				client := &session{conn: conn, reader: bufio.NewReader(conn), writer: bufio.NewWriter(conn)}
				*batchFlag, *measureFlag, *csvFlag = batchSize, measured, measured
				stats := &rttStats{}
				var csv bytes.Buffer
				csvOut := bufio.NewWriter(&csv)
				err = runBulk(client, count, "MATRIX", 7, func(index int) string {
					return fmt.Sprintf("SET %d value-%d", index, index)
				}, csvOut, stats, false)
				_ = conn.Close()
				_ = listener.Close()
				if err != nil {
					t.Fatalf("measured=%v batch=%d count=%d: %v", measured, batchSize, count, err)
				}
				if serverErr := <-serverDone; serverErr != nil {
					t.Fatalf("measured=%v batch=%d count=%d server: %v", measured, batchSize, count, serverErr)
				}
				close(commands)
				index := 0
				for command := range commands {
					want := fmt.Sprintf("SET %d value-%d", index, index)
					if command != want {
						t.Fatalf("measured=%v batch=%d count=%d command=%q want=%q", measured, batchSize, count, command, want)
					}
					index++
				}
				if index != count {
					t.Fatalf("received %d commands, want %d", index, count)
				}
				if measured && (stats.count != int64(count) || !strings.Contains(csv.String(), ",MATRIX,")) {
					t.Fatalf("measurement missing: stats=%+v csv=%q", stats, csv.String())
				}
				if !measured && stats.count != 0 {
					t.Fatalf("unexpected measurements: %+v", stats)
				}
			}
		}
	}

	if err := runBulk(nil, 0, "MATRIX", 0, nil, nil, &rttStats{}, false); err == nil {
		t.Fatal("zero-sized bulk should fail")
	}
}

func TestResponseSizeBoundaries(t *testing.T) {
	for _, test := range []struct {
		name    string
		size    int
		wantErr bool
	}{
		{name: "largest-accepted-line", size: maxResponseBytes - 1},
		{name: "first-rejected-line", size: maxResponseBytes, wantErr: true},
	} {
		clientConn, serverConn := net.Pipe()
		client := &session{conn: clientConn, reader: bufio.NewReaderSize(clientConn, 4096), writer: bufio.NewWriter(clientConn)}
		serverDone := make(chan struct{})
		go func() {
			defer close(serverDone)
			_, _ = serverConn.Write([]byte(strings.Repeat("x", test.size) + "\n"))
			_ = serverConn.Close()
		}()
		response, err := client.readResponse(5 * time.Second)
		_ = clientConn.Close()
		<-serverDone
		if (err != nil) != test.wantErr {
			t.Fatalf("%s: bytes=%d err=%v", test.name, len(response), err)
		}
		if !test.wantErr && len(response) != test.size {
			t.Fatalf("%s: got %d bytes, want %d", test.name, len(response), test.size)
		}
	}
}
