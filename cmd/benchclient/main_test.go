package main

import (
	"bufio"
	"math"
	"net"
	"strings"
	"testing"
	"time"
)

func TestBuildPayload(t *testing.T) {
	for _, size := range []int{32, 1024, 4096} {
		payload := buildPayload(size, 3, 7)
		if len(payload) != size {
			t.Fatalf("size=%d got=%d", size, len(payload))
		}
		if strings.ContainsAny(payload, "\r\n") {
			t.Fatalf("payload contains a line break")
		}
	}
}

func TestSummarize(t *testing.T) {
	values := []int64{1000, 2000, 3000, 4000, 5000}
	got := summarize(values)
	if got.Count != 5 || got.MinMS != 1 || got.P50MS != 3 || got.P95MS != 5 || got.P99MS != 5 || got.MaxMS != 5 {
		t.Fatalf("unexpected summary: %+v", got)
	}
	if math.Abs(got.MeanMS-3) > 0.0001 {
		t.Fatalf("mean=%v", got.MeanMS)
	}
}

func TestSummarizeEmpty(t *testing.T) {
	if got := summarize(nil); got != (latencySummary{}) {
		t.Fatalf("unexpected empty summary: %+v", got)
	}
}

func TestSessionExecute(t *testing.T) {
	server, client := netPipe(t)
	defer server.Close()
	defer client.Close()

	go func() {
		reader := bufio.NewReader(server)
		line, _ := reader.ReadString('\n')
		if strings.TrimSpace(line) == "STATUS" {
			_, _ = server.Write([]byte("OK\r\n"))
		}
	}()

	s := &session{conn: client, reader: bufio.NewReader(client), writer: bufio.NewWriter(client)}
	response, latency, err := s.execute("STATUS")
	if err != nil || response != "OK" || latency < 0 || latency > time.Second {
		t.Fatalf("response=%q latency=%v err=%v", response, latency, err)
	}
}

func netPipe(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	return net.Pipe()
}
