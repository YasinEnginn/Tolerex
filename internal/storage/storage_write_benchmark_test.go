package storage

import (
	"io"
	"log"
	"testing"

	"tolerex/internal/logger"
)

const benchmarkPayload = "TOLEREX_STORAGE_TEST"

func initBenchmarkLogger() {
	logger.Member = log.New(io.Discard, "", 0)
	logger.Leader = log.New(io.Discard, "", 0)
}

func benchmarkWriteMessageMode(b *testing.B, mode string) {
	initBenchmarkLogger()
	baseDir := b.TempDir()

	b.ReportAllocs()
	b.SetBytes(int64(len(benchmarkPayload)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := WriteMessage(baseDir, i, benchmarkPayload, mode); err != nil {
			b.Fatalf("WriteMessage failed: %v", err)
		}
	}
}

func BenchmarkWriteMessage_Buffered(b *testing.B) {
	benchmarkWriteMessageMode(b, "buffered")
}

func BenchmarkWriteMessage_Unbuffered(b *testing.B) {
	benchmarkWriteMessageMode(b, "unbuffered")
}
