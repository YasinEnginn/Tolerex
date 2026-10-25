package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestStorageModePayloadCombinationMatrix(t *testing.T) {
	payloads := []string{
		"",
		"plain ASCII",
		"  repeated   spaces  ",
		"Türkçe 世界 🚀 tolerex> ERROR:",
		"line one\r\nline two\nline three",
		"zero\x00byte",
		strings.Repeat("x", 1<<20),
	}
	modes := []string{ModeBuffered, ModeUnbuffered}

	for modeIndex, mode := range modes {
		baseDir := t.TempDir()
		for payloadIndex, payload := range payloads {
			id := modeIndex*100 + payloadIndex
			if err := WriteMessage(baseDir, id, payload, mode); err != nil {
				t.Fatalf("mode=%s payload=%d: write failed: %v", mode, payloadIndex, err)
			}
			got, err := ReadMessage(baseDir, id)
			if err != nil || got != payload {
				t.Fatalf("mode=%s payload=%d: got bytes=%d err=%v", mode, payloadIndex, len(got), err)
			}
		}
	}
}

func TestStorageOverwriteModeMatrix(t *testing.T) {
	modes := []string{ModeBuffered, ModeUnbuffered}
	for _, firstMode := range modes {
		for _, secondMode := range modes {
			baseDir := t.TempDir()
			if err := WriteMessage(baseDir, 1, "first-"+firstMode, firstMode); err != nil {
				t.Fatal(err)
			}
			want := "second-" + secondMode
			if err := WriteMessage(baseDir, 1, want, secondMode); err != nil {
				t.Fatal(err)
			}
			if got, err := ReadMessage(baseDir, 1); err != nil || got != want {
				t.Fatalf("%s -> %s: got=%q err=%v", firstMode, secondMode, got, err)
			}
		}
	}
}

func TestConcurrentAtomicOverwriteModeMatrix(t *testing.T) {
	modes := []string{ModeBuffered, ModeUnbuffered}
	for _, leftMode := range modes {
		for _, rightMode := range modes {
			baseDir := t.TempDir()
			payloads := make(map[string]struct{})
			var wg sync.WaitGroup
			errorsCh := make(chan error, 24)
			for index := 0; index < 24; index++ {
				mode := leftMode
				if index%2 == 1 {
					mode = rightMode
				}
				payload := fmt.Sprintf("%s-%02d-%s", mode, index, strings.Repeat("x", 4096+index))
				payloads[payload] = struct{}{}
				wg.Add(1)
				go func() {
					defer wg.Done()
					if err := WriteMessage(baseDir, 7, payload, mode); err != nil {
						errorsCh <- err
					}
				}()
			}
			wg.Wait()
			close(errorsCh)
			for err := range errorsCh {
				t.Fatalf("%s/%s concurrent write: %v", leftMode, rightMode, err)
			}

			got, err := ReadMessage(baseDir, 7)
			if err != nil {
				t.Fatal(err)
			}
			if _, complete := payloads[got]; !complete {
				t.Fatalf("%s/%s produced a torn payload of %d bytes", leftMode, rightMode, len(got))
			}
			entries, err := os.ReadDir(filepath.Join(baseDir, "messages"))
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				if strings.HasSuffix(entry.Name(), ".tmp") {
					t.Fatalf("temporary file leaked: %s", entry.Name())
				}
			}
		}
	}
}
