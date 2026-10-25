package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestWriteAndReadMessage(t *testing.T) {
	baseDir := t.TempDir()

	err := WriteMessage(baseDir, 1, "hello tolerex", "buffered")
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}

	msg, err := ReadMessage(baseDir, 1)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	if msg != "hello tolerex" {
		t.Errorf("expected 'hello tolerex', got '%s'", msg)
	}
}

func TestReadMessage_NotFound(t *testing.T) {
	baseDir := t.TempDir()

	_, err := ReadMessage(baseDir, 42)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestWriteMessage_Overwrite(t *testing.T) {
	baseDir := t.TempDir()

	if err := WriteMessage(baseDir, 5, "old", "unbuffered"); err != nil {
		t.Fatalf("first write failed: %v", err)
	}

	if err := WriteMessage(baseDir, 5, "new", "unbuffered"); err != nil {
		t.Fatalf("overwrite failed: %v", err)
	}

	msg, err := ReadMessage(baseDir, 5)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	if msg != "new" {
		t.Errorf("expected 'new', got '%s'", msg)
	}
}

func TestWriteMessage_CreatesDirectory(t *testing.T) {
	baseDir := t.TempDir()

	err := WriteMessage(baseDir, 7, "data", "buffered")
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}

	dir := filepath.Join(baseDir, "messages")
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("messages directory not created")
	}
}

func TestWriteMessageRejectsUnknownMode(t *testing.T) {
	err := WriteMessage(t.TempDir(), 1, "data", "unknown")
	if err == nil {
		t.Fatal("expected invalid mode error")
	}
}
