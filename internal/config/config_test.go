package config

import (
	"io"
	"log"
	"os"
	"path/filepath"
	"testing"
	"tolerex/internal/logger"
)

func init() {
	// Initialize logger to discard output for tests
	logger.Member = log.New(io.Discard, "", 0)
	logger.Leader = log.New(io.Discard, "", 0)
}

func TestReadTolerance_Valid(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tolerance.conf")

	content := "TOLERANCE=3"
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write temp config: %v", err)
	}

	val, err := ReadTolerance(path)
	if err != nil {
		t.Fatalf("ReadTolerance failed: %v", err)
	}

	if val != 3 {
		t.Errorf("expected 3, got %d", val)
	}
}

func TestReadTolerance_InvalidKey(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tolerance.conf")

	content := "INVALID=3"
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write temp config: %v", err)
	}

	_, err := ReadTolerance(path)
	if err == nil {
		t.Fatal("expected error for invalid key, got nil")
	}
}

func TestReadTolerance_InvalidValue(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tolerance.conf")

	content := "TOLERANCE=-1"
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write temp config: %v", err)
	}

	_, err := ReadTolerance(path)
	if err == nil {
		t.Fatal("expected error for negative tolerance, got nil")
	}
}

func TestReadTolerance_MissingFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "missing.conf")

	_, err := ReadTolerance(path)
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

func TestReadTolerance_InvalidFormatNoEquals(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tolerance.conf")

	content := "TOLERANCE 3"
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write temp config: %v", err)
	}

	_, err := ReadTolerance(path)
	if err == nil {
		t.Fatal("expected error for invalid format, got nil")
	}
}

func TestReadTolerance_InvalidValueNotInteger(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tolerance.conf")

	content := "TOLERANCE=abc"
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write temp config: %v", err)
	}

	_, err := ReadTolerance(path)
	if err == nil {
		t.Fatal("expected error for non-integer tolerance, got nil")
	}
}
