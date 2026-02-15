package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadTolerance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content string
		want    int
		wantErr bool
	}{
		{name: "valid", content: "TOLERANCE=2\n", want: 2},
		{name: "invalid key", content: "FOO=2\n", wantErr: true},
		{name: "invalid integer", content: "TOLERANCE=abc\n", wantErr: true},
		{name: "non positive", content: "TOLERANCE=0\n", wantErr: true},
		{name: "invalid format", content: "TOLERANCE:2\n", wantErr: true},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			path := filepath.Join(dir, "tolerance.conf")
			if err := os.WriteFile(path, []byte(tc.content), 0644); err != nil {
				t.Fatalf("write file failed: %v", err)
			}

			got, err := readTolerance(path)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got value=%d", got)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("readTolerance()=%d, want %d", got, tc.want)
			}
		})
	}
}
