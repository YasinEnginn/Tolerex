package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadToleranceCombinationMatrix(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    int
		wantErr bool
	}{
		{name: "one", content: "TOLERANCE=1", want: 1},
		{name: "spaces", content: "  TOLERANCE = 2  \r\n", want: 2},
		{name: "explicit-plus", content: "TOLERANCE=+3", want: 3},
		{name: "large", content: "TOLERANCE=2147483647", want: 2147483647},
		{name: "empty", content: "", wantErr: true},
		{name: "missing-equals", content: "TOLERANCE 2", wantErr: true},
		{name: "wrong-key", content: "REPLICAS=2", wantErr: true},
		{name: "lowercase-key", content: "tolerance=2", wantErr: true},
		{name: "zero", content: "TOLERANCE=0", wantErr: true},
		{name: "negative", content: "TOLERANCE=-1", wantErr: true},
		{name: "not-integer", content: "TOLERANCE=two", wantErr: true},
		{name: "decimal", content: "TOLERANCE=2.5", wantErr: true},
		{name: "extra-value", content: "TOLERANCE=2=3", wantErr: true},
		{name: "extra-line", content: "TOLERANCE=2\nOTHER=3", wantErr: true},
		{name: "commented-value", content: "TOLERANCE=2 # replicas", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "tolerance.conf")
			if err := os.WriteFile(path, []byte(test.content), 0o600); err != nil {
				t.Fatal(err)
			}
			got, err := ReadTolerance(path)
			if (err != nil) != test.wantErr || (!test.wantErr && got != test.want) {
				t.Fatalf("ReadTolerance()=%d, err=%v, want=%d, wantErr=%v", got, err, test.want, test.wantErr)
			}
		})
	}
}

func TestReadToleranceMissingFile(t *testing.T) {
	if _, err := ReadTolerance(filepath.Join(t.TempDir(), "missing.conf")); err == nil {
		t.Fatal("expected an error")
	}
}
