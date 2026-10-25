package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// ReadTolerance parses TOLERANCE=<positive integer>.
func ReadTolerance(path string) (int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	parts := strings.SplitN(strings.TrimSpace(string(data)), "=", 2)
	if len(parts) != 2 || strings.TrimSpace(parts[0]) != "TOLERANCE" {
		return 0, fmt.Errorf("invalid tolerance config; expected TOLERANCE=<positive integer>")
	}
	tolerance, err := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil {
		return 0, fmt.Errorf("tolerance must be a valid integer: %w", err)
	}
	if tolerance <= 0 {
		return 0, fmt.Errorf("tolerance must be greater than 0")
	}
	return tolerance, nil
}
