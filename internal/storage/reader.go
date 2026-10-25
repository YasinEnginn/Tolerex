package storage

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"

	"tolerex/internal/logger"
)

var ErrNotFound = errors.New("message not found")

// ReadMessage reads one message and maps a missing file to ErrNotFound.
func ReadMessage(baseDir string, id int) (string, error) {
	filename := filepath.Join(
		baseDir,
		"messages",
		strconv.Itoa(id)+".msg",
	)

	data, err := os.ReadFile(filename)
	if err != nil {
		if os.IsNotExist(err) {
			logger.Debug(logger.Member, "ReadMessage: file not found id=%d path=%s", id, filename)
			return "", ErrNotFound
		}

		logger.Error(logger.Member, "ReadMessage: read error id=%d path=%s err=%v", id, filename, err)
		return "", err
	}

	return string(data), nil
}
