package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"tolerex/internal/logger"
)

const (
	ModeBuffered   = "buffered"
	ModeUnbuffered = "unbuffered"
)

var writeLocks [256]sync.Mutex

func writeLock(path string) *sync.Mutex {
	hash := uint32(2166136261)
	for index := 0; index < len(path); index++ {
		hash ^= uint32(path[index])
		hash *= 16777619
	}
	return &writeLocks[hash%uint32(len(writeLocks))]
}

func IsValidMode(mode string) bool {
	return mode == ModeBuffered || mode == ModeUnbuffered
}

// WriteMessage atomically replaces the message file. Unbuffered mode also
// synchronizes the temporary file before the rename.
func WriteMessage(baseDir string, id int, text string, mode string) error {
	dir := filepath.Join(baseDir, "messages")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		logger.Error(logger.Member, "WriteMessage: failed to create messages dir path=%s err=%v", dir, err)
		return err
	}

	switch mode {
	case ModeUnbuffered:
		logger.Debug(logger.Member, "WriteMessage: writing unbuffered msg_id=%d", id)
		return writeAtomic(dir, id, text, true)
	case ModeBuffered:
		logger.Debug(logger.Member, "WriteMessage: writing buffered msg_id=%d", id)
		return writeAtomic(dir, id, text, false)
	default:
		return fmt.Errorf("invalid I/O mode %q", mode)
	}
}

func writeAtomic(dir string, id int, text string, durable bool) (err error) {
	filename := filepath.Join(dir, strconv.Itoa(id)+".msg")
	lock := writeLock(filename)
	lock.Lock()
	defer lock.Unlock()

	f, err := os.CreateTemp(dir, "."+strconv.Itoa(id)+"-*.tmp")
	if err != nil {
		logger.Error(logger.Member, "writeAtomic: failed to create temp file id=%d err=%v", id, err)
		return err
	}
	tempName := f.Name()
	defer func() {
		_ = f.Close()
		if err != nil {
			_ = os.Remove(tempName)
		}
	}()
	if err = f.Chmod(0o644); err != nil {
		return err
	}

	if _, err = f.WriteString(text); err != nil {
		logger.Error(logger.Member, "writeAtomic: failed to write file id=%d path=%s err=%v", id, filename, err)
		return err
	}
	if durable {
		if err = f.Sync(); err != nil {
			return err
		}
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(tempName, filename); err != nil {
		return err
	}
	return nil
}
