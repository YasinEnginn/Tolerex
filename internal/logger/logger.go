// ===================================================================================
// TOLEREX – CENTRALIZED LOGGING SUBSYSTEM
// ===================================================================================
//
// This package implements the centralized logging infrastructure used across
// the Tolerex distributed system.
//
// From a systems and observability perspective, this logger:
//
//   - Provides role-based loggers (Leader / Member)
//   - Supports runtime log-level filtering
//   - Uses simple, prefix-based structured logging
//   - Integrates log rotation via lumberjack to prevent unbounded disk growth
//   - Propagates request-scoped metadata (Request ID) via context
//
// Design principles:
//
//   - Minimal dependencies (standard log + lumberjack only)
//   - Deterministic, human-readable output
//   - Explicit control over behavior (no magic, no hidden defaults)
//   - Fail-fast semantics for unrecoverable errors
//
// This package intentionally avoids advanced logging frameworks (zap, zerolog)
// to keep behavior transparent and predictable in a systems-programming context.
//
// ===================================================================================

package logger

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"gopkg.in/natefinch/lumberjack.v2"
)

// ===================================================================================
// CONTEXT KEY DEFINITIONS
// ===================================================================================

// ctxKey is a strongly-typed context key to avoid collisions
// with keys defined in other packages.
type ctxKey string

// RequestIDKey is used to propagate request-scoped identifiers
// across goroutines and service boundaries.
const RequestIDKey ctxKey = "request_id"

// ===================================================================================
// ROLE-BASED LOGGER INSTANCES
// ===================================================================================
//
// Leader → coordination, control-plane, orchestration logs
// Member → storage, data-plane, persistence logs

var (
	Leader *log.Logger
	Member *log.Logger
)

// ===================================================================================
// LOG LEVEL DEFINITIONS
// ===================================================================================
//
// Log levels are ordered by severity.
// Messages below the current level are suppressed.

type Level int

const (
	DEBUG Level = iota // 0
	INFO               // 1
	WARN               // 2
	ERROR              // 3
	FATAL              // 4
)

// ===================================================================================
// LOG LEVEL MANAGEMENT
// ===================================================================================
//
// The current log level can be adjusted at runtime
// to control verbosity without restarting the process.
// currentLevel defines the global minimum log level.
// Default is INFO.
var currentLevel = INFO

// enabled returns true if the given level should be logged.
func enabled(level Level) bool {
	return level >= currentLevel
}

// SetLevel updates the global log verbosity at runtime.
func SetLevel(level Level) {
	currentLevel = level
}

// ===================================================================================
// LOG DIRECTORY RESOLUTION
// ===================================================================================
//
// In test mode, logs can be redirected to a temporary directory
// to avoid polluting production logs.

func logBaseDir() string {
	if os.Getenv("TOLEREX_TEST_MODE") == "1" {
		if d := os.Getenv("TOLEREX_TEST_DIR"); d != "" {
			return d
		}
	}
	return "logs"
}

// ===================================================================================
// LOGGER INITIALIZATION
// ===================================================================================
//
// Init initializes the shared logging backend with log rotation.
//
// Rotation policy:
//   - Max file size : 5 MB
//   - Max backups  : 7 files
//   - Max age      : 7 days
//   - Compression  : enabled (.gz)
//
// All loggers share the same sink but differ by prefix.
// Logs are written to both the rolling log file and stderr.

func Init() {
	baseDir := logBaseDir()
	_ = os.MkdirAll(baseDir, 0755)

	fileWriter := &lumberjack.Logger{
		Filename:   filepath.Join(baseDir, "tolerex.log"), // log file path
		MaxSize:    5,                                     // MB
		MaxBackups: 7,                                     // files
		MaxAge:     7,                                     // days
		Compress:   true,                                  // enabled

	}

	writer := io.MultiWriter(os.Stderr, fileWriter)

	// Include timestamps and source location for debugging
	flags := log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile

	Leader = log.New(writer, "[LEADER] ", flags)
	Member = log.New(writer, "[MEMBER] ", flags)
}

// ===================================================================================
// CONTEXT-AWARE LOGGER
// ===================================================================================
//
// WithContext returns a derived logger enriched with request-scoped metadata.
// If a Request ID is present in the context, it is injected into the log prefix.
//
// Example output:
//   [LEADER] [REQ:abc123] [INFO] Store request received

func WithContext(ctx context.Context, base *log.Logger) *log.Logger {
	if ctx == nil {
		return base
	}

	if reqID, ok := ctx.Value(RequestIDKey).(string); ok {
		return log.New(
			base.Writer(),
			base.Prefix()+"[REQ:"+reqID+"] ",
			base.Flags(),
		)
	}
	return base
}

// ===================================================================================
// LOGGING HELPERS (LEVEL-AWARE)
// ===================================================================================

func logWithLevel(log *log.Logger, level Level, tag string, format string, v ...any) {
	if !enabled(level) {
		return
	}
	msg := fmt.Sprintf("["+tag+"] "+format, v...)
	_ = log.Output(4, msg)
}

// Debug logs diagnostic information useful during development.
func Debug(log *log.Logger, format string, v ...any) {
	logWithLevel(log, DEBUG, "DEBUG", format, v...)
}

// Info logs normal operational events.
func Info(log *log.Logger, format string, v ...any) {
	logWithLevel(log, INFO, "INFO", format, v...)
}

// Warn logs abnormal but recoverable conditions.
func Warn(log *log.Logger, format string, v ...any) {
	logWithLevel(log, WARN, "WARN", format, v...)
}

// Error logs failures that do not immediately terminate the process.
func Error(log *log.Logger, format string, v ...any) {
	logWithLevel(log, ERROR, "ERROR", format, v...)
}

// Fatal logs an unrecoverable error and immediately terminates the process.
// This should only be used for system-level failures.
func Fatal(log *log.Logger, format string, v ...any) {
	msg := fmt.Sprintf("[FATAL] "+format, v...)
	_ = log.Output(4, msg)
	os.Exit(1)
}
