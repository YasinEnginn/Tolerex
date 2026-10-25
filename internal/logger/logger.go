package logger

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"unicode"

	"gopkg.in/natefinch/lumberjack.v2"
)

type ctxKey string

const RequestIDKey ctxKey = "request_id"

var (
	Leader = log.New(io.Discard, "[LEADER] ", 0)
	Member = log.New(io.Discard, "[MEMBER] ", 0)
)

type Level int

const (
	DEBUG Level = iota
	INFO
	WARN
	ERROR
	FATAL
)

var currentLevel atomic.Int32

func init() {
	currentLevel.Store(int32(INFO))
}

func Enabled(level Level) bool {
	return int32(level) >= currentLevel.Load()
}

func SetLevel(level Level) {
	currentLevel.Store(int32(level))
}

func parseLevel(value string) (Level, bool) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "debug":
		return DEBUG, true
	case "", "info":
		return INFO, true
	case "warn", "warning":
		return WARN, true
	case "error":
		return ERROR, true
	default:
		return INFO, false
	}
}

func safeName(value string) string {
	value = strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, value)
	if value == "" {
		return "tolerex"
	}
	return value
}

// Init configures stderr logging and, unless TOLEREX_LOG_FILE=off, a separate
// rotating file for this process component.
func Init(component string) {
	if level, ok := parseLevel(os.Getenv("TOLEREX_LOG_LEVEL")); ok {
		SetLevel(level)
	} else {
		fmt.Fprintf(os.Stderr, "invalid TOLEREX_LOG_LEVEL %q; using info\n", os.Getenv("TOLEREX_LOG_LEVEL"))
		SetLevel(INFO)
	}

	writers := []io.Writer{os.Stderr}
	if !strings.EqualFold(os.Getenv("TOLEREX_LOG_FILE"), "off") {
		baseDir := os.Getenv("TOLEREX_LOG_DIR")
		if baseDir == "" {
			baseDir = "logs"
		}
		if err := os.MkdirAll(baseDir, 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "log directory unavailable: %v\n", err)
		} else {
			writers = append(writers, &lumberjack.Logger{
				Filename:   filepath.Join(baseDir, safeName(component)+".log"),
				MaxSize:    5,
				MaxBackups: 3,
				MaxAge:     7,
				Compress:   true,
			})
		}
	}

	writer := io.MultiWriter(writers...)
	flags := log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile
	Leader = log.New(writer, "[LEADER] ", flags)
	Member = log.New(writer, "[MEMBER] ", flags)
}

func WithContext(ctx context.Context, base *log.Logger) *log.Logger {
	if ctx == nil || base == nil {
		return base
	}
	requestID, ok := ctx.Value(RequestIDKey).(string)
	if !ok || requestID == "" {
		return base
	}
	return log.New(base.Writer(), base.Prefix()+"[REQ:"+requestID+"] ", base.Flags())
}

func logWithLevel(target *log.Logger, level Level, tag, format string, values ...any) {
	if !Enabled(level) || target == nil {
		return
	}
	_ = target.Output(3, fmt.Sprintf("["+tag+"] "+format, values...))
}

func Debug(target *log.Logger, format string, values ...any) {
	logWithLevel(target, DEBUG, "DEBUG", format, values...)
}

func Info(target *log.Logger, format string, values ...any) {
	logWithLevel(target, INFO, "INFO", format, values...)
}

func Warn(target *log.Logger, format string, values ...any) {
	logWithLevel(target, WARN, "WARN", format, values...)
}

func Error(target *log.Logger, format string, values ...any) {
	logWithLevel(target, ERROR, "ERROR", format, values...)
}

func Fatal(target *log.Logger, format string, values ...any) {
	if target != nil {
		_ = target.Output(3, fmt.Sprintf("[FATAL] "+format, values...))
	}
	os.Exit(1)
}
