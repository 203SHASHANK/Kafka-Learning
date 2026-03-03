// Package logger provides a structured, leveled logger backed by uber-go/zap.
//
// WHY zap instead of the standard library log package?
//
//  1. Structured fields: `zap.String("topic", t)` vs `fmt.Sprintf("topic=%s", t)`
//     Structured logs can be parsed and indexed by tools like Datadog, Splunk, ELK.
//     String formatting is human-readable but unusable for log analytics.
//
//  2. Zero-allocation in hot paths: zap avoids fmt.Sprintf() overhead.
//     This matters when logging thousands of Kafka messages per second.
//
//  3. Levels with zero cost for disabled levels: calling log.Debug() when the
//     level is set to "info" has effectively zero overhead in zap.
//
//  4. Caller information: file:line added automatically.
package logger

import (
	"os"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	globalLogger *zap.Logger
	once         sync.Once
)

// Init initializes the global logger. Call once at program startup.
// Subsequent calls are no-ops (sync.Once ensures single initialization).
func Init(level, environment string) *zap.Logger {
	once.Do(func() {
		globalLogger = build(level, environment)
	})
	// Replace the global zap logger so libraries using zap.L() get our config.
	zap.ReplaceGlobals(globalLogger)
	return globalLogger
}

// Get returns the global logger. If Init was not called, uses info-level dev defaults.
func Get() *zap.Logger {
	if globalLogger == nil {
		return Init("info", "development")
	}
	return globalLogger
}

func build(levelStr, environment string) *zap.Logger {
	atomicLevel := zap.NewAtomicLevelAt(parseLevel(levelStr))

	var (
		encoderCfg zapcore.EncoderConfig
		encoder    zapcore.Encoder
	)

	if environment == "production" {
		// Production: JSON output for log aggregation systems.
		// Every field is a key-value pair that can be indexed and searched.
		encoderCfg = zap.NewProductionEncoderConfig()
		encoderCfg.TimeKey = "ts"
		encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder
		encoderCfg.EncodeLevel = zapcore.LowercaseLevelEncoder
		encoder = zapcore.NewJSONEncoder(encoderCfg)
	} else {
		// Development: human-readable colorized console output.
		encoderCfg = zap.NewDevelopmentEncoderConfig()
		encoderCfg.EncodeLevel = zapcore.CapitalColorLevelEncoder
		encoderCfg.EncodeTime = zapcore.TimeEncoderOfLayout("15:04:05.000")
		encoder = zapcore.NewConsoleEncoder(encoderCfg)
	}

	core := zapcore.NewCore(
		encoder,
		zapcore.AddSync(os.Stdout),
		atomicLevel,
	)

	return zap.New(core,
		zap.AddCaller(),    // Adds file:line to each log entry
		zap.AddStacktrace(zapcore.ErrorLevel), // Stack trace on error+
	)
}

func parseLevel(level string) zapcore.Level {
	switch level {
	case "debug":
		return zapcore.DebugLevel
	case "warn":
		return zapcore.WarnLevel
	case "error":
		return zapcore.ErrorLevel
	default:
		return zapcore.InfoLevel
	}
}
