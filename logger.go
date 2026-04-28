package main

import (
	"log/slog"
	"os"
)

// logger is the package-wide slog handler. Debug-level so step traces
// surface during development; switch to LevelInfo for prod once the
// pipeline is stable.
var logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
	Level: slog.LevelDebug,
}))

// logStep emits an info-level log tagged with the originating step
// (convention: "<file>.<Method>", e.g. "rssconnector.Fetch").
func logStep(step, msg string, args ...any) {
	logger.Info(msg, append([]any{"step", step}, args...)...)
}

// logDebug is the verbose counterpart of logStep — for trace events that
// shouldn't clutter info-level output.
func logDebug(step, msg string, args ...any) {
	logger.Debug(msg, append([]any{"step", step}, args...)...)
}

// logWarn flags a recoverable anomaly: a single-item failure, a skipped
// branch, or a context cancellation propagating through.
func logWarn(step, msg string, args ...any) {
	logger.Warn(msg, append([]any{"step", step}, args...)...)
}

// logError records a failure. The message is the error's text; callers
// supply additional kv args for the surrounding context (ids, urls, etc).
func logError(step string, err error, args ...any) {
	logger.Error(err.Error(), append([]any{"step", step}, args...)...)
}
