package database

// Logger - just a logger interface for convenient injection
type Logger interface {
	Error(msg string, args ...any)
	Info(msg string, args ...any)
}

// NullSafeLogger - convenience wrapper above Logger
type NullSafeLogger struct {
	log Logger
}

// Error - logging wrapper
func (l *NullSafeLogger) Error(msg string, args ...any) {
	if l.log == nil {
		return
	}
	l.Error(msg, args...)
}

// Info - logging wrapper
func (l *NullSafeLogger) Info(msg string, args ...any) {
	if l.log == nil {
		return
	}
	l.Info(msg, args...)
}
