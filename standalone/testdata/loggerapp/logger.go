package loggerapp

import "sync"

type Entry struct {
	Level, Message string
	Attributes     []any
}
type Logger struct {
	mu      sync.Mutex
	entries []Entry
}

func (l *Logger) add(level, message string, attributes []any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, Entry{level, message, append([]any(nil), attributes...)})
}
func (l *Logger) Debug(message string, attributes ...any) { l.add("debug", message, attributes) }
func (l *Logger) Info(message string, attributes ...any)  { l.add("info", message, attributes) }
func (l *Logger) Warn(message string, attributes ...any)  { l.add("warn", message, attributes) }
func (l *Logger) Error(message string, attributes ...any) { l.add("error", message, attributes) }
func (l *Logger) Snapshot() []Entry {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]Entry(nil), l.entries...)
}
