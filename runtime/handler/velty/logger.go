package velty

import xlogger "github.com/viant/xdatly/logger"

type Logger struct {
	service xlogger.Logger
}

func (l *Logger) Debug(message string) string {
	if l != nil && l.service != nil {
		l.service.Debug(message)
	}
	return ""
}

func (l *Logger) Info(message string) string {
	if l != nil && l.service != nil {
		l.service.Info(message)
	}
	return ""
}

func (l *Logger) Warn(message string) string {
	if l != nil && l.service != nil {
		l.service.Warn(message)
	}
	return ""
}

func (l *Logger) Error(message string) string {
	if l != nil && l.service != nil {
		l.service.Error(message)
	}
	return ""
}
