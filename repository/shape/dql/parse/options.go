package parse

type (
	UnknownNonReadMode string

	Options struct {
		UnknownNonReadMode UnknownNonReadMode
	}

	Option func(*Options)
)

const (
	UnknownNonReadModeWarn  UnknownNonReadMode = "warn"
	UnknownNonReadModeError UnknownNonReadMode = "error"
)

func WithUnknownNonReadMode(mode UnknownNonReadMode) Option {
	return func(o *Options) {
		if o == nil {
			return
		}
		o.UnknownNonReadMode = mode
	}
}

func defaultOptions() Options {
	return Options{
		UnknownNonReadMode: UnknownNonReadModeWarn,
	}
}

func normalizeUnknownNonReadMode(mode UnknownNonReadMode) UnknownNonReadMode {
	switch mode {
	case UnknownNonReadModeWarn, UnknownNonReadModeError:
		return mode
	default:
		return UnknownNonReadModeWarn
	}
}
