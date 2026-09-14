package reader_test

import (
	"github.com/viant/datly/runtime"
	"os"
	"sync/atomic"
	"time"
)

func parityReaderOption() runtime.Option {
	config := runtime.ObservabilityConfig{}
	if os.Getenv("PARITY_CALLBACK") == "1" {
		// A configured local callback with an observable effect, no formatting/I/O.
		count := &atomic.Int64{}
		config.ReadingData = func(_ string, _ time.Duration, _ string, rows int, _ []any, _ error) { count.Add(int64(rows)) }
	}
	return runtime.WithObservability(config)
}
