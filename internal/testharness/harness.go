package testharness

import (
	"github.com/viant/datly/internal/testharness/sqlite"
	"testing"
)

// Harness adds component fixtures to the shared, dependency-light SQLite owner.
type Harness struct{ *sqlite.Harness }
type HarnessOption = sqlite.Option

func WithDSN(dsn string) HarnessOption { return sqlite.WithDSN(dsn) }
func NewSQLiteHarness(t *testing.T, opts ...HarnessOption) *Harness {
	t.Helper()
	return &Harness{Harness: sqlite.New(t, opts...)}
}
