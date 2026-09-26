package golang

import (
	"os"
	"testing"

	"github.com/viant/datly/internal/testharness"
)

// Generated-fixture tests spawn race-instrumented go builds; cap fan-out so
// they do not oversubscribe the machine (see testharness.CapFixtureParallelism).
func TestMain(m *testing.M) {
	testharness.CapFixtureParallelism()
	os.Exit(m.Run())
}
