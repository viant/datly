package transcribe

import (
	"os"
	"testing"

	"github.com/viant/datly/internal/testharness"
)

// Generated-fixture tests spawn go builds; cap fan-out so they do not
// oversubscribe the machine (see testharness.CapFixtureParallelism).
func TestMain(m *testing.M) {
	testharness.CapFixtureParallelism()
	os.Exit(m.Run())
}
