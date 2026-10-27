package logging

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	xexec "github.com/viant/xdatly/exec"
)

// A single causal sequence proves completion ownership survives both application
// and metric callback panics without replacing the original application panic.
func TestHTTPCompletionRegistrationAndPanicOwnership(t *testing.T) {
	original := &struct{ message string }{"application panic"}
	factories, completions, logged := 0, 0, 0
	defer func() {
		if recover() != original {
			t.Error("application panic was replaced")
		}
		if factories != 1 || completions != 1 || logged != 1 {
			t.Errorf("factories=%d completions=%d logged=%d", factories, completions, logged)
		}
	}()
	ServeHTTPObserved(httptest.NewRecorder(), httptest.NewRequest("PATCH", "/records", nil), "test", func(w http.ResponseWriter, r *http.Request) {
		begin := func() func(time.Time, int, bool) {
			factories++
			return func(_ time.Time, status int, failed bool) {
				completions++
				if status != 500 || !failed {
					t.Errorf("panic outcome status=%d failed=%v", status, failed)
				}
				panic("metric callback failure")
			}
		}
		if !RegisterHTTPCompletion(r.Context(), begin) || RegisterHTTPCompletion(r.Context(), begin) {
			t.Error("duplicate registration was admitted")
		}
		panic(original)
	}, func(context.Context, *xexec.Context) { logged++ })
}
