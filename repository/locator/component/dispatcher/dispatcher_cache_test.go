package dispatcher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/service/session"
	"github.com/viant/datly/view"
)

// TestResolveCacheDisabled covers propagation of the view cache setting into components
// dispatched as kind=component parameters. Previously the dispatched component always
// built a session with the zero value, so a request level Datly-Disable-Cache silently
// stopped at the dispatch boundary.
func TestResolveCacheDisabled(t *testing.T) {
	parentSession := func(cacheDisabled bool) context.Context {
		aSession := session.New(&view.View{Name: "parent"}, session.WithCacheDisabled(cacheDisabled))
		return aSession.Context(context.Background(), true)
	}

	var testCases = []struct {
		description string
		ctx         context.Context
		options     []contract.Option
		expect      bool
		expectOK    bool
	}{
		{
			description: "no dispatching session and no option leaves the setting unknown",
			ctx:         context.Background(),
			expectOK:    false,
		},
		{
			description: "inherits disabled cache from the dispatching session",
			ctx:         parentSession(true),
			expect:      true,
			expectOK:    true,
		},
		{
			description: "inherits enabled cache from the dispatching session",
			ctx:         parentSession(false),
			expect:      false,
			expectOK:    true,
		},
		{
			description: "explicit option wins over the dispatching session",
			ctx:         parentSession(true),
			options:     []contract.Option{contract.WithCacheDisabled(false)},
			expect:      false,
			expectOK:    true,
		},
		{
			description: "explicit option applies without a dispatching session",
			ctx:         context.Background(),
			options:     []contract.Option{contract.WithCacheDisabled(true)},
			expect:      true,
			expectOK:    true,
		},
	}

	for _, testCase := range testCases {
		actual, ok := resolveCacheDisabled(testCase.ctx, contract.NewOptions(testCase.options...))
		assert.Equal(t, testCase.expectOK, ok, testCase.description)
		if !testCase.expectOK {
			continue
		}
		assert.Equal(t, testCase.expect, actual, testCase.description)
	}
}
