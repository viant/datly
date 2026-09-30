package runtime

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/bindly/locator"
	"github.com/viant/structology"
)

type applicationOptionProvider string

func (p applicationOptionProvider) Kind() string                              { return string(p) }
func (applicationOptionProvider) Priority() int                               { return 0 }
func (applicationOptionProvider) DefaultCacheable() bool                      { return true }
func (p applicationOptionProvider) Locate(*structology.State) locator.Locator { return p }
func (p applicationOptionProvider) Value(context.Context, reflect.Type, string) (any, bool, error) {
	return string(p), true, nil
}

func TestWithApplicationProvidersValidatesKinds(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		providers []locator.Provider
	}{
		{name: "nil", providers: []locator.Provider{nil}},
		{name: "empty", providers: []locator.Provider{applicationOptionProvider("")}},
		{name: "duplicate", providers: []locator.Provider{applicationOptionProvider("auth"), applicationOptionProvider("auth")}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if err := WithApplicationProviders(testCase.providers...)(&options{}); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}
