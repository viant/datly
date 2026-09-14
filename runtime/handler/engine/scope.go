package engine

import (
	"github.com/viant/bindly/locator"
	dexec "github.com/viant/datly/exec"
)

type providerScope struct {
	providers []locator.Provider
}

func (s *providerScope) Providers() []locator.Provider {
	if s == nil {
		return nil
	}
	return append([]locator.Provider(nil), s.providers...)
}

// ComposeScope makes explicit child providers the per-name authority over an
// inherited protocol scope. The result can be inherited by deeper invocations.
func ComposeScope(parent dexec.ProviderScope, child ...locator.Provider) (dexec.ProviderScope, error) {
	parentProviders := []locator.Provider(nil)
	if parent != nil {
		parentProviders = parent.Providers()
	}
	if len(parentProviders) == 0 && len(child) == 0 {
		return nil, nil
	}
	providers, err := (providerComposer{}).compose(providerComposition{
		protocol: parentProviders,
		child:    child,
	})
	if err != nil {
		return nil, err
	}
	return &providerScope{providers: providers}, nil
}
