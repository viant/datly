package engine

import (
	"fmt"
	"sort"

	"github.com/viant/bindly/locator"
	dexec "github.com/viant/datly/exec"
	rhandler "github.com/viant/datly/runtime/handler"
	xhandler "github.com/viant/xdatly/handler"
)

type providerComposition struct {
	component []locator.Provider
	protocol  []locator.Provider
	child     []locator.Provider
	runtime   []locator.Provider
	constants locator.Provider
}

type providerComposer struct{}

func (providerComposer) compose(input providerComposition) ([]locator.Provider, error) {
	component, err := indexProviderLayer("component", input.component, protectedComponentKind)
	if err != nil {
		return nil, err
	}
	protocol, err := indexProviderLayer("protocol", input.protocol, protectedProtocolKind)
	if err != nil {
		return nil, err
	}
	child, err := indexProviderLayer("child", input.child, protectedProtocolKind)
	if err != nil {
		return nil, err
	}
	runtime, err := indexProviderLayer("runtime", input.runtime, nil)
	if err != nil {
		return nil, err
	}
	if input.constants != nil {
		if input.constants.Kind() != "const" {
			return nil, fmt.Errorf("canonical constants provider has kind %q, want const", input.constants.Kind())
		}
		if _, ok := runtime["const"]; ok {
			return nil, fmt.Errorf("runtime provider kind %q is duplicated", "const")
		}
		runtime["const"] = input.constants
	}

	kinds := make(map[string]struct{}, len(component)+len(protocol)+len(child)+len(runtime))
	for kind := range component {
		kinds[kind] = struct{}{}
	}
	for kind := range protocol {
		kinds[kind] = struct{}{}
	}
	for kind := range child {
		kinds[kind] = struct{}{}
	}
	for kind := range runtime {
		kinds[kind] = struct{}{}
	}
	ordered := make([]string, 0, len(kinds))
	for kind := range kinds {
		ordered = append(ordered, kind)
	}
	sort.Strings(ordered)

	result := make([]locator.Provider, 0, len(ordered))
	for _, kind := range ordered {
		layers := make([]locator.ProviderLayer, 0, 4)
		if provider := runtime[kind]; provider != nil {
			layers = append(layers, locator.ProviderLayer{Name: "runtime", Provider: provider})
		}
		if provider := child[kind]; provider != nil {
			layers = append(layers, locator.ProviderLayer{Name: "child", Provider: provider})
		}
		if provider := protocol[kind]; provider != nil {
			layers = append(layers, locator.ProviderLayer{Name: "protocol", Provider: provider})
		}
		if provider := component[kind]; provider != nil {
			layers = append(layers, locator.ProviderLayer{Name: "component", Provider: provider})
		}
		if len(layers) == 1 {
			result = append(result, layers[0].Provider)
			continue
		}
		provider, composeErr := locator.ComposeProviders(kind, layers...)
		if composeErr != nil {
			return nil, composeErr
		}
		result = append(result, provider)
	}
	return result, nil
}

func indexProviderLayer(name string, providers []locator.Provider, protected func(string) bool) (map[string]locator.Provider, error) {
	result := make(map[string]locator.Provider, len(providers))
	for index, provider := range providers {
		if provider == nil {
			return nil, fmt.Errorf("%s provider at index %d is required", name, index)
		}
		kind := provider.Kind()
		if kind == "" {
			return nil, fmt.Errorf("%s provider at index %d has no kind", name, index)
		}
		if protected != nil && protected(kind) {
			return nil, fmt.Errorf("%s provider cannot supply protected runtime kind %q", name, kind)
		}
		if _, ok := result[kind]; ok {
			return nil, fmt.Errorf("%s provider kind %q is duplicated", name, kind)
		}
		result[kind] = provider
	}
	return result, nil
}

func protectedComponentKind(kind string) bool {
	return kind != "const" && protectedRuntimeKind(kind)
}

func protectedProtocolKind(kind string) bool {
	return protectedRuntimeKind(kind)
}

func protectedRuntimeKind(kind string) bool {
	switch kind {
	case "input", "param", "caller_output", "component", string(xhandler.DataKey), string(xhandler.DMLKey),
		string(xhandler.SequencerKey), string(xhandler.FlusherKey),
		string(dexec.ComponentInvokerKey),
		string(dexec.InvocationKey),
		string(dexec.ReaderInputPreparerKey),
		string(xhandler.InputSnapshotKey),
		string(xhandler.ReadMetadataKey),
		string(xhandler.TransactionStarterKey),
		string(rhandler.TransactionSQLCapabilityKey),
		string(xhandler.FrameworkValidatorKey),
		string(rhandler.ConnectorCapabilityKey), string(rhandler.DifferCapabilityKey),
		string(rhandler.ValidatorCapabilityKey), string(rhandler.LoggerCapabilityKey), string(rhandler.MessageBusCapabilityKey):
		return true
	default:
		return false
	}
}
