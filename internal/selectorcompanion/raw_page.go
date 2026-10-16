package selectorcompanion

import (
	"context"
	"fmt"
	"reflect"
	"strconv"

	"github.com/viant/bindly"
	bindinput "github.com/viant/bindly/input"
	"github.com/viant/bindly/locator"
	requestprovider "github.com/viant/bindly/provider/request"
)

var nativeQueryProviderType = reflect.TypeOf(requestprovider.NewValues().Query())

// pageSource is invocation-only, unevaluated query authority. It never stores a
// substitute typed Page or fetches a value before the reader validates Fields.
type pageSource struct {
	provider locator.Provider
	binding  bindly.BindingSpec
}

func newPageSource(provider locator.Provider, binding bindly.BindingSpec) (*pageSource, error) {
	if provider == nil || reflect.TypeOf(provider) != nativeQueryProviderType || reflect.ValueOf(provider).IsNil() || provider.Kind() != "query" || binding.Location.Kind != "query" || binding.Location.In == "" {
		return nil, fmt.Errorf("deferred companion Page requires the exact native query provider")
	}
	return &pageSource{provider: provider, binding: binding}, nil
}

func (p *pageSource) resolve(ctx context.Context) (int, error) {
	locator := p.provider.Locate(nil)
	if locator == nil {
		return 0, p.failure(fmt.Errorf("native query locator is missing"))
	}
	for index, name := range []string{p.binding.Location.In, "_page"} {
		raw, present, err := locator.Value(ctx, nil, name)
		if err != nil {
			return 0, p.failure(err)
		}
		if !present {
			continue
		}
		values, ok := raw.([]string)
		if !ok {
			return 0, p.failure(fmt.Errorf("unexpected native query value %T", raw))
		}
		// The original locator returns one string for a single query value. Its
		// binding conversion handles that string before numeric selector conversion.
		// Repeated values instead reach the numeric selector as an unconverted string.
		if len(values) > 1 {
			return 0, p.failure(fmt.Errorf("invalid page value: unsupported type: string"))
		}
		text := ""
		if len(values) == 1 {
			text = values[0]
		}
		page := 0
		if text != "" {
			page, err = strconv.Atoi(text)
		}
		if err != nil {
			if index == 0 {
				continue
			}
			return 0, p.failure(err)
		}
		if page < 1 {
			page = 1
		}
		return page, nil
	}
	return 0, nil
}

func (p *pageSource) failure(cause error) error {
	b := p.binding
	return &bindly.BindingError{Name: b.Name, Path: b.Path, Kind: b.Location.Kind, In: b.Location.In, Code: b.ErrorCode, Message: b.ErrorMessage, Cause: &bindinput.Error{Cause: cause}}
}
