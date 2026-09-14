// Package input projects compiled MCP payloads into canonical Bindly request
// providers. Bindly remains the sole binding and conversion owner.
package input

import (
	"fmt"
	"net/textproto"
	"reflect"
	"sort"
	"strings"

	requestprovider "github.com/viant/bindly/provider/request"
	bindstate "github.com/viant/bindly/state"
)

// Argument is one public MCP argument mapped to its canonical provider source.
type Argument struct {
	PublicName string
	Source     bindstate.Location
	SourceType reflect.Type
}

// Plan is an immutable argument-to-provider projection.
type Plan struct {
	arguments []Argument
}

// Compiler owns the supported canonical source-kind registry.
type Compiler struct {
	registry map[string]bool
}

func NewCompiler() *Compiler {
	result := &Compiler{registry: make(map[string]bool, 6)}
	for _, kind := range []string{
		requestprovider.QueryKind, requestprovider.PathKind, requestprovider.HeaderKind,
		requestprovider.CookieKind, requestprovider.FormKind,
	} {
		result.registry[kind] = true
	}
	result.registry[requestprovider.BodyKind] = true
	return result
}

func (c *Compiler) Compile(arguments []Argument) (*Plan, error) {
	if c == nil {
		return nil, fmt.Errorf("MCP input compiler is required")
	}
	result := &Plan{arguments: make([]Argument, len(arguments))}
	publicNames := make(map[string]bool, len(arguments))
	sources := make(map[string]bool, len(arguments))
	for index, argument := range arguments {
		argument.PublicName = strings.TrimSpace(argument.PublicName)
		argument.Source.Kind = strings.ToLower(strings.TrimSpace(argument.Source.Kind))
		argument.Source.In = normalizeSourceName(argument.Source.Kind, argument.Source.In)
		if argument.PublicName == "" {
			return nil, fmt.Errorf("MCP argument %d public name is required", index+1)
		}
		if publicNames[argument.PublicName] {
			return nil, fmt.Errorf("duplicate MCP argument name %q", argument.PublicName)
		}
		publicNames[argument.PublicName] = true
		if !c.registry[argument.Source.Kind] {
			return nil, fmt.Errorf("MCP argument %q has unsupported binding kind %q", argument.PublicName, argument.Source.Kind)
		}
		if argument.SourceType == nil {
			return nil, fmt.Errorf("MCP argument %q source type is required", argument.PublicName)
		}
		sourceKey := argument.Source.Kind + "\x00" + argument.Source.In
		if sources[sourceKey] {
			return nil, fmt.Errorf("duplicate MCP source %s/%s", argument.Source.Kind, argument.Source.In)
		}
		sources[sourceKey] = true
		result.arguments[index] = argument
	}
	if len(arguments) > 1 {
		hasWhole := false
		hasNamed := false
		for _, argument := range result.arguments {
			if argument.Source.Kind != requestprovider.BodyKind {
				continue
			}
			hasWhole = hasWhole || argument.Source.In == ""
			hasNamed = hasNamed || argument.Source.In != ""
		}
		if hasWhole && hasNamed {
			return nil, fmt.Errorf("MCP body arguments cannot mix whole and named sources")
		}
	}
	sort.SliceStable(result.arguments, func(i, j int) bool {
		return result.arguments[i].PublicName < result.arguments[j].PublicName
	})
	return result, nil
}

func normalizeSourceName(kind, name string) string {
	name = strings.TrimSpace(name)
	if kind == requestprovider.HeaderKind {
		return textproto.CanonicalMIMEHeaderKey(name)
	}
	return name
}

func (p *Plan) Arguments() []Argument {
	if p == nil {
		return nil
	}
	return append([]Argument(nil), p.arguments...)
}
