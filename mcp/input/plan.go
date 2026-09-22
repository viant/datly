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
	Aliases    []string
	Source     bindstate.Location
	SourceType reflect.Type
}

// Plan is an immutable argument-to-provider projection.
type Plan struct {
	arguments []Argument
	aliases   map[string]string
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
		argument.Aliases = normalizeAliases(argument.Aliases)
		argument.Source.Kind = strings.ToLower(strings.TrimSpace(argument.Source.Kind))
		argument.Source.In = normalizeSourceName(argument.Source.Kind, argument.Source.In)
		if argument.PublicName == "" {
			return nil, fmt.Errorf("MCP argument %d public name is required", index+1)
		}
		if publicNames[argument.PublicName] {
			return nil, fmt.Errorf("duplicate MCP argument name %q", argument.PublicName)
		}
		publicNames[argument.PublicName] = true
		for _, alias := range argument.Aliases {
			if publicNames[alias] {
				return nil, fmt.Errorf("duplicate MCP argument name %q", alias)
			}
			publicNames[alias] = true
			if result.aliases == nil {
				result.aliases = map[string]string{}
			}
			result.aliases[alias] = argument.PublicName
		}
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
		result.arguments[index] = cloneArgument(argument)
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
	result := make([]Argument, len(p.arguments))
	for i, argument := range p.arguments {
		result[i] = cloneArgument(argument)
	}
	return result
}

// NormalizeArguments projects tool argument aliases to canonical public names
// without mutating the caller-owned map.
func (p *Plan) NormalizeArguments(arguments map[string]interface{}) (map[string]interface{}, error) {
	if p == nil {
		return nil, fmt.Errorf("MCP input plan is required")
	}
	if len(arguments) == 0 {
		return nil, nil
	}
	known := make(map[string]bool, len(p.arguments))
	for _, argument := range p.arguments {
		known[argument.PublicName] = true
	}
	result := make(map[string]interface{}, len(arguments))
	for name, value := range arguments {
		canonical := name
		if mapped := p.aliases[name]; mapped != "" {
			canonical = mapped
		} else if !known[name] {
			return nil, fmt.Errorf("unknown MCP argument %q", name)
		}
		if _, exists := result[canonical]; exists {
			return nil, fmt.Errorf("conflicting MCP argument %q", canonical)
		}
		result[canonical] = value
	}
	return result, nil
}

func cloneArgument(argument Argument) Argument {
	argument.Aliases = append([]string(nil), argument.Aliases...)
	return argument
}

func normalizeAliases(aliases []string) []string {
	if len(aliases) == 0 {
		return nil
	}
	seen := map[string]bool{}
	result := make([]string, 0, len(aliases))
	for _, alias := range aliases {
		alias = strings.TrimSpace(alias)
		if alias == "" || seen[alias] {
			continue
		}
		seen[alias] = true
		result = append(result, alias)
	}
	return result
}
