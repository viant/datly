package runtime

import (
	"fmt"
	"sync"

	"github.com/viant/datly/exec"

	"github.com/viant/datly/runtime/output"
	"github.com/viant/x/shape"
)

// prepareOutputDiscovery captures only generation-owned registration metadata.
// Every presentation is compiled through the same native output owners as MCP.
func prepareOutputDiscovery(entry *RegisteredComponent) func(*shape.Representation) (*exec.OutputDiscoverySchemas, error) {
	formats := map[string]bool{}
	if entry.Component != nil {
		for _, route := range entry.Component.Routes {
			for _, exposure := range route.MCP {
				if exposure != nil {
					formats[exposure.EffectiveCaseFormat()] = true
				}
			}
		}
	}
	var baseOnce sync.Once
	var base *output.Plan
	var baseErr error
	return func(metadata *shape.Representation) (*exec.OutputDiscoverySchemas, error) {
		baseOnce.Do(func() {
			base = entry.Output
			if base == nil {
				base, baseErr = (output.Compiler{}).Compile(output.CompileInput{Component: entry.Component, Type: entry.OutputType})
			}
		})
		if baseErr != nil {
			return nil, baseErr
		}
		selected, err := base.WithContract(entry.OutputType, metadata)
		if err != nil {
			return nil, err
		}
		result := &exec.OutputDiscoverySchemas{Schemas: make(map[string]map[string]any, len(formats)), Errors: make(map[string]error)}
		for format := range formats {
			plan, err := selected.WithCaseFormat(format)
			if err == nil {
				plan, err = plan.WithMCPVisibility()
			}
			if err != nil {
				result.Errors[format] = fmt.Errorf("prepare output discovery %s: %w", format, err)
				continue
			}
			document, err := plan.JSONSchema()
			if err != nil {
				result.Errors[format] = fmt.Errorf("prepare output discovery %s: %w", format, err)
				continue
			}
			result.Schemas[format] = document
		}
		return result, nil
	}
}
