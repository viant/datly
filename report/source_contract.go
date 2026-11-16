package report

import (
	"context"
	"fmt"
	bindinput "github.com/viant/bindly/input"
	"github.com/viant/bindly/locator"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	xshape "github.com/viant/x/shape"
	xcontract "github.com/viant/xdatly/contract"
	xmcp "github.com/viant/xdatly/handler/mcp"
	"reflect"
	"strings"
	"sync"
)

func (h *Handler) SourceContractTarget() exec.ComponentTarget { return h.plan.target }
func (h *Handler) PrepareSourceContract(component *spec.Component, inputType, outputType reflect.Type) (func(*exec.EffectiveContract) (*exec.ContractProjection, error), error) {
	index, err := compileSourceContractIndex(component, outputType)
	if err != nil {
		return nil, err
	}
	paths := map[string]string{}
	for _, items := range [][]selection{h.plan.dimensions, h.plan.measures} {
		for _, item := range items {
			if _, ok := index.fields[item.name]; !ok {
				return nil, fmt.Errorf("report selection %s has no source output provenance", item.name)
			}
			path, err := definitionPath(h.plan.inputType, item.index)
			if err != nil {
				return nil, err
			}
			paths[path] = item.name
		}
	}
	filterPaths, err := sourceFilterPaths(component, inputType, h.plan.filters, h.plan.inputType)
	if err != nil {
		return nil, err
	}
	walker, err := xshape.CompileRewrite(h.plan.inputType)
	if err != nil {
		return nil, err
	}
	return func(source *exec.EffectiveContract) (*exec.ContractProjection, error) {
		var once sync.Once
		var snapshot *sourceContractVisibility
		var selectionErr error
		prepare := func() (any, error) {
			once.Do(func() { snapshot, selectionErr = index.prepareVisibility(source) })
			return snapshot, selectionErr
		}
		policy := &xcontract.Policy{Field: func(path string, parent reflect.Type, field *xcontract.Field) (*xcontract.Field, error) {
			fullPath := joinContractPath(path, field.Name)
			if index, found := filterPaths[fullPath]; found {
				visible, err := contractInputFieldVisible(source.Input, source.InputRepresentation, index)
				if err != nil {
					return nil, err
				}
				if !visible {
					return field.SetTag("mcp", "-"), nil
				}
				linked, ok := parent.FieldByName(field.Name)
				if !ok {
					return nil, fmt.Errorf("report filter has no canonical field")
				}
				return projectFilterEnum(source, index, linked.Type, field)
			}
			sourceName, ok := paths[joinContractPath(path, field.Name)]
			if !ok {
				return nil, nil
			}
			_, selectionErr = prepare()
			if selectionErr != nil {
				return nil, selectionErr
			}
			if !snapshot.fields[sourceName] {
				return field.SetTag("mcp", "-"), nil
			}
			return nil, nil
		}}
		return &exec.ContractProjection{Policy: policy, Prepare: func(selected *xshape.Representation) (any, error) {
			_, err := prepare()
			if err != nil {
				return nil, err
			}
			snapshot.normalization, err = selectorRecognition(walker, selected, paths, snapshot.fields)
			return snapshot, err
		}}, nil
	}, nil
}
func joinContractPath(path, name string) string {
	if path == "" {
		return name
	}
	return path + "." + name
}

func (h *composeHandler) SourceContractTarget() exec.ComponentTarget { return h.source.target }
func (h *composeHandler) PrepareSourceContract(component *spec.Component, inputType, outputType reflect.Type) (func(*exec.EffectiveContract) (*exec.ContractProjection, error), error) {
	index, err := compileSourceContractIndex(component, outputType)
	if err != nil {
		return nil, err
	}
	filterPaths, err := sourceFilterPaths(component, inputType, h.source.filters, h.source.inputType)
	if err != nil {
		return nil, err
	}
	framePaths := map[string][]int{}
	for path, index := range filterPaths {
		_, leaf, ok := strings.Cut(path, ".")
		if !ok {
			return nil, fmt.Errorf("compose source filter path is invalid")
		}
		framePaths["Cubes[].Filters."+leaf] = index
	}
	return func(source *exec.EffectiveContract) (*exec.ContractProjection, error) {
		policy := &xcontract.Policy{Field: func(path string, parent reflect.Type, field *xcontract.Field) (*xcontract.Field, error) {
			if index, ok := framePaths[joinContractPath(path, field.Name)]; ok {
				visible, err := contractInputFieldVisible(source.Input, source.InputRepresentation, index)
				if err != nil {
					return nil, err
				}
				if !visible {
					return field.SetTag("mcp", "-"), nil
				}
				linked, ok := parent.FieldByName(field.Name)
				if !ok {
					return nil, fmt.Errorf("report filter has no canonical field")
				}
				return projectFilterEnum(source, index, linked.Type, field)
			}
			return nil, nil
		}}
		return &exec.ContractProjection{Policy: policy, Prepare: func(*xshape.Representation) (any, error) { return index.prepareVisibility(source) }}, nil
	}, nil
}

type reportContractScope []locator.Provider

func (s reportContractScope) Providers() []locator.Provider {
	return append([]locator.Provider(nil), s...)
}

func admitSourceContract(ctx context.Context, invoker exec.ComponentInvoker, target exec.ComponentTarget, providers []locator.Provider, fields []string) error {
	if _, isMCP := xmcp.LookupContext(ctx); !isMCP {
		return nil
	}
	resolver, ok := invoker.(exec.ContractResolver)
	if !ok {
		return fmt.Errorf("report source contract resolver is unavailable")
	}
	selected, err := resolver.ResolveContract(ctx, target, reportContractScope(providers))
	if err != nil {
		return err
	}
	snapshot, ok := selected.Projection.(*sourceContractVisibility)
	if !ok || snapshot == nil {
		if !selected.Dynamic {
			return nil
		} // unchanged no-Contract public default
		return fmt.Errorf("report source visibility preparation is unavailable")
	}
	for _, field := range fields {
		if !snapshot.fields[field] && !snapshot.relations[field] {
			return &bindinput.Error{Cause: fmt.Errorf("report source field is not available")}
		}
	}
	return nil
}
