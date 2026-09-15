package transcribe

import (
	"fmt"
	"strings"

	gen "github.com/viant/datly/transcribe/generate"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"github.com/viant/datly/transcribe/handler/compiler"
	handlergo "github.com/viant/datly/transcribe/handler/golang"
	xshape "github.com/viant/x/shape"
)

func (g *handlerGeneration) prepareMutationScaffold(semantic *plan.Plan, config handlergo.Config) (*plan.Plan, error) {
	proposal, err := handlergo.ScaffoldMutationHooks(semantic, config)
	if err != nil {
		return nil, err
	}
	if proposal.File == nil {
		return proposal.Plan, nil
	}
	if g.directory == "" {
		return nil, fmt.Errorf("mutation hook scaffolding requires the generated package destination")
	}
	asset := &gen.HookScaffoldAsset{Destination: g.options.Handler.Hooks.Destination, File: proposal.File, PackagePath: config.PackagePath, Catalog: g.compiled.Source.Types}
	asset.Destination, err = asset.Filename()
	if err != nil {
		return nil, err
	}
	imports := map[string]string{}
	for _, item := range config.Imports {
		imports[item.Alias] = item.Package
	}
	resolver := xshape.Resolver{Package: config.PackagePath, Imports: imports}
	var contracts []compiler.EntityHookRequest
	for _, binding := range proposal.Bindings {
		request := compiler.EntityHookRequest{Hook: binding.Hook, Entity: binding.Entity, Parent: binding.Parent}
		if binding.Root {
			request.Input, err = resolver.Canonical(config.InputType)
			if err != nil {
				return nil, err
			}
			request.Output, err = resolver.Canonical(config.OutputType)
			if err != nil {
				return nil, err
			}
		}
		contracts = append(contracts, request)
		asset.EntityHooks = append(asset.EntityHooks, gen.HookScaffoldContract{Type: binding.Hook, Identity: binding.Identity, Path: append([]string(nil), binding.Path...), Required: []string{"Init", "Validate"}})
	}
	// Approve and retain the complete proposal's native signatures independently
	// of the mutable artifact. Existing user source may omit optional methods.
	proposedTypes, err := asset.ResolveEntityHookTypes("")
	if err != nil {
		return nil, err
	}
	evidence := map[string][]xshape.Method{}
	for _, contract := range contracts {
		if _, err := (compiler.EntityHookCompiler{Types: proposedTypes}).Compile(contract); err != nil {
			return nil, err
		}
		descriptor, err := proposedTypes.Descriptor(contract.Hook)
		if err != nil {
			return nil, err
		}
		methods, err := xshape.New(descriptor, proposedTypes.Descriptor).Methods(true)
		if err != nil {
			return nil, err
		}
		evidence[contract.Hook] = methods
	}
	if err = asset.RetainEntityHookEvidence(evidence); err != nil {
		return nil, err
	}
	types, err := asset.ResolveEntityHookTypes(g.directory)
	if err != nil {
		return nil, fmt.Errorf("mutation hook scaffold: %w", err)
	}
	for _, contract := range contracts {
		if _, err := (compiler.EntityHookCompiler{Types: types}).Compile(contract); err != nil {
			return nil, err
		}
	}
	input := *g.input
	input.TypeResolver = types
	generation := *g
	generation.input = &input
	compilation := &entityHookCompilation{generation: &generation}
	type role struct{ identity, path string }
	bindings := map[role]bool{}
	for _, binding := range proposal.Bindings {
		bindings[role{binding.Identity, strings.Join(binding.Path, "\x00")}] = true
	}
	var apply func(*plan.RecordPlan) error
	apply = func(record *plan.RecordPlan) error {
		if record == nil {
			return nil
		}
		if bindings[role{record.Identity, strings.Join(record.InputPath, "\x00")}] {
			record.Entity.HooksBind, err = compilation.bindingRequired(record.Entity.Hooks)
			if err != nil {
				return err
			}
		}
		for _, relation := range record.Relations {
			if relation != nil {
				if err := apply(relation.Child); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err = apply(proposal.Plan.Root); err != nil {
		return nil, err
	}
	g.input.HookScaffold = asset
	return proposal.Plan, nil
}
