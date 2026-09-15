package transcribe

import (
	"fmt"
	"strings"

	"github.com/viant/bindly"
	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"github.com/viant/datly/transcribe/handler/compiler"
	handlergo "github.com/viant/datly/transcribe/handler/golang"
	xshape "github.com/viant/x/shape"
)

// entityHookCompilation binds source view authority to final generated entity
// identities. It never instantiates a hook or carries invocation dependencies.
type entityHookCompilation struct {
	generation *handlerGeneration
	generated  *gen.Plan
	views      map[string]*spec.View
	types      map[string]string
}

func (g *handlerGeneration) compileEntityHooks(semantic *plan.Plan, generated *gen.Plan, records []handlergo.RecordType) error {
	if g.compiled == nil || g.compiled.Component == nil {
		return nil
	}
	compilation := &entityHookCompilation{generation: g, generated: generated, views: map[string]*spec.View{}, types: map[string]string{}}
	visited := map[*spec.View]bool{}
	hasHooks := false
	var index func(*spec.View) error
	index = func(view *spec.View) error {
		if view == nil || visited[view] {
			return nil
		}
		visited[view] = true
		identity, err := view.Identity()
		if err != nil {
			return err
		}
		if prior := compilation.views[identity]; prior != nil && prior.EntityHooks != view.EntityHooks {
			return fmt.Errorf("view %s has conflicting entity hook declarations", identity)
		}
		compilation.views[identity] = view
		hasHooks = hasHooks || strings.TrimSpace(view.EntityHooks) != ""
		for _, relation := range view.Relations {
			if relation != nil {
				if err = index(relation.View); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := index(g.compiled.Component.RootView); err != nil {
		return err
	}
	for _, view := range g.compiled.Component.Views {
		if err := index(view); err != nil {
			return err
		}
	}
	if !hasHooks {
		return nil
	}
	for _, record := range records {
		compilation.types[strings.Join(record.Path, ".")] = record.Value
	}
	return compilation.apply(semantic.Root, "")
}

func (c *entityHookCompilation) apply(record *plan.RecordPlan, parent string) error {
	if record == nil {
		return fmt.Errorf("entity hook graph contains a nil record")
	}
	entity, err := c.identity(record)
	if err != nil {
		return err
	}
	if view := c.views[record.Identity]; view != nil && strings.TrimSpace(view.EntityHooks) != "" {
		if record.Auxiliary {
			return fmt.Errorf("auxiliary view %s cannot declare mutation entity hooks", record.Identity)
		}
		if record.Entity == nil {
			return fmt.Errorf("entity hook view %s has no entity metadata", record.Identity)
		}
		request := compiler.EntityHookRequest{Hook: view.EntityHooks, Entity: entity, Parent: parent}
		if parent == "" && c.generated.Input.Type != "" && c.generated.Output.Type != "" {
			request.Input, err = c.generated.CanonicalType(c.generation.input.TargetPackage, c.generated.Input.Type)
			if err != nil {
				return err
			}
			request.Output, err = c.generated.CanonicalType(c.generation.input.TargetPackage, c.generated.Output.Type)
			if err != nil {
				return err
			}
		}
		hook, err := (compiler.EntityHookCompiler{Types: c.generation.input.TypeResolver}).Compile(request)
		if err != nil {
			return fmt.Errorf("view %s: %w", record.Identity, err)
		}
		record.Entity.Hooks = hook
		binding, err := c.bindingRequired(hook)
		if err != nil {
			return err
		}
		record.Entity.HooksBind = binding
	}
	for _, relation := range record.Relations {
		if relation != nil {
			if err = c.apply(relation.Child, entity); err != nil {
				return err
			}
		}
	}
	return nil
}

// bindingRequired enriches the compiled hook with canonical Bindly field
// metadata at the orchestration boundary. Target-neutral compilation does not
// own binding. Named scalar/slice hooks have no bindable fields.
func (c *entityHookCompilation) bindingRequired(hook spec.TypeRef) (bool, error) {
	if c == nil || c.generation == nil || c.generation.input == nil || c.generation.input.TypeResolver == nil {
		return false, fmt.Errorf("entity hook binding requires canonical type authority")
	}
	types := c.generation.input.TypeResolver
	expression := hook.Name
	if hook.Package != "" {
		expression = hook.Package + "." + expression
	}
	resolved, err := types.ResolveShape(expression)
	if err != nil {
		return false, err
	}
	if resolved == nil || resolved.Descriptor == nil {
		return false, fmt.Errorf("entity hook %s was not resolved", expression)
	}
	shape := xshape.New(resolved.Descriptor, types.Descriptor)
	structured, err := shape.IsStruct()
	if err != nil {
		return false, err
	}
	if !structured {
		return false, nil
	}
	fields, err := shape.Fields()
	if err != nil {
		return false, err
	}
	required := false
	for _, field := range fields {
		if !field.Exported {
			continue
		}
		_, found, err := bindly.BindingSpecFromField(field.StructField())
		if err != nil {
			return false, err
		}
		required = required || found
	}
	return required, nil
}

func (c *entityHookCompilation) identity(record *plan.RecordPlan) (string, error) {
	expression := c.types[strings.Join(record.InputPath, ".")]
	base, err := c.generation.recordBase(expression, record.Cardinality)
	if err != nil {
		return "", err
	}
	if view := c.generated.ViewByType(base); view != nil && view.Ownership == gen.ViewGenerated {
		packagePath := view.Package
		if packagePath == "" {
			packagePath = c.generation.input.TargetPackage
		}
		if packagePath == "" {
			return "", fmt.Errorf("generated entity %s requires target package identity", base)
		}
		return (xshape.Resolver{Package: packagePath}).Canonical(base)
	}
	if c.generation.input.TypeResolver == nil {
		return "", fmt.Errorf("linked entity %s requires canonical type authority", base)
	}
	resolved, err := c.generation.input.TypeResolver.ResolveShape(base)
	if err != nil {
		return "", err
	}
	if resolved == nil || resolved.Descriptor == nil {
		return "", fmt.Errorf("linked entity type %s was not found", base)
	}
	return resolved.Identity, nil
}
