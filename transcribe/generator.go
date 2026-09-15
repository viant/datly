package transcribe

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	gen "github.com/viant/datly/transcribe/generate"
	handlercompiler "github.com/viant/datly/transcribe/handler/compiler"
	"github.com/viant/datly/typecatalog"
)

// Generator derives request contracts and mutation state before lowering a
// canonical handler. Transcribe remains the lower-level authored-contract path.
// Destination is only the project root; DQL and package metadata own artifacts.
type Generator struct {
	Operation string
	Language  HandlerTarget
}

type GenerationRequest struct {
	Source *Source
	// Compiled accepts the canonical discovery product, including package authority.
	Compiled    *Result
	Destination string
}

func (g Generator) Generate(ctx context.Context, request GenerationRequest) (*GeneratedPackage, error) {
	if (request.Source == nil) == (request.Compiled == nil) {
		return nil, fmt.Errorf("transcribe requires exactly one authored source or compiled component")
	}
	compiled := request.Compiled
	var err error
	if compiled == nil {
		source := *request.Source
		if source.Types == nil {
			source.Types = typecatalog.NewCatalog()
		} else {
			source.Types, err = source.Types.Clone()
			if err != nil {
				return nil, err
			}
		}
		compiled, err = NewCompiler().Compile(ctx, &source)
		if err != nil {
			return nil, err
		}
	} else {
		compiled, err = compiled.projectClone()
		if err != nil {
			return nil, err
		}
	}

	key, err := compiled.projectKey()
	if err != nil {
		return nil, err
	}
	// PackagePath is canonical #package destination authority. Source/import
	// packages and DefaultPackage are type lookup context, not destinations.
	linkedGo := compiled.Source.PackageComponent != nil && strings.TrimSpace(compiled.Source.Text) == ""
	fallback := ""
	if linkedGo {
		fallback = filepath.Join("generated", projectComponentSlug(key))
	} else if compiled.Component.TypeContext == nil || strings.TrimSpace(compiled.Component.TypeContext.PackagePath) == "" {
		return nil, fmt.Errorf("transcribe requires an explicit #package('path/to/package') destination in DQL")
	}
	return g.generate(ctx, request.Destination, fallback, compiled)
}

func (g Generator) generate(ctx context.Context, root, dir string, compiled *Result) (*GeneratedPackage, error) {
	operation := strings.ToLower(strings.TrimSpace(g.Operation))
	language := g.Language
	if language == "" {
		language = HandlerGo
	}
	if operation != "get" && operation != "patch" && operation != "post" && operation != "put" {
		return nil, fmt.Errorf("transcribe operation must be get, patch, post or put")
	}
	if language != HandlerGo && language != HandlerVelty {
		return nil, fmt.Errorf("unsupported transcribe language %q", language)
	}
	for _, route := range compiled.Component.Routes {
		if route != nil && route.Method != "" && !strings.EqualFold(route.Method, operation) {
			return nil, fmt.Errorf("transcribe operation %q conflicts with authored route method %q", operation, route.Method)
		}
	}
	inputTarget := gen.Input{Component: compiled.Component}
	if err := inputTarget.ValidateLifecycleTarget(operation != "get" && language == HandlerGo); err != nil {
		return nil, err
	}
	if operation == "get" {
		return NewCompiler().generateCompiledAt(ctx, root, dir, compiled)
	}
	input, dir, err := generationInput(root, dir, compiled)
	if err != nil {
		return nil, err
	}
	initial, err := gen.New(input).Plan()
	if err != nil {
		return nil, err
	}
	derived, err := (&handlercompiler.Compiler{}).BuildInput(handlercompiler.Request{Component: input.Component, ViewBindings: input.ViewBindings, Operation: WriteOperation(operation)}, initial.RootViewType)
	if err != nil {
		return nil, err
	}
	copy := *compiled
	copy.Component = derived.Component
	copy.ViewBindings = derived.ViewBindings
	declarations, err := newDeclarationCompiler(copy.Component, nil).compile()
	if err != nil {
		return nil, err
	}
	copy.Declarations = declarations.generation
	input, dir, err = generationInput(root, dir, &copy)
	if err != nil {
		return nil, err
	}
	options := Options{Contracts: ContractsAuto, Handler: HandlerOptions{Target: language, Operation: derived.Operation, Input: derived.Input, Output: derived.Output, Currents: derived.Currents}}
	if language == HandlerGo {
		options.Handler.Go.Execution = GoExecutionMutation
		options.Handler.Hooks.Scaffold = true
	}
	handlers := newHandlerGeneration(&copy, &input, options)
	handlers.directory = filepath.Join(root, dir)
	contractPlan, err := gen.New(input).Plan()
	if err != nil {
		return nil, err
	}
	semantic, err := handlers.compilePlan()
	if err != nil {
		return nil, handlers.diagnostic(err)
	}
	if err = handlers.validateContractAssignment(semantic, contractPlan); err != nil {
		return nil, handlers.diagnostic(err)
	}
	if err = handlers.prepare(); err != nil {
		return nil, handlers.diagnostic(err)
	}
	return NewCompiler().generateInputAt(ctx, root, dir, &copy, input)
}
