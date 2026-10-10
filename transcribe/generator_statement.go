package transcribe

import (
	"context"
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	plan "github.com/viant/datly/transcribe/handler/ast"
	handlercompiler "github.com/viant/datly/transcribe/handler/compiler"
	handlergo "github.com/viant/datly/transcribe/handler/golang"
	"github.com/viant/datly/typecatalog"
)

func (g Generator) generateStatement(ctx context.Context, root, dir string, compiled *Result) (*GeneratedPackage, error) {
	// The CLI operation selects the transport generation path. This product
	// supplies its own typed contract handler, so it must not advertise the
	// automatic record writer to standalone materialization.
	compiled.Component.Settings.Mutation = ""
	// An explicit statement is the sole persistence owner. Record policies must
	// not silently add sequencing, classification or reconciliation beside it.
	if settings := compiled.Component.Settings; settings != nil && settings.SequenceStrategy != "" {
		return nil, fmt.Errorf("buffered result statement cannot select an allocator strategy")
	}
	views := append([]*spec.View{compiled.Component.RootView}, compiled.Component.Views...)
	for _, view := range views {
		if view == nil {
			continue
		}
		if view.Reconciliation != nil || view.WriterIdentityPolicy != "" || view.QueueContract != "" || view.WriterActionPolicy != "" || view.EntityHooks != "" {
			return nil, fmt.Errorf("buffered result statement is incompatible with record identity, sequencing, queue and reconciliation policies")
		}
		for _, column := range view.Columns {
			if column != nil && (column.DeleteMarker || column.ConcurrencyToken || strings.Contains(column.Tag, `sequence:`)) {
				return nil, fmt.Errorf("buffered result statement cannot select record sequence or mutation token policies")
			}
		}
	}
	input, dir, err := generationInput(root, dir, compiled)
	if err != nil {
		return nil, err
	}
	contracts, err := gen.New(input).Plan()
	if err != nil {
		return nil, err
	}
	authority := map[string]plan.StatementSelector{}
	for _, contract := range []struct {
		root  string
		value gen.ContractPlan
	}{{"Input", contracts.Input}, {"Output", contracts.Output}} {
		for _, field := range contract.value.Fields {
			if field.Anonymous || field.Implementation {
				continue
			}
			authority[contract.root+"."+field.Name] = plan.StatementSelector{Path: plan.FieldPath{contract.root, field.Name}, Type: spec.TypeRef{Name: field.Type}, Addressable: true}
		}
	}
	semantic, err := (&handlercompiler.Compiler{}).CompileStatement(compiled.StatementProgram, authority)
	if err != nil {
		return nil, err
	}
	factory := "New" + typecatalog.FieldName(compiled.Component.Name) + "Handler"
	if strings.TrimSpace(compiled.Component.Name) == "" {
		return nil, fmt.Errorf("buffered result statement component name is required")
	}
	file, err := handlergo.StatementContract(semantic, handlergo.Config{Package: contracts.PackageName(), Factory: factory, InputType: contracts.Input.Type, OutputType: contracts.Output.Type, Imports: contracts.Imports})
	if err != nil {
		return nil, err
	}
	input.ContractHandler = &gen.ContractHandlerAsset{Factory: factory, File: file}
	return NewCompiler().generateInputAt(ctx, root, dir, compiled, input)
}
