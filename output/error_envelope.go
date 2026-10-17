package output

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/internal/readfailure"
	"github.com/viant/datly/spec"
	"github.com/viant/x/shape"
	legacyresponse "github.com/viant/xdatly/handler/response"
)

// errorEnvelope retains only immutable, checked output-member authority.
type errorEnvelope struct {
	root       reflect.Type
	status     *shape.Accessor
	message    *shape.Accessor
	errors     *shape.Accessor
	readerView string
	data       *shape.Accessor
	dataType   reflect.Type
}

func compileErrorEnvelope(input CompileInput) (*errorEnvelope, error) {
	if input.Component == nil || input.Type == nil {
		return nil, nil
	}
	root := input.Type
	if root.Kind() == reflect.Pointer {
		root = root.Elem()
	}
	if root.Kind() != reflect.Struct {
		return nil, nil
	}
	owner := shape.Linked(root)
	var declared []*spec.Parameter
	var legacy *spec.Parameter
	for _, parameter := range spec.EffectiveParameters(input.Component.Parameters) {
		if parameter == nil || !(parameter.EmitOutput || strings.EqualFold(strings.TrimSpace(parameter.Source.Kind), "output")) ||
			!strings.EqualFold(strings.TrimSpace(parameter.Source.Name), "status") {
			continue
		}
		declared = append(declared, parameter)
		field, err := owner.StructField(strings.TrimSpace(parameter.Name))
		if err != nil {
			continue
		}
		if field.Type == reflect.TypeFor[legacyresponse.Status]() || field.Type == reflect.TypeFor[*legacyresponse.Status]() {
			legacy = parameter
		}
	}
	if legacy == nil {
		return nil, nil
	}
	if len(declared) != 1 {
		return nil, fmt.Errorf("legacy error output requires one unambiguous declared status slot")
	}
	name := strings.TrimSpace(legacy.Name)
	status, err := owner.Accessor(name + ".Status")
	if err != nil {
		return nil, fmt.Errorf("compile legacy error status %s: %w", name, err)
	}
	message, err := owner.Accessor(name + ".Message")
	if err != nil {
		return nil, fmt.Errorf("compile legacy error message %s: %w", name, err)
	}
	errors, err := owner.Accessor(name + ".Errors")
	if err != nil {
		return nil, fmt.Errorf("compile legacy error details %s: %w", name, err)
	}
	result := &errorEnvelope{root: input.Type, status: status, message: message, errors: errors, readerView: errorReaderView(input.Component)}
	result.data, result.dataType = compileErrorData(input, root, owner, result.readerView)
	// Check writable authority once; this value is discarded, never retained.
	if _, err = result.fresh("", []*legacyresponse.Error{{}}); err != nil {
		return nil, fmt.Errorf("compile legacy error output: %w", err)
	}
	return result, nil
}

// errorReaderView caches only declared executable read metadata. A runtime
// handler attached without a Handler declaration cannot be distinguished here.
func errorReaderView(component *spec.Component) string {
	if component == nil || component.RootView == nil || (component.Settings != nil && component.Settings.Mutation != "") {
		return ""
	}
	root := component.RootView
	if strings.TrimSpace(root.Name) == "" || root.InMemory || root.Auxiliary {
		return ""
	}
	source := root.RuntimeSource()
	if source == nil || (strings.TrimSpace(source.SQL) == "" && strings.TrimSpace(source.URI) == "" && strings.TrimSpace(source.Table) == "") {
		return ""
	}
	for _, route := range component.Routes {
		if route != nil && route.Handler != "" {
			return ""
		}
	}
	return root.Name
}

// compileErrorData retains only one direct, declared executable reader slot.
// Allocation always uses its checked linked type, never a parsed approximation.
func compileErrorData(input CompileInput, root reflect.Type, owner *shape.Type, readerView string) (*shape.Accessor, reflect.Type) {
	if readerView == "" ||
		(&Plan{typeOf: input.Type}).JSONOnly() ||
		(input.Component.Settings != nil && strings.TrimSpace(input.Component.Settings.JSONMarshalType) != "") {
		return nil, nil
	}
	cardinality, err := spec.NormalizeCardinality(input.Component.RootView.Cardinality)
	if err != nil || cardinality == spec.CardinalityOne {
		return nil, nil
	}
	var declared *spec.Parameter
	for _, parameter := range spec.EffectiveParameters(input.Component.Parameters) {
		if parameter == nil || !(parameter.EmitOutput || strings.EqualFold(strings.TrimSpace(parameter.Source.Kind), "output")) ||
			!strings.EqualFold(strings.TrimSpace(parameter.Source.Name), "view") {
			continue
		}
		if declared != nil {
			return nil, nil
		}
		declared = parameter
	}
	if declared == nil {
		return nil, nil
	}
	name := strings.TrimSpace(declared.Name)
	if name == "" || (input.DataField != "" && input.DataField != name) {
		return nil, nil
	}
	field, ok := root.FieldByName(name)
	if !ok || field.Name != name || len(field.Index) != 1 || field.PkgPath != "" || field.Type.Kind() != reflect.Slice ||
		field.Type.Elem().Kind() == reflect.Uint8 || (&Plan{typeOf: field.Type}).JSONOnly() {
		return nil, nil
	}
	if declared.Cardinality != "" && !strings.EqualFold(strings.TrimSpace(declared.Cardinality), "many") {
		return nil, nil
	}
	for _, expression := range []string{declared.TypeExpr, declared.OutputTypeExpr} {
		expression = strings.TrimSpace(expression)
		if expression == "" {
			continue
		}
		reference, err := (shape.Resolver{}).Reference(expression)
		if err != nil {
			return nil, nil
		}
		actual := field.Type
		for _, wrapper := range reference.Wrappers {
			// A declared structural wrapper cannot erase a named linked container.
			if actual.Name() != "" {
				return nil, nil
			}
			switch wrapper.Kind {
			case shape.WrapperSlice:
				if actual.Kind() != reflect.Slice {
					return nil, nil
				}
			case shape.WrapperPointer:
				if actual.Kind() != reflect.Pointer {
					return nil, nil
				}
			default:
				return nil, nil
			}
			actual = actual.Elem()
		}
		packages := shape.Packages{Default: root.PkgPath(), Imports: map[string]string{}}
		if context := input.Component.TypeContext; context != nil {
			if context.PackagePath != "" {
				packages.Default = context.PackagePath
			}
			for _, item := range context.Imports {
				packages.Imports[item.Alias] = item.Package
			}
		}
		// Unqualified builtins have no package. Named application types continue to
		// require the declared/root package identity, including named slices.
		if reference.Qualifier == "" && actual.PkgPath() == "" {
			packages.Default = ""
		}
		if err := (shape.Contract{Packages: packages}).ValidateLinked(expression, field.Type); err != nil {
			return nil, nil
		}
	}
	accessor, err := owner.Accessor(name)
	if err != nil {
		return nil, nil
	}
	// Retain the actual exact slice type only after the checked writable proof.
	probe := reflect.New(root)
	if err := accessor.Set(probe.Interface(), reflect.MakeSlice(field.Type, 0, 0).Interface()); err != nil {
		return nil, nil
	}
	return accessor, field.Type
}

func (e *errorEnvelope) fresh(message string, details any) (any, error) {
	root := e.root
	if root.Kind() == reflect.Pointer {
		root = root.Elem()
	}
	value := reflect.New(root)
	if e.data != nil {
		if err := e.data.Set(value.Interface(), reflect.MakeSlice(e.dataType, 0, 0).Interface()); err != nil {
			return nil, err
		}
	}
	if err := e.status.Set(value.Interface(), "error"); err != nil {
		return nil, err
	}
	if err := e.message.Set(value.Interface(), message); err != nil {
		return nil, err
	}
	if err := e.errors.Set(value.Interface(), details); err != nil {
		return nil, err
	}
	if e.root.Kind() == reflect.Pointer {
		return value.Convert(e.root).Interface(), nil
	}
	return value.Elem().Interface(), nil
}

// ErrorEnvelope creates a fresh declared legacy output from already projected
// public text. False means this output has no eligible legacy status authority.
func (p *Plan) ErrorEnvelope(message string) (any, bool, error) {
	if p == nil || p.errorEnvelope == nil {
		return nil, false, nil
	}
	value, err := p.errorEnvelope.fresh(message, nil)
	return value, true, err
}

// ErrorEnvelopeWithCause adds source-owned binding details to the existing
// declared legacy output. message and statusCode are already selected public
// values; unsupported cause graphs retain the plain ErrorEnvelope behavior.
func (p *Plan) ErrorEnvelopeWithCause(message string, statusCode int, cause error) (any, bool, error) {
	if p == nil || p.errorEnvelope == nil {
		return nil, false, nil
	}
	// Original reader finalization places a direct public business message in
	// the declared Status.Errors string array. This grants no binding ownership
	// and deliberately does not inspect private causes or structured objects.
	if direct, ok := cause.(*legacyresponse.Error); ok && direct != nil &&
		direct.Code >= 400 && direct.Code < 500 && direct.Code == statusCode && direct.Message != "" {
		value, err := p.errorEnvelope.fresh(direct.Message, []string{direct.Message})
		return value, true, err
	}
	readerView := p.errorEnvelope.readerView
	if readerView == "" {
		readerView = sourceReaderView(cause)
	}
	if diagnostic, ok := readfailure.Message(cause, statusCode, readerView); ok {
		value, err := p.errorEnvelope.fresh(diagnostic, []string{diagnostic})
		return value, true, err
	}
	details, projectedMessage, ok := projectBindingErrors(cause, statusCode, readerView)
	if !ok {
		return p.ErrorEnvelope(message)
	}
	if projectedMessage != "" {
		message = projectedMessage
	}
	if view := readerView; view != "" {
		for _, detail := range details {
			if detail != nil && detail.View == "" {
				detail.View = view
			}
		}
	}
	value, err := p.errorEnvelope.fresh(message, detailsValue(details))
	return value, true, err
}
func detailsValue(details []*legacyresponse.Error) any {
	if details == nil {
		return nil
	}
	return details
}
