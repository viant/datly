package output

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	xshape "github.com/viant/x/shape"
)

// WithMCPOutputRoot selects only the demonstrated converted reader shape: a
// hidden declared SQL view and one distinct, declared public body collection.
// The SQL view remains owned by the reader; only this transport's rows change.
func (p *Plan) WithMCPOutputRoot() (*Plan, error) {
	if p == nil || p.compileInput.publicOutputRoot {
		return p, nil
	}
	input := p.compileInput
	c := input.Component
	if errorReaderView(c) == "" {
		return p, nil
	}
	// This extension owns collection carriers only. Native singleton carriers
	// retain their existing plan and selector behavior, including converted bodies.
	if p.rows == nil || (p.rows.sourceType.Kind() != reflect.Slice && p.rows.sourceType.Kind() != reflect.Array) {
		return p, nil
	}
	// Canonical Parameters include the native companion declarations. View
	// projection permission alone does not declare a public Fields input.
	hasRootFields := false
	for _, parameter := range spec.EffectiveParameters(c.Parameters) {
		if parameter == nil || parameter.QuerySelector == nil || parameter.QuerySelector.Property != spec.SelectorPropertyFields {
			continue
		}
		target := strings.TrimSpace(parameter.QuerySelector.View)
		if target == "" || strings.EqualFold(target, c.RootView.CanonicalName()) {
			hasRootFields = true
			break
		}
	}
	if !hasRootFields {
		return p, nil
	}
	var views, bodies []*spec.Parameter
	for _, parameter := range spec.EffectiveParameters(c.Parameters) {
		if parameter == nil || !strings.EqualFold(strings.TrimSpace(parameter.Source.Kind), "output") {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(parameter.Source.Name)) {
		case "view":
			views = append(views, parameter)
		case "body":
			bodies = append(bodies, parameter)
		}
	}
	if len(bodies) == 0 || len(views) == 0 {
		return p, nil
	}
	root := (xshape.Runtime{}).Indirect(p.typeOf)
	// The existing compiled row-holder authority identifies the root. Independent
	// output/view declarations cannot become a converted destination by position.
	if p.rows == nil || p.rows.name == "" {
		return p, nil
	}
	var view reflect.StructField
	var declaredView *spec.Parameter
	for _, parameter := range views {
		field, ok := typecatalog.FieldByName(root, parameter.Name)
		if ok && reflect.DeepEqual(field.Index, p.rows.index) {
			view, declaredView = field, parameter
			break
		}
	}
	if declaredView == nil {
		return p, nil
	}
	// Ordinary visible roots retain their existing multi-output contract.
	if jsonName, _, _ := strings.Cut(view.Tag.Get("json"), ","); jsonName != "-" && view.Tag.Get("internal") != "true" {
		return p, nil
	}
	if len(views) != 1 || len(bodies) != 1 {
		return nil, fmt.Errorf("MCP converted output requires one declared view and body")
	}
	body, ok := typecatalog.FieldByName(root, bodies[0].Name)
	if !ok {
		return nil, fmt.Errorf("MCP declared output body %q is missing", bodies[0].Name)
	}
	if view.Name == body.Name {
		return p, nil
	}
	if len(view.Index) != 1 || len(body.Index) != 1 || !body.IsExported() || body.Type.Kind() != reflect.Slice || body.Type.Elem().Kind() == reflect.Uint8 || c.RootView.Cardinality == spec.CardinalityOne {
		return nil, fmt.Errorf("MCP converted output requires direct root collection holders")
	}
	row := body.Type.Elem()
	for row.Kind() == reflect.Pointer {
		row = row.Elem()
	}
	if row.Kind() != reflect.Struct || selectorOpaqueJSON(row) || p.JSONOnly() {
		return nil, fmt.Errorf("MCP converted output requires transparent public rows")
	}
	packages := xshape.Packages{Default: root.PkgPath(), Imports: map[string]string{}}
	if context := c.TypeContext; context != nil {
		if context.PackagePath != "" {
			packages.Default = context.PackagePath
		}
		for _, imported := range context.Imports {
			packages.Imports[imported.Alias] = imported.Package
		}
	}
	for _, declaration := range []struct {
		parameter *spec.Parameter
		field     reflect.StructField
	}{{declaredView, view}, {bodies[0], body}} {
		if declaration.parameter.Cardinality != "" && declaration.parameter.Cardinality != string(spec.CardinalityMany) {
			return nil, fmt.Errorf("MCP output %s must declare many cardinality", declaration.field.Name)
		}
		for _, expression := range []string{declaration.parameter.TypeExpr, declaration.parameter.OutputTypeExpr} {
			if expression == "" {
				continue
			}
			if err := (xshape.Contract{Packages: packages}).ValidateLinked(expression, declaration.field.Type); err != nil {
				return nil, fmt.Errorf("MCP output %s linked declaration: %w", declaration.field.Name, err)
			}
		}
	}
	input.DataField, input.publicOutputRoot = body.Name, true
	result, err := p.compiler.Compile(input)
	if err != nil {
		return nil, err
	}
	if p.representation != nil {
		result, err = result.WithRepresentation(p.representation)
	}
	return result, err
}

// SelectorOutputRoot returns detached binding metadata. The indexed field
// catalog belongs to this effective plan, never to an invocation or SQL view.
func (p *Plan) SelectorOutputRoot() *exec.SelectorOutputRoot {
	if p == nil || p.selectorOutputRoot == nil {
		return nil
	}
	result := *p.selectorOutputRoot
	result.Path = append([]string(nil), result.Path...)
	return &result
}

func (p *Plan) prepareSelectorOutputRoot() error {
	if !p.compileInput.publicOutputRoot {
		return nil
	}
	if p.effectiveSource != nil && p.effectiveSource != p.typeOf {
		return fmt.Errorf("converted public output selection requires canonical Contract representation")
	}

	if p.rows == nil || p.rows.name != p.compileInput.DataField {
		return fmt.Errorf("MCP public output root lost its declared holder")
	}
	row := p.rows.sourceType.Elem()
	for row.Kind() == reflect.Pointer {
		row = row.Elem()
	}
	root := (xshape.Runtime{}).Indirect(p.typeOf)
	original := root.FieldByIndex(p.rows.index)
	field := original
	if metadata := representationForType(p.representation, root); metadata != nil {
		field = metadata.Field(p.rows.index[0])
	}
	holder := p.schemaFieldPresentation(original, field, "")
	admitted := holder != nil && !holder.excluded
	var err error
	fields := []SelectorFieldName(nil)
	if admitted {
		fields, err = p.SelectorFieldNames(row, p.rows.name)
		if err != nil {
			return err
		}
	}
	catalog := map[string]SelectorFieldName{}
	ambiguous := map[string]bool{}
	for _, field := range fields {
		for _, name := range []string{field.PublicName, field.GoName} {
			key := strings.ToLower(strings.TrimSpace(name))
			if previous, exists := catalog[key]; exists && !reflect.DeepEqual(previous.Index, field.Index) {
				ambiguous[key] = true
			}
			catalog[key] = field
		}
	}
	p.selectorOutputRoot = &exec.SelectorOutputRoot{View: p.compileInput.Component.RootView.Name, Path: []string{p.rows.name}, RowType: row, OutputType: p.typeOf, Resolve: func(name string) (exec.SelectorFieldName, bool, error) {
		key := strings.ToLower(strings.TrimSpace(name))
		if ambiguous[key] {
			return exec.SelectorFieldName{}, false, fmt.Errorf("ambiguous public output field %q", name)
		}
		field, exists := catalog[key]
		return exec.SelectorFieldName{GoName: field.GoName, PublicName: field.PublicName, Index: append([]int(nil), field.Index...)}, exists, nil
	}}
	return nil
}
