package transcribe

import (
	"context"
	"fmt"
	"go/token"
	"go/types"
	"reflect"
	"strings"

	"github.com/viant/bindly"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/dql"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

const nativeHandlerPackage = "github.com/viant/xdatly/handler"

func handlerSymbol(ref string) (string, string, error) {
	i := strings.LastIndex(ref, ".")
	if i <= strings.LastIndex(ref, "/") || i < 1 || !token.IsIdentifier(ref[i+1:]) || !token.IsExported(ref[i+1:]) {
		return "", "", fmt.Errorf("handler symbol %q must be an exported, package-qualified name", ref)
	}
	return ref[:i], ref[i+1:], nil
}

func (c *Compiler) compileSourceHandler(ctx context.Context, source *Source, header *dql.HandlerHeader, body string) (*Result, error) {
	if source.PackageComponent != nil || source.GoHandler != nil || source.VeltyHandler != nil {
		return nil, fmt.Errorf("source handler cannot overlay another component or handler asset")
	}
	prepared, settings, err := prepareHandlerSource(body)
	if err != nil {
		return nil, err
	}
	d := prepared.Directives
	if prepared.TypeContext == nil || prepared.TypeContext.PackagePath == "" {
		return nil, fmt.Errorf("source handler requires an explicit #package destination")
	}
	destination := prepared.TypeContext.PackagePath
	refs := []string{header.Factory, header.InputType, header.OutputType}
	packages, names := make([]string, 3), make([]string, 3)
	for i, ref := range refs {
		var err error
		packages[i], names[i], err = handlerSymbol(ref)
		if err != nil {
			return nil, err
		}
		for _, imp := range prepared.TypeContext.Imports {
			if packages[i] == imp.Alias {
				packages[i] = imp.Package
			}
		}
		if packages[i] == destination {
			return nil, fmt.Errorf("handler factory and contracts must be separate from destination %q", destination)
		}
	}
	build := source.GoBuild.WithContext(ctx)
	if build.Dir == "" {
		build.Dir = source.BaseDir()
	}
	if build.Dir == "" {
		return nil, fmt.Errorf("source handler requires a Go build directory")
	}
	dependencies := append([]string(nil), packages...)
	dependencies = append(dependencies, nativeHandlerPackage)
	for _, imp := range prepared.TypeContext.Imports {
		dependencies = append(dependencies, imp.Package)
	}
	loader, err := build.Importer(dependencies...)
	if err != nil {
		return nil, err
	}
	objects := make([]types.Object, 3)
	for i := range packages {
		pkg, err := loader.Import(packages[i])
		if err != nil {
			return nil, err
		}
		objects[i] = pkg.Scope().Lookup(names[i])
		if objects[i] == nil {
			return nil, fmt.Errorf("handler symbol %s.%s not found in selected build", packages[i], names[i])
		}
	}
	contracts := make([]types.Type, 2)
	for i := range contracts {
		obj, ok := objects[i+1].(*types.TypeName)
		if !ok {
			return nil, fmt.Errorf("handler contract %s is not a Go type", refs[i+1])
		}
		contracts[i] = obj.Type()
		if _, ok := contracts[i].Underlying().(*types.Struct); !ok {
			return nil, fmt.Errorf("handler contract %s must be a named struct or alias", refs[i+1])
		}
	}
	factory, ok := objects[0].(*types.Func)
	if !ok {
		return nil, fmt.Errorf("handler factory must be a declared function")
	}
	sig := factory.Type().(*types.Signature)
	native, err := loader.Import(nativeHandlerPackage)
	if err != nil {
		return nil, err
	}
	contractObject, ok := native.Scope().Lookup("Contract").(*types.TypeName)
	if !ok {
		return nil, fmt.Errorf("selected %s does not declare Contract", nativeHandlerPackage)
	}
	want, err := types.Instantiate(nil, contractObject.Type(), contracts, true)
	if err != nil {
		return nil, err
	}
	if sig.Recv() != nil || sig.TypeParams().Len() != 0 || sig.Params().Len() != 0 || sig.Results().Len() != 1 || !types.Identical(sig.Results().At(0).Type(), want) {
		return nil, fmt.Errorf("handler factory %s must have exact signature func() handler.Contract[%s, %s]", header.Factory, header.InputType, header.OutputType)
	}
	matchedMapping := false
	for _, binding := range source.HandlerBindings {
		if binding == nil || header.Type == "" || binding.mapping.LegacyType != header.Type {
			continue
		}
		if matchedMapping {
			return nil, fmt.Errorf("multiple handler mappings for %q", header.Type)
		}
		matchedMapping = true
		m := binding.mapping
		if m.FactoryPackage != packages[0] || m.FactoryName != names[0] || m.DestinationPackage != destination {
			return nil, fmt.Errorf("authored handler factory conflicts with compiled mapping for %q", header.Type)
		}
		// The mapping does not become source authority; independently compare its
		// contract declarations using the same Go importer (including aliases).
		for i, rt := range []reflect.Type{binding.input, binding.output} {
			pkg, e := loader.Import(rt.PkgPath())
			if e != nil {
				return nil, e
			}
			obj := pkg.Scope().Lookup(rt.Name())
			if obj == nil || !types.Identical(obj.Type(), contracts[i]) {
				return nil, fmt.Errorf("compiled handler contract conflicts with source declaration")
			}
		}
	}
	if err = validateSourceHandlerParameters(loader, contracts[0], prepared.TypeContext, d.Params); err != nil {
		return nil, err
	}
	connector := header.Connector
	if source.Connector != "" {
		if connector != "" && connector != source.Connector {
			return nil, fmt.Errorf("handler connector declarations conflict")
		}
		connector = source.Connector
	}
	settings.DefaultConnector = connector
	component := &spec.Component{
		Key: spec.Key{Kind: spec.KindComponent, Scope: source.Scope, Name: header.Name}, Name: header.Name, Description: header.Description,
		TypeContext: prepared.TypeContext, Settings: settings,
		Routes: []*spec.Route{{Name: header.Name, Path: header.URI, Method: header.Method, Internal: header.Internal, Handler: packages[0] + "." + names[0]}},
	}
	if header.MCPTool {
		component.Routes[0].MCP = []*spec.MCPExposure{{Kind: "tool", Name: header.Name}}
	}
	copy := *source
	copy.GoBuild = build.WithContext(nil)
	copy.Types = typecatalog.NewCatalog()
	if source.Types != nil {
		copy.Types, err = source.Types.Clone()
		if err != nil {
			return nil, err
		}
	}
	// These are source descriptors, not fabricated runtime types. Imported
	// contracts remain handwritten; the generated holder links them at go build.
	descriptors := make([]*x.Type, 2)
	for i, contract := range contracts {
		// Emit the concrete named identity of an alias, as reflect.Type.Name
		// reports that identity when indexed bootstrap checks the linked holder.
		named, ok := types.Unalias(contract).(*types.Named)
		if !ok || !named.Obj().Exported() || named.TypeArgs().Len() != 0 {
			return nil, fmt.Errorf("handler contract %s must resolve to an exported, nongeneric named struct", refs[i+1])
		}
		descriptor := &x.Type{PkgPath: named.Obj().Pkg().Path(), Name: named.Obj().Name()}
		if _, exists, e := copy.Types.Resolve(typecatalog.TranscribeAuthority, descriptor.Key()); e != nil {
			return nil, e
		} else if !exists {
			if err = copy.Types.Register(typecatalog.TypeOriginPackage, descriptor); err != nil {
				return nil, err
			}
		}
		descriptors[i] = descriptor
	}
	input, output := descriptors[0], descriptors[1]
	typeContext := compileTypeContext(&copy, prepared.TypeContext)
	resolver, err := typecatalog.NewResolver(copy.Types, typecatalog.TranscribeAuthority, typeContext)
	if err != nil {
		return nil, err
	}
	return &Result{Source: &copy, Component: component, TypeContext: typeContext, TypeResolver: resolver, TypeAuthority: typecatalog.TranscribeAuthority,
		Contracts:       gen.ContractReferences{Input: &gen.ContractReference{Expression: input.Name, DescriptorKey: input.Key()}, Output: &gen.ContractReference{Expression: output.Name, DescriptorKey: output.Key()}},
		ExternalHandler: &gen.ExternalHandler{Package: packages[0], Name: names[0], Build: build.WithContext(nil)}}, nil
}

type sourceHandlerField struct {
	typ     types.Type
	binding bindly.BindingSpec
}

func sourceHandlerFields(typ types.Type) (map[string]sourceHandlerField, error) {
	names := map[string]bool{}
	seen := map[types.Type]bool{}
	var collect func(types.Type)
	collect = func(t types.Type) {
		if p, ok := types.Unalias(t).(*types.Pointer); ok {
			t = p.Elem()
		}
		if seen[t] {
			return
		}
		seen[t] = true
		s, ok := t.Underlying().(*types.Struct)
		if !ok {
			return
		}
		for i := 0; i < s.NumFields(); i++ {
			f := s.Field(i)
			if f.Exported() {
				names[f.Name()] = true
			}
			if f.Embedded() {
				collect(f.Type())
			}
		}
	}
	collect(typ)
	result := map[string]sourceHandlerField{}
	for name := range names {
		obj, index, _ := types.LookupFieldOrMethod(typ, true, nil, name)
		field, ok := obj.(*types.Var)
		if !ok || !field.IsField() {
			continue
		}
		t := typ
		var tags string
		for _, i := range index {
			if p, ok := types.Unalias(t).(*types.Pointer); ok {
				t = p.Elem()
			}
			s := t.Underlying().(*types.Struct)
			tags, t = s.Tag(i), s.Field(i).Type()
		}
		// Bindly parses name/tags only; no reflected contract is manufactured.
		binding, found, err := bindly.BindingSpecFromField(reflect.StructField{Name: field.Name(), Tag: reflect.StructTag(tags)})
		if err != nil {
			return nil, err
		}
		if !found {
			continue
		}
		key := binding.Name
		if key == "" {
			key = field.Name()
		}
		if _, exists := result[key]; exists {
			return nil, fmt.Errorf("ambiguous handler binding %q", key)
		}
		result[key] = sourceHandlerField{typ: field.Type(), binding: binding}
	}
	return result, nil
}

func validateSourceHandlerParameters(loader types.Importer, input types.Type, tc *spec.TypeContext, params []*spec.Parameter) error {
	fields, err := sourceHandlerFields(input)
	if err != nil {
		return err
	}
	scope := types.NewPackage("datly.local/handlercheck", "handlercheck")
	for _, imp := range tc.Imports {
		pkg, err := loader.Import(imp.Package)
		if err != nil {
			return err
		}
		alias := imp.Alias
		if alias == "" {
			alias = pkg.Name()
		}
		if scope.Scope().Insert(types.NewPkgName(token.NoPos, scope, alias, pkg)) != nil {
			return fmt.Errorf("duplicate handler type import %q", alias)
		}
	}
	seen := map[string]bool{}
	for _, param := range params {
		if seen[param.Name] {
			return fmt.Errorf("duplicate handler parameter %q", param.Name)
		}
		seen[param.Name] = true
		field, ok := fields[param.Name]
		if !ok {
			return fmt.Errorf("handler parameter %q is not in the source contract; remove obsolete declarations", param.Name)
		}
		binding := field.binding
		if param.Source != (spec.BindSource{Kind: binding.Location.Kind, Name: binding.Location.In}) {
			return fmt.Errorf("handler parameter %q binding conflicts with source contract", param.Name)
		}
		if param.Required != nil && *param.Required != (binding.Required != nil && *binding.Required) {
			return fmt.Errorf("handler parameter %q requiredness conflicts with source contract", param.Name)
		}
		if param.TypeExpr != "" && param.TypeExpr != "?" {
			value, err := types.Eval(token.NewFileSet(), scope, token.NoPos, param.TypeExpr)
			if err != nil {
				return fmt.Errorf("handler parameter %q: %w", param.Name, err)
			}
			if !value.IsType() || !types.Identical(value.Type, field.typ) {
				return fmt.Errorf("handler parameter %q type conflicts with source contract", param.Name)
			}
		}
		remainder := *param
		remainder.Name, remainder.TypeExpr, remainder.Raw = "", "", ""
		remainder.Source, remainder.Required, remainder.Declaration = spec.BindSource{}, nil, ""
		if !reflect.DeepEqual(remainder, spec.Parameter{}) {
			return fmt.Errorf("handler parameter %q has unsupported contract overrides", param.Name)
		}
	}
	return nil
}
