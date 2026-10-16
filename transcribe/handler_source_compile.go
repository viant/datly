package transcribe

import (
	"context"
	"fmt"
	"go/token"
	"go/types"
	"reflect"
	"strconv"
	"strings"

	"github.com/viant/bindly"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/bootstrap/routes"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/tag"
	"github.com/viant/datly/transcribe/dql"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/transcribe/internal/link"
	"github.com/viant/datly/transcribe/internal/program"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

const nativeHandlerPackage = "github.com/viant/xdatly/handler"

func (c *Compiler) compileSourceHandler(ctx context.Context, source *Source, header *dql.HandlerHeader, body string) (*Result, error) {
	if source.PackageComponent != nil || source.GoHandler != nil || source.VeltyHandler != nil {
		return nil, fmt.Errorf("source handler cannot overlay another component or handler asset")
	}
	prepared := dql.PrepareSource(body)
	if err := prepared.Err(); err != nil {
		return nil, err
	}
	settings := prepared.Directives.Settings.Clone()
	if settings == nil {
		settings = &spec.Settings{}
	}

	d := prepared.Directives
	if prepared.TypeContext == nil || prepared.TypeContext.PackagePath == "" {
		return nil, fmt.Errorf("source handler requires an explicit #package destination")
	}
	destination := prepared.TypeContext.PackagePath
	if names, ok := program.FactoryContracts(header, prepared); ok {
		if header.AdapterFactory != "" {
			return nil, fmt.Errorf("handler adapter requires source-authored contracts separate from the generated destination")
		}
		return c.compileGeneratedPostFactory(ctx, source, header, prepared, settings, names)
	}
	var err error
	if prepared, settings, err = prepareHandlerSource(body); err != nil {
		return nil, err
	}
	refs := []string{header.Factory, header.InputType, header.OutputType}
	if header.AdapterFactory != "" {
		refs = append(refs, header.AdapterFactory)
	}
	packages, names := make([]string, len(refs)), make([]string, len(refs))
	for i, ref := range refs {
		var err error
		packages[i], names[i], err = program.Symbol(ref)
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
	if header.AdapterFactory != "" {
		dependencies = append(dependencies, "github.com/viant/datly/runtime/handler")
	}
	for _, imp := range prepared.TypeContext.Imports {
		dependencies = append(dependencies, imp.Package)
	}
	loader, err := build.Importer(dependencies...)
	if err != nil {
		return nil, err
	}
	objects := make([]types.Object, len(refs))
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
	var adapterPackage, adapterName string
	if header.AdapterFactory != "" {
		if err := validateSourceHandlerAdapter(loader, objects[3], header.AdapterFactory); err != nil {
			return nil, err
		}
		adapterPackage, adapterName = packages[3], names[3]
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
	fields, err := sourceHandlerFields(contracts[0])
	if err != nil {
		return nil, err
	}
	outputs, err := sourceHandlerFields(contracts[1])
	if err != nil {
		return nil, err
	}
	wireTypes, wireMatch, err := sourceHandlerWireTypes(loader, fields, prepared.TypeContext)
	if err != nil {
		return nil, err
	}
	if err = validateSourceHandlerParameters(loader, fields, outputs, prepared.TypeContext, d.Params, wireMatch); err != nil {
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
		TypeContext: prepared.TypeContext, Settings: settings, Parameters: d.Params,
		Routes: []*spec.Route{{Name: header.Name, Path: header.URI, Method: header.Method, Internal: header.Internal, Handler: packages[0] + "." + names[0]}},
	}
	if header.Declarative {
		component.Documentation = d.Documentation.Clone()
		component.Routes = d.ComponentRoutes(header.Name, packages[0]+"."+names[0])
	}
	if header.MCPTool {
		component.Routes[0].MCP = []*spec.MCPExposure{{Kind: "tool", Name: header.Name}}
	}
	if header.Declarative {
		if err := (routes.Compiler{Component: component}).Compile(); err != nil {
			return nil, err
		}
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
	for _, descriptor := range wireTypes {
		if _, exists, e := copy.Types.Resolve(typecatalog.TranscribeAuthority, descriptor.Key()); e != nil {
			return nil, e
		} else if !exists {
			if err = copy.Types.Register(typecatalog.TypeOriginPackage, descriptor); err != nil {
				return nil, err
			}
		}
	}
	wireReferences := make([]string, 0, len(wireTypes))
	for _, descriptor := range wireTypes {
		wireReferences = append(wireReferences, descriptor.PkgPath+"."+descriptor.Name)
	}
	input, output := descriptors[0], descriptors[1]
	typeContext := compileTypeContext(&copy, prepared.TypeContext)
	resolver, err := typecatalog.NewResolver(copy.Types, typecatalog.TranscribeAuthority, typeContext)
	if err != nil {
		return nil, err
	}
	return &Result{Source: &copy, Component: component, TypeContext: typeContext, TypeResolver: resolver, TypeAuthority: typecatalog.TranscribeAuthority,
		Contracts:       gen.ContractReferences{Input: &gen.ContractReference{Expression: input.Name, DescriptorKey: input.Key()}, Output: &gen.ContractReference{Expression: output.Name, DescriptorKey: output.Key()}},
		ExternalHandler: &gen.ExternalHandler{Package: packages[0], Name: names[0], AdapterPackage: adapterPackage, AdapterName: adapterName, Build: build.WithContext(nil), WireTypes: wireReferences}}, nil
}

func validateSourceHandlerAdapter(loader types.Importer, object types.Object, reference string) error {
	factory, ok := object.(*types.Func)
	if !ok {
		return fmt.Errorf("handler adapter %s must be a declared function", reference)
	}
	pkg, err := loader.Import("github.com/viant/datly/runtime/handler")
	if err != nil {
		return err
	}
	handler := pkg.Scope().Lookup("TypedHandler")
	if handler == nil {
		return fmt.Errorf("runtime handler package does not declare TypedHandler")
	}
	sig := factory.Type().(*types.Signature)
	if sig.Recv() != nil || sig.TypeParams().Len() != 0 || sig.Variadic() || sig.Params().Len() != 0 || sig.Results().Len() != 2 ||
		!types.Identical(sig.Results().At(0).Type(), handler.Type()) || !types.Identical(sig.Results().At(1).Type(), types.Universe.Lookup("error").Type()) {
		return fmt.Errorf("handler adapter %s must have exact signature func() (runtime/handler.TypedHandler, error)", reference)
	}
	return nil
}

type sourceHandlerField struct {
	pkg      *types.Package
	typ      types.Type
	binding  bindly.BindingSpec
	tags     reflect.StructTag
	embedded bool
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
		result[key] = sourceHandlerField{pkg: field.Pkg(), typ: field.Type(), binding: binding, tags: reflect.StructTag(tags), embedded: field.Embedded()}
	}
	return result, nil
}

func validateSourceHandlerParameters(loader types.Importer, fields, outputs map[string]sourceHandlerField, tc *spec.TypeContext, params []*spec.Parameter, wireMatch func(string, string) (string, bool)) error {
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
		if param.Source.Kind == "output" {
			field, ok = outputs[param.Name]
		}
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
		typeExpr := param.TypeExpr
		if param.Codec != nil {
			metadata, err := tag.ParseCodec(field.tags.Get("codec"))
			if err != nil {
				return err
			}
			if metadata == nil || param.Codec.Body != metadata.Body || !reflect.DeepEqual(param.Codec.Args, metadata.Arguments) {
				return fmt.Errorf("handler parameter %q codec conflicts with source contract", param.Name)
			}
			if binding.DataType != "" && param.TypeExpr != binding.DataType {
				return fmt.Errorf("handler parameter %q codec source type conflicts with source contract", param.Name)
			}
			if binding.DataType == "" {
				value, err := types.Eval(token.NewFileSet(), scope, token.NoPos, param.TypeExpr)
				if err != nil || !types.Identical(value.Type, field.typ) {
					return fmt.Errorf("handler parameter %q codec source type cannot be proved from source contract", param.Name)
				}
			}
			typeExpr = param.OutputTypeExpr
		}
		if typeExpr != "" && typeExpr != "?" {
			value, err := types.Eval(token.NewFileSet(), scope, token.NoPos, typeExpr)
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
		if param.ErrorStatusCode != 0 {
			if param.ErrorStatusCode != binding.ErrorCode {
				return fmt.Errorf("handler parameter %q error status conflicts with source contract", param.Name)
			}
			remainder.ErrorStatusCode = 0
		}
		if param.WireTypeExpr != "" {
			canonical, ok := wireMatch(param.Name, param.WireTypeExpr)
			if !ok {
				return fmt.Errorf("handler parameter %q wireType conflicts with source contract", param.Name)
			}
			param.WireTypeExpr = canonical
		}
		remainder.WireTypeExpr = ""
		if param.Tag != "" {
			reconciled, ok := sourceHandlerTags(field.tags, param.Tag, field.embedded, func(value string) (string, bool) { return wireMatch(param.Name, value) })
			if !ok {
				return fmt.Errorf("handler parameter %q tags conflict with source contract", param.Name)
			}
			param.Tag = reconciled
			remainder.Tag = ""
		}
		remainder.Codec = nil
		if param.Codec != nil {
			remainder.OutputTypeExpr = ""
		}
		if !reflect.DeepEqual(remainder, spec.Parameter{}) {
			return fmt.Errorf("handler parameter %q has unsupported contract overrides", param.Name)
		}
	}
	return nil
}

// Compare complete tag keys and decoded values. Substring matching would let
// xjson satisfy json, and cannot prove a declaration belongs to the contract.
func sourceHandlerTagsMatch(existing reflect.StructTag, authored string, embedded bool) bool {
	_, ok := sourceHandlerTags(existing, authored, embedded, nil)
	return ok
}

func sourceHandlerTags(existing reflect.StructTag, authored string, embedded bool, wireMatch func(string) (string, bool)) (string, bool) {
	var reconciled []string
	seen := map[string]bool{}
	for rest := strings.TrimSpace(authored); rest != ""; {
		colon := strings.IndexByte(rest, ':')
		if colon <= 0 {
			return "", false
		}
		key := rest[:colon]
		if strings.ContainsAny(key, " \t\r\n\"") || seen[key] {
			return "", false
		}
		seen[key] = true
		quoted := rest[colon+1:]
		if len(quoted) < 2 || quoted[0] != '"' {
			return "", false
		}
		end := 1
		for end < len(quoted) {
			if quoted[end] == '\\' {
				end += 2
				continue
			}
			if quoted[end] == '"' {
				break
			}
			end++
		}
		if end >= len(quoted) {
			return "", false
		}
		value, err := strconv.Unquote(quoted[:end+1])
		if err != nil {
			return "", false
		}
		actual, found := existing.Lookup(key)
		if !found && embedded && key == "anonymous" && value == "true" {
			actual, found = "true", true
		}
		if key == "wireType" && wireMatch != nil {
			canonical, ok := wireMatch(value)
			if !ok {
				return "", false
			}
			value = canonical
		} else if !found || actual != value {
			return "", false
		}
		reconciled = append(reconciled, key+":"+strconv.Quote(value))
		rest = quoted[end+1:]
		if rest != "" && rest[0] != ' ' {
			return "", false
		}
		rest = strings.TrimSpace(rest)
	}
	return strings.Join(reconciled, " "), len(seen) != 0
}

func (c *Compiler) compileGeneratedPostFactory(ctx context.Context, source *Source, header *dql.HandlerHeader, prepared *dql.PreparedSource, settings *spec.Settings, names [3]string) (*Result, error) {
	destination := prepared.TypeContext.PackagePath
	build := source.GoBuild.WithContext(ctx)
	if build.Dir == "" {
		build.Dir = source.BaseDir()
	}
	if build.Dir == "" {
		return nil, fmt.Errorf("source handler requires a Go build directory")
	}
	ownership := &factoryDeclarations{ctx: ctx, build: build, pkg: destination}
	// Qualified local contracts use the same generated ownership as bare names.
	// Protect selected authored declarations before discovery or publication.
	for i, role := range []string{"input", "output"} {
		file := settings.Generation.File(role, role+".go")
		if err := ownership.validateGeneratedContract(header.Name, names[i+1], file); err != nil {
			return nil, fmt.Errorf("handler authored contracts must be separate from destination %q: %w", destination, err)
		}
	}
	if err := ownership.requireFunction(names[0]); err != nil {
		return nil, err
	}
	if prepared.Directives.Static != nil {
		return nil, fmt.Errorf("generated POST factory cannot contain static content")
	}
	settings.InputType, settings.OutputType = names[1], names[2]
	connector := header.Connector
	if source.Connector != "" {
		if connector != "" && connector != source.Connector {
			return nil, fmt.Errorf("handler connector declarations conflict")
		}
		connector = source.Connector
	}
	settings.DefaultConnector = connector
	d := prepared.Directives
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: source.Scope, Name: header.Name}, Name: header.Name,
		Description: header.Description, Documentation: d.Documentation.Clone(), TypeContext: prepared.TypeContext, Settings: settings, Parameters: d.Params, Views: d.Views,
		Routes: d.ComponentRoutes(header.Name, destination+"."+names[0])}
	for index, view := range component.Views {
		if view != nil {
			component.Views[index] = view.Clone()
			component.Views[index].Key.Scope = source.Scope
		}
	}
	if err := (routes.Compiler{Component: component}).Compile(); err != nil {
		return nil, err
	}
	copy := *source
	copy.GoBuild = build.WithContext(nil)
	copy.Types = typecatalog.NewCatalog()
	var err error
	if source.Types != nil {
		copy.Types, err = source.Types.Clone()
		if err != nil {
			return nil, err
		}
	}
	if err = synthesizeConstantParams(component); err != nil {
		return nil, err
	}
	typeContext := compileTypeContext(&copy, prepared.TypeContext)
	if err = compilePredicateContracts(component, copy.Types, typeContext); err != nil {
		return nil, err
	}
	resolver, err := typecatalog.NewResolver(copy.Types, typecatalog.TranscribeAuthority, typeContext)
	if err != nil {
		return nil, err
	}
	if !source.Const.Empty() {
		if err = source.Const.Validate(component, resolver.Type); err != nil {
			return nil, err
		}
	}
	declarations, err := newDeclarationCompiler(component, component.Views, copy.Resources).compile()
	if err != nil {
		return nil, err
	}
	component.Views, err = declarations.removeSkeletons(component.Views)
	if err != nil {
		return nil, err
	}
	component.Views, err = link.MergeViews(component.Views, declarations.views)
	if err != nil {
		return nil, err
	}
	viewBindings, err := link.NormalizeIndependentViewParams(component, declarations.viewsByParam)
	if err != nil {
		return nil, err
	}
	linkedViews := gen.ViewReferences{}
	inputShape, err := c.compileFactoryInputShape(ctx, &copy, prepared, component, connector, resolver, declarations.generation, gen.ViewBindings(viewBindings), linkedViews, ownership)
	if err != nil {
		return nil, fmt.Errorf("generated POST factory cannot contain SQL except a validated input graph: %w", err)
	}
	// Auxiliary shape-only composers retain their existing SQL-free runtime.
	if inputShape != nil && inputShape.Auxiliary && len(component.Views) == 0 {
		settings.DefaultConnector = ""
	}
	if _, err = bootstrap.NormalizeCodecReferences(component, typeContext); err != nil {
		return nil, err
	}
	return &Result{Source: &copy, Component: component, TypeContext: typeContext, TypeResolver: resolver, TypeAuthority: typecatalog.TranscribeAuthority,
		Declarations: declarations.generation, Views: linkedViews, ViewBindings: gen.ViewBindings(viewBindings), ExternalHandler: &gen.ExternalHandler{Package: destination, Name: names[0], Build: build.WithContext(nil), GeneratedContracts: true, InputShape: inputShape}}, nil
}

// Go's selected source list is available before the generated contracts exist.
