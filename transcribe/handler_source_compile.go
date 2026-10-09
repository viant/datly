package transcribe

import (
	"context"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"

	"github.com/viant/bindly"
	"github.com/viant/datly/bootstrap"
	readerpredicate "github.com/viant/datly/runtime/predicate/velty"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/tag"
	"github.com/viant/datly/transcribe/dql"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/transcribe/gobuild"
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
	if names, ok := generatedPostFactory(header, prepared); ok {
		return c.compileGeneratedPostFactory(ctx, source, header, prepared, settings, names)
	}
	var err error
	if prepared, settings, err = prepareHandlerSource(body); err != nil {
		return nil, err
	}
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
	if err = validateSourceHandlerParameters(loader, contracts[0], contracts[1], prepared.TypeContext, d.Params); err != nil {
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
		component.Routes[0].Internal = d.Internal || d.MCPOnly
		component.Routes[0].APIKeyHeader = d.Route.APIKeyHeader
		component.Routes[0].APIKeyValue = d.Route.APIKeyValue
		if d.MCP != nil {
			component.Routes[0].MCP = []*spec.MCPExposure{d.MCP.Clone()}
		}
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
		result[key] = sourceHandlerField{typ: field.Type(), binding: binding, tags: reflect.StructTag(tags), embedded: field.Embedded()}
	}
	return result, nil
}

func validateSourceHandlerParameters(loader types.Importer, input, output types.Type, tc *spec.TypeContext, params []*spec.Parameter) error {
	fields, err := sourceHandlerFields(input)
	if err != nil {
		return err
	}
	outputs, err := sourceHandlerFields(output)
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
		if param.Tag != "" {
			if !sourceHandlerTagsMatch(field.tags, param.Tag, field.embedded) {
				return fmt.Errorf("handler parameter %q tags conflict with source contract", param.Name)
			}
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
	seen := map[string]bool{}
	for rest := strings.TrimSpace(authored); rest != ""; {
		colon := strings.IndexByte(rest, ':')
		if colon <= 0 {
			return false
		}
		key := rest[:colon]
		if strings.ContainsAny(key, " \t\r\n\"") || seen[key] {
			return false
		}
		seen[key] = true
		quoted := rest[colon+1:]
		if len(quoted) < 2 || quoted[0] != '"' {
			return false
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
			return false
		}
		value, err := strconv.Unquote(quoted[:end+1])
		if err != nil {
			return false
		}
		actual, found := existing.Lookup(key)
		if !found && embedded && key == "anonymous" && value == "true" {
			actual, found = "true", true
		}
		if !found || actual != value {
			return false
		}
		rest = quoted[end+1:]
		if rest != "" && rest[0] != ' ' {
			return false
		}
		rest = strings.TrimSpace(rest)
	}
	return len(seen) != 0
}

// generatedPostFactory is deliberately narrower than source-backed handler
// registration: canonical post execution owns both local generated contracts,
// independently of POST or PATCH transport.
func generatedPostFactory(header *dql.HandlerHeader, prepared *dql.PreparedSource) ([3]string, bool) {
	var names [3]string
	if header == nil || !header.Declarative || (!strings.EqualFold(header.Method, "POST") && !strings.EqualFold(header.Method, "PATCH")) || prepared == nil || prepared.TypeContext == nil || prepared.TypeContext.PackagePath == "" {
		return names, false
	}
	destination := prepared.TypeContext.PackagePath
	for i, ref := range []string{header.Factory, header.InputType, header.OutputType} {
		if token.IsIdentifier(ref) && token.IsExported(ref) {
			names[i] = ref
			continue
		}
		// Qualified contract references retain the existing source-backed path.
		// Only bare local names explicitly opt into generated ownership.
		if i > 0 {
			return names, false
		}
		pkg, name, err := handlerSymbol(ref)
		if err != nil {
			return names, false
		}
		for _, imp := range prepared.TypeContext.Imports {
			if pkg == imp.Alias {
				pkg = imp.Package
			}
		}
		if pkg != destination {
			return names, false
		}
		names[i] = name
	}
	return names, true
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
	if err := requireSelectedFactoryFunction(ctx, build, destination, names[0]); err != nil {
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
		Routes: []*spec.Route{{Name: header.Name, Path: header.URI, Method: header.Method, Internal: d.Internal || d.MCPOnly, Handler: destination + "." + names[0], RequestBodyMode: d.Route.RequestBodyMode, APIKeyHeader: d.Route.APIKeyHeader, APIKeyValue: d.Route.APIKeyValue}}}
	for index, view := range component.Views {
		if view != nil {
			component.Views[index] = view.Clone()
			component.Views[index].Key.Scope = source.Scope
		}
	}
	if d.MCP != nil {
		component.Routes[0].MCP = []*spec.MCPExposure{d.MCP.Clone()}
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
	if err = (readerpredicate.DefinitionCompiler{Context: typeContext, Catalog: copy.Types, RequireAvailable: true}).Compile(component); err != nil {
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
	declarations, err := newDeclarationCompiler(component, component.Views).compile()
	if err != nil {
		return nil, err
	}
	component.Views, err = declarations.removeSkeletons(component.Views)
	if err != nil {
		return nil, err
	}
	loader := &componentLoader{}
	component.Views, err = loader.mergeViews(component.Views, declarations.views)
	if err != nil {
		return nil, err
	}
	viewBindings, err := loader.normalizeIndependentViewParams(component, declarations.viewsByParam)
	if err != nil {
		return nil, err
	}
	inputShape, err := c.compileFactoryInputShape(ctx, &copy, prepared, component, connector, resolver, build, declarations.generation, gen.ViewBindings(viewBindings))
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
		Declarations: declarations.generation, ViewBindings: gen.ViewBindings(viewBindings), ExternalHandler: &gen.ExternalHandler{Package: destination, Name: names[0], Build: build.WithContext(nil), GeneratedContracts: true, InputShape: inputShape}}, nil
}

// Go's selected source list is available before the generated contracts exist.
// Only declaration kind is checked here; the existing staged Go build checks
// the exact native generic factory signature with those generated contracts.
func requireSelectedFactoryFunction(ctx context.Context, build *gobuild.Context, pkg, name string) error {
	args := []string{"list", "-e", "-json"}
	if build.Tags != "" {
		args = append(args, "-tags", build.Tags)
	}
	env := append(os.Environ(), build.Env...)
	flags := ""
	for _, value := range env {
		if v, ok := strings.CutPrefix(value, "GOFLAGS="); ok {
			flags = v
		}
	}
	for _, flag := range strings.Fields(flags) {
		if strings.Trim(flag, "\"'") == "-mod=mod" {
			args = append(args, "-mod=readonly")
		}
	}
	args = append(args, "--", pkg)
	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Dir, cmd.Env = build.Dir, env
	data, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("handler factory build selection: %w", err)
	}
	var target struct {
		Dir               string
		GoFiles, CgoFiles []string
	}
	if err = json.Unmarshal(data, &target); err != nil {
		return err
	}
	for _, file := range append(target.GoFiles, target.CgoFiles...) {
		parsed, err := parser.ParseFile(token.NewFileSet(), filepath.Join(target.Dir, file), nil, 0)
		if err != nil {
			return err
		}
		for _, decl := range parsed.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.Name == name {
				return nil
			}
		}
	}
	return fmt.Errorf("handler factory %s.%s must be a declared function in the selected build", pkg, name)
}
