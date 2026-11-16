package generate

import (
	"fmt"
	"github.com/viant/datly/internal/selectorcompanion"
	selectorModel "github.com/viant/datly/internal/selectorcompanion/model"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	xshape "github.com/viant/x/shape"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
)

type cubeSourceContracts struct {
	input, output *x.Type
	types         *typecatalog.Resolver
}

func (r *planResolver) cubeSourceContracts(component *spec.Component) (*cubeSourceContracts, error) {
	proof := r.input.PreparedSelector
	if proof == nil || component.SelectorCompanion == nil {
		return nil, nil
	}
	if err := proof.Validate(r.input.Component); err != nil {
		return nil, err
	}
	p := r.plan
	oldIn, oldOut, err := proof.Contracts()
	if err != nil {
		return nil, err
	}
	holder, err := component.SelectorCompanion.Holder()
	if err != nil {
		return nil, err
	}
	companion, err := component.SelectorCompanion.Companion()
	if err != nil {
		return nil, err
	}
	// The admitted holder keeps its exact generic public identities and route
	// declarations. Only its representation becomes source-capable for authoring.
	sourceType := func(descriptor *x.Type) (*x.Type, error) {
		copy, err := (x.Cloner{}).Type(descriptor)
		if err != nil {
			return nil, err
		}
		if copy.SynteticType == nil {
			fields, err := xshape.New(descriptor, r.types.Descriptor).Fields()
			if err != nil {
				return nil, err
			}
			var imports []spec.ImportSpec
			qualifier := func(location, suggested string) string {
				if location == descriptor.PkgPath {
					return ""
				}
				for _, imp := range imports {
					if imp.Package == location {
						return imp.Alias
					}
				}
				alias := availableImportAlias(imports, suggested)
				imports = append(imports, spec.ImportSpec{Package: location, Alias: alias})
				return alias
			}
			var declarations []Field
			for _, field := range fields {
				expression, err := (xshape.Resolver{Qualifier: func(location, suggested string) string { return location }}).Expression(field.ReflectedType)
				if err == nil {
					expression, err = (xshape.Resolver{Rewriter: func(name string) (string, error) {
						ref, e := (xshape.Resolver{}).Reference(name)
						if e != nil {
							return "", e
						}
						if ref.Qualifier == "" {
							return name, nil
						}
						alias := qualifier(ref.Qualifier, packageAlias(ref.Qualifier))
						if alias == "" {
							return ref.Name, nil
						}
						return alias + "." + ref.Name, nil
					}}).Rewrite(expression)
				}
				if err != nil {
					return nil, err
				}
				declarations = append(declarations, Field{Name: field.Name, Type: expression, Tag: string(field.Tag)})
			}
			parsed, err := sourceDescriptors(descriptor.PkgPath, structFileWithImports(p.PackageName(), "", descriptor.Name, declarations, imports))
			if err != nil {
				return nil, err
			}
			if len(parsed) != 1 {
				return nil, fmt.Errorf("prepared selector declaration coverage")
			}
			copy = parsed[0]
			methods, err := xshape.New(descriptor, r.types.Descriptor).Methods(true)
			if err != nil {
				return nil, err
			}
			for _, method := range methods {
				if method.Name == selectorcompanion.Method {
					if len(method.Parameters) != 0 || len(method.Results) != 1 || method.Variadic {
						return nil, fmt.Errorf("prepared selector method signature changed")
					}
					result := strings.TrimPrefix(method.Results[0], descriptor.PkgPath+".")
					file, err := parser.ParseFile(token.NewFileSet(), "selector_signature.go", "package "+p.PackageName()+"; func ("+descriptor.Name+") "+method.Name+"() "+result+" {}", 0)
					if err != nil {
						return nil, err
					}
					copy.SynteticType.MethodsAST = append(copy.SynteticType.MethodsAST, file.Decls[0].(*ast.FuncDecl))
				}
			}
		}
		copy.Type = nil
		copy.SynteticType.ReflectType = nil
		copy.SynteticType.LinkedinType = nil
		return copy, nil
	}
	holder, err = sourceType(holder)
	if err != nil {
		return nil, err
	}
	companion, err = sourceType(companion)
	if err != nil {
		return nil, err
	}
	// Only the exact admitted holder field may omit its generic container:
	// CheckContracts owns Component[I,O] identity. Business generic declarations
	// must retain their native catalog base and specialized dependencies.
	holderFields, err := xshape.New(holder, nil).Fields()
	if err != nil {
		return nil, err
	}
	holderGeneric := ""
	for _, field := range holderFields {
		if field.Name == component.SelectorCompanion.Field() {
			holderGeneric, err = field.CanonicalType()
			if err != nil {
				return nil, err
			}
			break
		}
	}
	if holderGeneric == "" {
		return nil, fmt.Errorf("prepared selector holder field missing")
	}
	prospective := map[string]*x.Type{holder.Key(): holder, companion.Key(): companion}
	add := func(pkg, source string) error {
		declarations, err := sourceDescriptors(pkg, source)
		if err != nil {
			return err
		}
		for _, declaration := range declarations {
			if prospective[declaration.Key()] != nil {
				return fmt.Errorf("duplicate prospective declaration %s", declaration.Key())
			}
			prospective[declaration.Key()] = declaration
		}
		return nil
	}
	if p.Input.Ownership == ContractGenerated {
		if p.Input.Package != oldIn.PkgPath || p.Input.Type != oldIn.Name {
			return nil, fmt.Errorf("prepared selector public input identity changed")
		}
		if err = add(p.Input.Package, p.inputSource(p.PackageName(), "", p.Input.Type, p.Input.Fields, p.Imports)); err != nil {
			return nil, err
		}
	}
	if p.Output.Ownership == ContractGenerated {
		if p.Output.Package != oldOut.PkgPath || p.Output.Type != oldOut.Name {
			return nil, fmt.Errorf("prepared selector public output identity changed")
		}
		if err = add(p.Output.Package, structFileWithImports(p.PackageName(), "", p.Output.Type, p.Output.Fields, p.Imports)); err != nil {
			return nil, err
		}
	}
	// Pure declaration emitters only: no scaffold/resource/cube emission here.
	for _, shape := range append([]*Plan{p}, p.ShapePackages...) {
		if err = add(shape.Package, viewFileContent(shape.PackageName(), shape, generatedSourceViews(shape), true)); err != nil {
			return nil, err
		}
	}
	for _, generated := range p.GeneratedTypes {
		source, err := generatedTypeFileText(p.PackageName(), generated)
		if err != nil {
			return nil, err
		}
		if err = add(p.Package, source); err != nil {
			return nil, err
		}
	}
	original, err := proof.Catalog()
	if err != nil {
		return nil, err
	}
	originalTypes := proof.Types()
	catalog := typecatalog.NewCatalog()
	var declarations []*x.Type
	for _, descriptor := range prospective {
		declarations = append(declarations, descriptor)
	}
	if err = catalog.RegisterAll(typecatalog.TypeOriginGenerated, declarations...); err != nil {
		return nil, err
	}
	visited := map[string]bool{}
	var imports []string
	imported := map[string]bool{}
	addImport := func(key string) {
		if !imported[key] {
			imported[key] = true
			imports = append(imports, key)
		}
	}
	var visit func(*x.Type) error
	visit = func(descriptor *x.Type) error {
		if descriptor == nil {
			return fmt.Errorf("prepared selector dependency missing")
		}
		key := descriptor.Key()
		if visited[key] {
			return nil
		}
		visited[key] = true
		if prospective[key] == nil {
			addImport(key)
		}
		if descriptor.SynteticType == nil {
			return nil
		}
		references, err := xshape.New(descriptor, nil).References()
		if err != nil {
			return err
		}
		for _, key := range references {
			child := prospective[key]
			if child == nil {
				reference, err := (xshape.Resolver{}).Reference(key)
				if err != nil {
					return err
				}
				if descriptor.Key() == holder.Key() && key == holderGeneric {
					continue
				}
				if len(reference.Arguments) > 0 {
					// Native References includes the instantiated expression and its
					// arguments, but not the generic base declaration separately.
					base := reference.BaseName
					if reference.Qualifier != "" {
						base = reference.Qualifier + "." + base
					}
					declared, found, resolveErr := original.Resolve(typecatalog.PackageAuthority, key)
					if resolveErr != nil {
						return fmt.Errorf("prepared selector dependency %q: %w", key, resolveErr)
					}
					if found && declared != nil {
						addImport(key)
					} else {
						generic, resolveErr := originalTypes.Descriptor(base)
						if resolveErr != nil {
							return fmt.Errorf("prepared selector dependency %q: %w", key, resolveErr)
						}
						if generic != nil {
							addImport(base)
						}
					}
				}
				child, err = originalTypes.Descriptor(key)
				if err != nil {
					return fmt.Errorf("prepared selector dependency %q: %w", key, err)
				}
				if child == nil {
					return fmt.Errorf("prepared selector dependency %q missing", key)
				}
			}
			if err = visit(child); err != nil {
				return err
			}
		}
		return nil
	}
	for _, descriptor := range declarations {
		if err = visit(descriptor); err != nil {
			return nil, err
		}
	}
	for _, descriptor := range []*x.Type{oldIn, oldOut} {
		if prospective[descriptor.Key()] == nil {
			if err = visit(descriptor); err != nil {
				return nil, err
			}
		}
	}
	if err = catalog.ImportReferences(original, imports); err != nil {
		return nil, err
	}
	types, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, nil)
	if err != nil {
		return nil, err
	}
	input, err := types.Descriptor(oldIn.Key())
	if err != nil {
		return nil, err
	}
	output, err := types.Descriptor(oldOut.Key())
	if err != nil {
		return nil, err
	}
	component.SelectorCompanion, err = selectorModel.New(holder, component.SelectorCompanion.Field(), companion)
	if err != nil {
		return nil, err
	}
	if _, err = selectorcompanion.SelectContracts(component, input, output, types.Descriptor); err != nil {
		return nil, fmt.Errorf("prospective selector contract: %w", err)
	}
	if strings.TrimSpace(input.Key()) == "" || strings.TrimSpace(output.Key()) == "" {
		return nil, fmt.Errorf("prospective selector public identities missing")
	}
	return &cubeSourceContracts{input: input, output: output, types: types}, nil
}

func generatedSourceViews(p *Plan) []ViewPlan {
	var result []ViewPlan
	for _, view := range p.Views {
		if view.Ownership == ViewGenerated && p.localShape(view.Package) {
			result = append(result, view)
		}
	}
	return result
}
