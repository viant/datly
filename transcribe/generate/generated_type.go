package generate

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/printer"
	"go/token"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/x"
	smodel "github.com/viant/x/syntetic/model"
)

// GeneratedTypeReference selects one synthetic catalog descriptor for
// package-owned named type emission.
type GeneratedTypeReference struct {
	DescriptorKey string
	Destination   string
}

// GeneratedTypePlan is immutable source material for one package-owned type.
type GeneratedTypePlan struct {
	Name        string
	Destination string
	Declaration string
	Imports     []spec.ImportSpec
}

func (r *planResolver) resolveGeneratedTypes() ([]GeneratedTypePlan, error) {
	references, targetPackage, resolver := r.input.GeneratedTypes, r.input.TargetPackage, r.types
	if len(references) == 0 {
		return nil, nil
	}
	targetPackage = strings.TrimSpace(targetPackage)
	if targetPackage == "" {
		return nil, fmt.Errorf("generated types require a target package")
	}
	if resolver == nil {
		return nil, fmt.Errorf("generated types require type authority")
	}
	result := make([]GeneratedTypePlan, 0, len(references))
	for index, reference := range references {
		key := strings.TrimSpace(reference.DescriptorKey)
		if key == "" {
			return nil, fmt.Errorf("generated type %d descriptor key is required", index+1)
		}
		descriptor, err := resolver.Descriptor(key)
		if err != nil {
			return nil, fmt.Errorf("resolve generated type %q: %w", key, err)
		}
		if descriptor != nil && strings.TrimSpace(descriptor.Key()) != key {
			return nil, fmt.Errorf("generated type descriptor key %q resolved as %q; an exact catalog key is required", key, descriptor.Key())
		}
		plan, err := generatedTypePlan(descriptor, targetPackage, reference.Destination)
		if err != nil {
			return nil, fmt.Errorf("resolve generated type %q: %w", key, err)
		}
		if r.plan.Generation != nil {
			fallback := ""
			if reference.Destination == "" {
				fallback = plan.Destination
			}
			if file := r.plan.Generation.File("type:"+plan.Name, fallback); file != "" {
				plan.Destination = file
			}
		}
		result = append(result, plan)
	}
	sort.SliceStable(result, func(i, j int) bool {
		if result[i].Destination == result[j].Destination {
			return result[i].Name < result[j].Name
		}
		return result[i].Destination < result[j].Destination
	})
	return result, nil
}

func generatedTypePlan(descriptor *x.Type, targetPackage, destination string) (GeneratedTypePlan, error) {
	if descriptor == nil {
		return GeneratedTypePlan{}, fmt.Errorf("descriptor is unavailable")
	}
	name := strings.TrimSpace(descriptor.Name)
	if !token.IsIdentifier(name) || !ast.IsExported(name) {
		return GeneratedTypePlan{}, fmt.Errorf("descriptor name %q must be an exported Go identifier", name)
	}
	if packagePath := strings.TrimSpace(descriptor.PkgPath); packagePath != targetPackage {
		return GeneratedTypePlan{}, fmt.Errorf("descriptor %s belongs to package %q, not target package %q", name, packagePath, targetPackage)
	}
	synthetic := descriptor.SynteticType
	if synthetic == nil || synthetic.TypeSpec == nil || synthetic.TypeSpec.Type == nil {
		return GeneratedTypePlan{}, fmt.Errorf("descriptor %s has no synthetic type declaration", name)
	}
	if syntheticName := strings.TrimSpace(synthetic.Name); syntheticName != "" && syntheticName != name {
		return GeneratedTypePlan{}, fmt.Errorf("descriptor %s synthetic name %q does not match", name, syntheticName)
	}
	if syntheticPackage := strings.TrimSpace(synthetic.PkgPath); syntheticPackage != "" && syntheticPackage != targetPackage {
		return GeneratedTypePlan{}, fmt.Errorf("descriptor %s synthetic package %q does not match target package %q", name, syntheticPackage, targetPackage)
	}
	if synthetic.TypeSpec.Name == nil || synthetic.TypeSpec.Name.Name != name {
		return GeneratedTypePlan{}, fmt.Errorf("descriptor %s declaration name does not match", name)
	}
	if len(synthetic.MethodsAST) > 0 || len(synthetic.PtrMethodsAST) > 0 {
		return GeneratedTypePlan{}, fmt.Errorf("descriptor %s carries methods; generated type assets accept declarations only", name)
	}
	declaration, err := renderTypeDeclaration(synthetic.TypeSpec)
	if err != nil {
		return GeneratedTypePlan{}, err
	}
	imports, err := generatedTypeImports(synthetic.Imports, targetPackage)
	if err != nil {
		return GeneratedTypePlan{}, fmt.Errorf("descriptor %s imports: %w", name, err)
	}
	if strings.TrimSpace(destination) == "" {
		destination = lowerSnake(name) + ".go"
	}
	return GeneratedTypePlan{Name: name, Destination: destination, Declaration: declaration, Imports: imports}, nil
}

func renderTypeDeclaration(typeSpec *ast.TypeSpec) (string, error) {
	var output bytes.Buffer
	output.WriteString("type ")
	if err := printer.Fprint(&output, token.NewFileSet(), typeSpec); err != nil {
		return "", fmt.Errorf("render synthetic type declaration: %w", err)
	}
	return output.String(), nil
}

func generatedTypeImports(source map[string]*smodel.ImportRef, targetPackage string) ([]spec.ImportSpec, error) {
	if len(source) == 0 {
		return nil, nil
	}
	aliases := make(map[string]string, len(source))
	for key, item := range source {
		if item == nil {
			return nil, fmt.Errorf("import %q is nil", key)
		}
		packagePath := strings.TrimSpace(item.Path)
		if packagePath == "" {
			return nil, fmt.Errorf("import %q has no package path", key)
		}
		if packagePath == targetPackage {
			return nil, fmt.Errorf("import %q references its own package", packagePath)
		}
		alias := strings.TrimSpace(item.Alias)
		if alias == "" {
			alias = strings.TrimSpace(key)
		}
		if alias == "" {
			alias = path.Base(packagePath)
		}
		if !token.IsIdentifier(alias) || token.Lookup(alias).IsKeyword() || alias == "_" {
			return nil, fmt.Errorf("import alias %q for %q is invalid", alias, packagePath)
		}
		if previous := aliases[alias]; previous != "" && previous != packagePath {
			return nil, fmt.Errorf("import alias %q is shared by %q and %q", alias, previous, packagePath)
		}
		aliases[alias] = packagePath
	}
	ordered := make([]string, 0, len(aliases))
	for alias := range aliases {
		ordered = append(ordered, alias)
	}
	sort.Strings(ordered)
	result := make([]spec.ImportSpec, 0, len(ordered))
	for _, alias := range ordered {
		result = append(result, spec.ImportSpec{Alias: alias, Package: aliases[alias]})
	}
	return result, nil
}

func generatedTypeFileText(packageName string, plan GeneratedTypePlan) (string, error) {
	var source strings.Builder
	source.WriteString("package ")
	source.WriteString(packageName)
	source.WriteString("\n")
	if len(plan.Imports) > 0 {
		source.WriteString("\nimport (\n")
		for _, item := range plan.Imports {
			source.WriteString("\t")
			if item.Alias != "" {
				source.WriteString(item.Alias)
				source.WriteString(" ")
			}
			source.WriteString(strconv.Quote(item.Package))
			source.WriteString("\n")
		}
		source.WriteString(")\n")
	}
	source.WriteString("\n// ")
	source.WriteString(plan.Name)
	source.WriteString(" is generated canonical type metadata.\n")
	source.WriteString(plan.Declaration)
	source.WriteString("\n")
	formatted, err := format.Source([]byte(source.String()))
	if err != nil {
		return "", fmt.Errorf("format generated type %s: %w", plan.Name, err)
	}
	return string(formatted), nil
}
