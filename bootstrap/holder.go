package bootstrap

import (
	"fmt"
	"go/token"
	"reflect"
	"strings"

	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
	xshape "github.com/viant/x/shape"
)

// componentPackagePath is the import path of the public component primitive the
// route-holder convention anchors on: a holder field's type must be this
// package's Component[I, O].
const componentPackagePath = "github.com/viant/xdatly"

// componentTypeName is the component primitive type name within
// componentPackagePath.
const componentTypeName = "Component"

// holderField is one discovered tagged component holder field within a file.
type holderField struct {
	holderType string
	fieldName  string
	tag        dtag.Component
	inputType  string
	outputType string
	imports    []spec.ImportSpec
}

// fileHolders is the per-file scan result: the package clause name and the
// tagged holder fields declared in the file.
type fileHolders struct {
	packageName string
	fields      []holderField
}

// scanFileHolders parses one Go source file (AST only — nothing is compiled or
// loaded) and returns its tagged component holder fields. A field carrying the
// component tag must be a complete route tag on a Component[I, O] field;
// anything else is a hard error rather than a silent skip.
func scanFileHolders(path string) (*fileHolders, error) {
	file, err := (xshape.SourceParser{}).ParseFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to parse %s: %w", path, err)
	}
	result := &fileHolders{packageName: file.Package}
	for _, field := range file.Fields {
		if field.Tag == "" || len(field.Names) == 0 {
			continue
		}
		tag, present, err := dtag.ParseComponent(reflect.StructTag(field.Tag))
		if err != nil {
			return nil, fmt.Errorf("%s: invalid component tag on %s.%s: %w", path, field.Owner, field.Names[0], err)
		}
		if !present {
			continue
		}
		if err := tag.ValidateRoute(); err != nil {
			return nil, fmt.Errorf("%s: incomplete component tag on %s.%s: %w", path, field.Owner, field.Names[0], err)
		}
		inputType, outputType, ok := componentTypeArgs(field.TypeExpr, file.Imports)
		if !ok {
			return nil, fmt.Errorf("%s: component-tagged field %s.%s is not a %s.%s[I, O] holder", path, field.Owner, field.Names[0], componentPackagePath, componentTypeName)
		}
		contractImports, err := componentContractImports(field.TypeExpr, tag, file.Imports)
		if err != nil {
			return nil, fmt.Errorf("%s: invalid contract type on %s.%s: %w", path, field.Owner, field.Names[0], err)
		}
		for _, name := range field.Names {
			result.fields = append(result.fields, holderField{
				holderType: field.Owner, fieldName: name, tag: tag,
				inputType: inputType, outputType: outputType, imports: contractImports,
			})
		}
	}
	return result, nil
}

func componentContractImports(typeExpr string, tag dtag.Component, imports map[string]xshape.SourceImport) ([]spec.ImportSpec, error) {
	expressions := xshape.Resolver{}
	generic, err := expressions.Generic(typeExpr)
	if err != nil {
		return nil, nil
	}
	seen := map[string]bool{}
	var result []spec.ImportSpec
	appendExpr := func(contract string) error {
		qualifiers, err := expressions.Qualifiers(contract)
		if err != nil {
			return err
		}
		for _, alias := range qualifiers {
			if seen[alias] {
				continue
			}
			imported := imports[alias]
			if imported.Path == "" {
				return fmt.Errorf("contract type uses unknown package alias %q", alias)
			}
			if !imported.Explicit {
				return fmt.Errorf("contract package %q must use an explicit import alias", imported.Path)
			}
			seen[alias] = true
			result = append(result, spec.ImportSpec{Alias: alias, Package: imported.Path})
		}
		return nil
	}
	for _, contract := range generic.Arguments {
		if err := appendExpr(contract); err != nil {
			return nil, err
		}
	}
	for _, authored := range []string{tag.Input, tag.Output} {
		authored = strings.TrimSpace(authored)
		if authored == "" {
			continue
		}
		if err = appendExpr(authored); err != nil {
			return nil, err
		}
	}
	if strings.TrimSpace(tag.Handler) != "" {
		reference, err := expressions.Reference(tag.Handler)
		if err != nil {
			return nil, fmt.Errorf("handler factory reference: %w", err)
		}
		if reference.Qualifier != "" && token.IsIdentifier(reference.Qualifier) {
			if err := appendExpr(tag.Handler); err != nil {
				return nil, err
			}
		}
	}
	return result, nil
}

// componentTypeArgs reports whether the field type expression is a
// Component[I, O] reference from componentPackagePath and returns the rendered
// generic argument type names.
func componentTypeArgs(typeExpr string, imports map[string]xshape.SourceImport) (string, string, bool) {
	generic, err := (xshape.Resolver{}).Generic(typeExpr)
	if err != nil || len(generic.Arguments) != 2 || generic.Name != componentTypeName {
		return "", "", false
	}
	if generic.Qualifier == "" || imports[generic.Qualifier].Path != componentPackagePath {
		return "", "", false
	}
	return generic.Arguments[0], generic.Arguments[1], true
}
