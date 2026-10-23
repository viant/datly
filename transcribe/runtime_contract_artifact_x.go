package transcribe

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path"
	"reflect"
	"strconv"
	"strings"
	"testing/fstest"

	"github.com/viant/datly/transcribe/internal/discoverytype"
	"github.com/viant/datly/typecatalog"
	xast "github.com/viant/x/loader/ast"
	xload "github.com/viant/x/loader/xreflect"
	"github.com/viant/x/shape"
	"github.com/viant/x/syntetic/model"
)

const artifactXPackage = "compiled.invalid/reader"

// renderArtifactXType persists an anonymous structural contract as Go source.
// viant/x owns the reflect-to-AST conversion and stable Go rendering.
func renderArtifactXType(typeOf reflect.Type) (string, error) {
	if typeOf == nil || typeOf.Kind() != reflect.Struct || typeOf.Name() != "" {
		return "", fmt.Errorf("x artifact requires an anonymous struct")
	}
	for i := 0; i < typeOf.NumField(); i++ {
		if field := typeOf.Field(i); field.PkgPath != "" {
			return "", fmt.Errorf("x artifact cannot persist unexported field %q", field.Name)
		}
	}
	declared, err := xload.BuildType(typeOf,
		xload.WithPackagePath(artifactXPackage),
		xload.WithNamePolicy(func(reflect.Type) (string, bool) { return "Shape", false }))
	if err != nil {
		return "", err
	}
	// The pinned viant/x AST renderer leaves an empty struct's FieldList nil;
	// Go's printer requires a non-nil list. This changes only the temporary AST.
	normalizeArtifactXExpr(declared.TypeSpec.Type)
	file := &model.GoFile{PkgName: "reader"}
	file.AddType(declared)
	return file.Render()
}

// x's reflect bridge may leave empty struct/interface lists nil. The Go
// printer expects explicit empty lists even for nested anonymous shapes.
func normalizeArtifactXExpr(expression ast.Expr) {
	switch value := expression.(type) {
	case *ast.StructType:
		if value.Fields == nil {
			value.Fields = &ast.FieldList{}
		}
		for _, field := range value.Fields.List {
			if field != nil {
				if field.Tag != nil && strings.HasPrefix(field.Tag.Value, "`") && strings.HasSuffix(field.Tag.Value, "`") {
					content := strings.TrimSuffix(strings.TrimPrefix(field.Tag.Value, "`"), "`")
					if strings.Contains(content, "`") { field.Tag.Value = strconv.Quote(content) }
				}
				normalizeArtifactXExpr(field.Type)
			}
		}
	case *ast.InterfaceType:
		if value.Methods == nil {
			value.Methods = &ast.FieldList{}
		}
		for _, field := range value.Methods.List {
			if field != nil {
				normalizeArtifactXExpr(field.Type)
			}
		}
	case *ast.StarExpr:
		normalizeArtifactXExpr(value.X)
	case *ast.ArrayType:
		normalizeArtifactXExpr(value.Elt)
	case *ast.MapType:
		normalizeArtifactXExpr(value.Key)
		normalizeArtifactXExpr(value.Value)
	case *ast.ParenExpr:
		normalizeArtifactXExpr(value.X)
	case *ast.ChanType:
		normalizeArtifactXExpr(value.Value)
	case *ast.FuncType:
		for _, list := range []*ast.FieldList{value.Params, value.Results} {
			if list == nil {
				continue
			}
			for _, field := range list.List {
				if field != nil {
					normalizeArtifactXExpr(field.Type)
				}
			}
		}
	}
}

// materializeArtifactXType reparses the exact saved source through viant/x,
// then requires every named field type to resolve to current host authority.
func materializeArtifactXType(source string, catalog *typecatalog.Catalog) (reflect.Type, error) {
	if strings.TrimSpace(source) == "" {
		return nil, fmt.Errorf("x artifact source is required")
	}
	parsed, err := parser.ParseFile(token.NewFileSet(), "shape.go", source, parser.AllErrors)
	if err != nil {
		return nil, err
	}
	imports := map[string]string{}
	for _, imported := range parsed.Imports {
		name, e := strconv.Unquote(imported.Path.Value)
		if e != nil {
			return nil, e
		}
		alias := path.Base(name)
		if imported.Name != nil {
			alias = imported.Name.Name
		}
		if alias == "_" || alias == "." || alias == "" {
			return nil, fmt.Errorf("x artifact import alias is unsupported")
		}
		if prior := imports[alias]; prior != "" && prior != name {
			return nil, fmt.Errorf("x artifact import alias conflict")
		}
		imports[alias] = name
	}
	folder := fstest.MapFS{
		"go.mod":          &fstest.MapFile{Data: []byte("module compiled.invalid\n\ngo 1.25.8\n")},
		"reader/shape.go": &fstest.MapFile{Data: []byte(source)},
	}
	loaded, err := xast.LoadPackageFS(context.Background(), fs.FS(folder), "reader")
	if err != nil {
		return nil, err
	}
	if loaded.PkgPath != artifactXPackage || len(loaded.Types) != 1 || loaded.Types[0] == nil || loaded.Types[0].Name != "Shape" || len(loaded.Funcs) != 0 || len(loaded.Consts) != 0 || len(loaded.Vars) != 0 {
		return nil, fmt.Errorf("x artifact source has unexpected package or type closure")
	}
	declared := loaded.Types[0]
	lookup := func(name string) (reflect.Type, error) {
		if typeOf := discoverytype.StandardType(name); typeOf != nil {
			return typeOf, nil
		}
		if catalog != nil {
			for _, authority := range []typecatalog.Authority{typecatalog.PackageAuthority, typecatalog.TranscribeAuthority} {
				resolved, found, err := catalog.ResolveRuntimeType(authority, name)
				if err != nil {
					return nil, err
				}
				if found && resolved != nil {
					return resolved, nil
				}
			}
		}
		return nil, fmt.Errorf("x artifact named type %q is not linked", name)
	}
	typeOf, err := (shape.Runtime{Imports: imports, Lookup: lookup}).Type(declared.Body())
	if err != nil {
		return nil, err
	}
	if typeOf == nil || typeOf.Kind() != reflect.Struct {
		return nil, fmt.Errorf("x artifact did not materialize a struct")
	}
	return typeOf, nil
}
