// Package discoverytype holds structural source-type materialization for column discovery only.
package discoverytype

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"reflect"
	"strings"
	"time"

	"github.com/viant/x"
	xshape "github.com/viant/x/shape"
	xhandler "github.com/viant/xdatly/handler"
	xresponse "github.com/viant/xdatly/response"
)

// Materialize consumes an already selected descriptor; callers retain identity validation, caching and recursion ownership.
func Materialize(descriptor *x.Type, lookup xshape.Lookup, runtime xshape.Runtime) (reflect.Type, error) {
	// Source-only named scalars and containers need their underlying type only
	// for discovery. Generated Go keeps the named identity and its methods.
	key := descriptor.Key()
	var err error
	var declaration ast.Expr
	imports := map[string]string{}
	if synthetic := descriptor.SynteticType; synthetic != nil && synthetic.TypeSpec != nil {
		declaration = synthetic.TypeSpec.Type
		for alias, imported := range synthetic.Imports {
			if imported != nil {
				imports[alias] = imported.Path
			}
		}
	} else if strings.TrimSpace(descriptor.Definition) != "" {
		declaration, err = parser.ParseExpr(descriptor.Definition)
		if err != nil {
			return nil, fmt.Errorf("discovery declaration %q: %w", key, err)
		}
	}
	if declaration != nil {
		if _, isStruct := declaration.(*ast.StructType); !isStruct {
			canonical, err := (xshape.Resolver{Package: descriptor.PkgPath, Imports: imports}).Canonical(declaration)
			if err != nil {
				return nil, fmt.Errorf("discovery declaration %q: %w", key, err)
			}
			result, err := runtime.Type(canonical)
			if err != nil {
				return nil, fmt.Errorf("materialize discovery type %q: %w", key, err)
			}
			return result, nil
		}
	}
	fields, err := xshape.New(descriptor, lookup).Fields()
	if err != nil {
		return nil, err
	}
	retainedAnonymousRoots := map[int]bool{}
	for _, field := range fields {
		if field.Exported && field.Anonymous && len(field.Index) == 1 {
			retainedAnonymousRoots[field.Index[0]] = true
		}
	}
	temporary := make([]xshape.RuntimeField, 0, len(fields))
	for _, field := range fields {
		if !field.Exported || (len(field.Index) > 1 && retainedAnonymousRoots[field.Index[0]]) {
			continue
		}
		canonical, err := field.CanonicalType()
		if err != nil {
			return nil, err
		}
		temporary = append(temporary, xshape.RuntimeField{Name: field.Name, TypeExpr: canonical, Type: field.ReflectedType, Tag: field.Tag, Anonymous: field.Anonymous})
	}
	result, err := runtime.Struct(temporary)
	if err != nil {
		return nil, fmt.Errorf("materialize discovery type %q: %w", key, err)
	}
	return result, nil
}

func StandardType(name string) reflect.Type {
	switch strings.TrimSpace(name) {
	case "encoding/json.RawMessage":
		return reflect.TypeFor[json.RawMessage]()
	case "time.Time":
		return reflect.TypeFor[time.Time]()
	case "github.com/viant/xdatly/response.Status", "response.Status":
		return reflect.TypeFor[xresponse.Status]()
	case "github.com/viant/xdatly/response.Metrics", "response.Metrics":
		return reflect.TypeFor[xresponse.Metrics]()
	case "github.com/viant/xdatly/handler.Violation", "handler.Violation", "xhandler.Violation":
		return reflect.TypeFor[xhandler.Violation]()
	}
	return nil
}
