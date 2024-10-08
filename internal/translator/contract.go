package translator

import (
	"context"
	"fmt"
	"github.com/viant/afs/url"
	"github.com/viant/datly/view"
	"github.com/viant/xreflect"
	"go/ast"
	"go/parser"
	"path"
	"reflect"
	"strings"
)

func (r *Resource) loadState(ctx context.Context, URL string) error {
	typeRegistry := r.ensureRegistry()
	aPath := url.Path(URL)
	location, _ := path.Split(aPath)

	var typeDefs view.TypeDefinitions
	var registered = map[string]map[string]bool{}

	filePackage := ""
	if index := strings.Index(URL, r.ModuleLocation); index != -1 && r.ModuleLocation != "" {
		filePackage = URL[index+len(r.ModuleLocation)+1:]
		if index = strings.LastIndex(filePackage, "/"); index != -1 {
			filePackage = filePackage[:index]
		}
	}

	dirTypes, err := xreflect.ParseTypes(location,
		xreflect.WithParserMode(parser.ParseComments),
		xreflect.WithRegistry(typeRegistry),
		xreflect.WithModule(r.Module, r.rule.ModuleLocation),
		xreflect.WithOnField(func(typeName string, field *ast.Field, imports xreflect.GoImports) error {
			return nil
		}), xreflect.WithOnLookup(func(packagePath, pkg, typeName string, rType reflect.Type) {
			if pkg == "" {
				return
			}
			if strings.HasSuffix(filePackage, "/"+pkg) {
				pkg = filePackage
			}
			if _, ok := registered[pkg]; !ok {
				registered[pkg] = map[string]bool{}
			}
			if registered[pkg][typeName] {
				return
			}
			registered[pkg][typeName] = true
			r.registerType(typeName, pkg, rType, true, &typeDefs)
		}))
	if err != nil {
		return err
	}
	inputTypeName := dirTypes.MatchTypeNamesInPath(aPath, "@input")
	outputTypeName := dirTypes.MatchTypeNamesInPath(aPath, "@output")
	loadType := func(typeName string) (reflect.Type, error) {
		return r.loadType(dirTypes, filePackage, typeName, aPath, registered, &typeDefs)
	}
	if inputTypeName == "" && outputTypeName == "" {
		return fmt.Errorf("failed to locate contract types in %s, \n\tforgot struct{...}//@input or //@output comment ?", aPath)
	}
	if inputTypeName != "" {
		inputType, err := loadType(inputTypeName)
		if err != nil {
			return err
		}
		if err = r.extractState(loadType, inputType, &r.State); err != nil {
			return err
		}
		r.AppendTypeDefinitions(typeDefs)
	}
	if outputTypeName != "" {
		outputType, err := loadType(outputTypeName)
		if err != nil {
			return err
		}
		if err = r.extractState(loadType, outputType, &r.OutputState); err != nil {
			return err
		}
		r.AppendTypeDefinitions(typeDefs.Exclude(outputTypeName))
	}
	return nil
}
