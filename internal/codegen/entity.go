package codegen

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/viant/datly/internal/plugin"
	"github.com/viant/datly/view/extension"
	"github.com/viant/datly/view/state"
	"github.com/viant/xreflect"
	"go/format"
	"reflect"
	"strings"
)

const (
	registerTypeTemplate     = `	core.RegisterType(PackageName, "%v", reflect.TypeOf(%v{}), checksum.GeneratedTime)`
	registerMapEntryTemplate = `	"%v": reflect.TypeOf(%v{}),`
)

//go:embed tmpl/entity.gox
var entityTemplate string

// GenerateEntity generate golang entity
func (t *Template) GenerateEntity(ctx context.Context, pkg string, info *plugin.Info) (string, error) {
	pkg = info.Package(pkg)
	if t.MethodFragment != "" && t.MethodFragment != "get" {
		pkg = strings.ToLower(t.MethodFragment)
	}
	if err := t.TypeDef.Init(context.Background(), extension.Config.Types.Lookup); err != nil {
		return "", err
	}
	rType := t.TypeDef.Schema.Type()
	imps := t.Imports.Clone()
	initCode := ""
	globalDeclaration := ""
	imps.AddPackage("reflect")
	if !info.IsStandalone() {
		imps.AddPackage(info.TypeCorePkg())
		imps.AddPackage(info.ChecksumPkg())
		initCode = t.generateRegisterType()
	} else {
		globalDeclaration = "var Types map[string]reflect.Type"
		initCode = fmt.Sprintf(`	Types = map[string]reflect.Type{
	%v
	}
`, t.generateMapTypeBody())
	}
	initSnippet := strings.Replace(entityTemplate, "$Init", initCode, 1)

	registryPackage := pkg + "/" + t.FileMethodFragment()
	if t.Resource.Rule.InputType != "" {
		if index := strings.LastIndex(t.Resource.Rule.InputType, "."); index != -1 {
			registryPackage = t.Resource.Rule.InputType[:index]
		}
	}

	initSnippet = strings.Replace(initSnippet, "$Package", registryPackage, 1)

	initSnippet = strings.Replace(initSnippet, "$GlobalDeclaration", globalDeclaration, 1)

	recv := strings.ToLower(t.TypeDef.Name[:1])

	afterSnippet := strings.Builder{}
	for i := 0; i < rType.NumField(); i++ {
		field := rType.Field(i)
		isPtr := field.Type.Kind() == reflect.Ptr
		rawType := field.Type
		if isPtr {
			rawType = field.Type.Elem()
		}
		if rawType.Name() == "" || strings.Contains(string(field.Tag), `json:"-\"`) {
			continue
		}

		afterSnippet.WriteString(fmt.Sprintf("\nfunc (%v *%v) Set%v(value %v) {", recv, t.TypeDef.Name, field.Name, rawType.String()))
		if isPtr {
			afterSnippet.WriteString(fmt.Sprintf("\n\t%v.%v = &value", recv, field.Name))
		} else {
			afterSnippet.WriteString(fmt.Sprintf("\n\t%v.%v = value", recv, field.Name))
		}
		afterSnippet.WriteString(fmt.Sprintf("\n\t%v.Has.%v = true", recv, field.Name))
		afterSnippet.WriteString("\n}\n\n")
	}
	if !t.IsHandler {
		afterSnippet = strings.Builder{}
	}

	generatedStruct := xreflect.GenerateStruct(t.TypeDef.Name, rType,
		xreflect.WithPackage(pkg),
		xreflect.WithImports(imps.Packages),
		xreflect.WithSnippetBefore(initSnippet),
		xreflect.WithSnippetAfter(afterSnippet.String()),
	)
	formatted, err := format.Source([]byte(generatedStruct))
	if err != nil {
		return "", err
	}
	return string(formatted), nil
}

func (t *Template) generateRegisterType() string {
	registry := &customTypeRegistry{}
	for _, param := range t.State {
		if !param.In.IsView() {
			continue
		}
		registry.register(param.Schema.SimpleTypeName())
	}
	for _, param := range t.State.FilterByKind(state.KindRequestBody) {
		registry.register(param.Schema.SimpleTypeName())
	}
	return registry.stringify()
}

func (t *Template) generateMapTypeBody() string {
	var initElements []string
	for _, param := range t.State {
		if !param.In.IsView() {
			continue
		}
		initElements = append(initElements, fmt.Sprintf(registerMapEntryTemplate, param.Schema.TypeName(), param.Schema.TypeName()))
	}

	initCode := strings.Join(initElements, "\n")
	return initCode
}
