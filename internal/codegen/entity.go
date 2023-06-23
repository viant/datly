package codegen

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/viant/datly/config"
	"github.com/viant/datly/internal/plugin"
	"github.com/viant/datly/view"
	"github.com/viant/xreflect"
	"go/format"
	"strings"
)

const (
	registerTypeTemplate     = `	core.RegisterType(PackageName, "%v", reflect.TypeOf(%v{}), checksum.GeneratedTime)`
	registerMapEntryTemplate = `	"%v": reflect.TypeOf(%v{}),`
)

//go:embed tmpl/entity.gox
var entityTemplate string

//GenerateEntity generate golang entity
func (t *Template) GenerateEntity(ctx context.Context, pkg string, info *plugin.Info) (string, error) {
	pkg = info.Package(pkg)
	if err := t.TypeDef.Init(context.Background(), config.Config.LookupType); err != nil {
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
	initSnippet = strings.Replace(initSnippet, "$Package", pkg, 1)
	initSnippet = strings.Replace(initSnippet, "$GlobalDeclaration", globalDeclaration, 1)

	generatedStruct := xreflect.GenerateStruct(t.TypeDef.Name, rType,
		xreflect.WithPackage(pkg),
		xreflect.WithImports(imps.Packages),
		xreflect.WithSnippetBefore(initSnippet))
	formatted, err := format.Source([]byte(generatedStruct))
	if err != nil {
		return "", err
	}
	return string(formatted), nil
}

func (t *Template) generateRegisterType() string {
	var initElements []string
	for _, param := range t.State {
		if param.In.Kind != view.KindDataView {
			continue
		}
		initElements = append(initElements, fmt.Sprintf(registerTypeTemplate, param.Schema.DataType, param.Schema.DataType))
	}

	initCode := strings.Join(initElements, "\n")
	return initCode
}

func (t *Template) generateMapTypeBody() string {
	var initElements []string
	for _, param := range t.State {
		if param.In.Kind != view.KindDataView {
			continue
		}
		initElements = append(initElements, fmt.Sprintf(registerMapEntryTemplate, param.Schema.DataType, param.Schema.DataType))
	}

	initCode := strings.Join(initElements, "\n")
	return initCode
}
