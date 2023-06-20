package codegen

import (
	"context"
	"fmt"
	"github.com/viant/datly/config"
	"github.com/viant/xreflect"
	"go/format"
)

//GenerateEntity generate golang entity
func (t *Template) GenerateEntity(ctx context.Context, pkg string, extensionMode bool) (string, error) {
	pkg = t.getPakcage(pkg)
	if err := t.TypeDef.Init(context.Background(), config.Config.LookupType); err != nil {
		return "", err
	}
	rType := t.TypeDef.Schema.Type()

	/*
		TODO
		core.RegisterType(PackageName, "Campaign", reflect.TypeOf(Campaign{}), checksum.GeneratedTime)
		vs
		Types (standalone plugin no dep)

	*/
	generatedStruct := xreflect.GenerateStruct(t.TypeDef.Name, rType,
		xreflect.WithPackage(pkg),
		xreflect.WithImports(t.Imports.Packages),
		xreflect.WithSnippetBefore(""))

	fmt.Printf("SOURCE: %v\n", generatedStruct)
	formatted, err := format.Source([]byte(generatedStruct))
	if err != nil {
		return "", err
	}
	return string(formatted), nil
}

/*
packageName := s.PackageName(aConfig.fileName)

	var extraTypes = map[string]reflect.Type{}

	for i := 0; i < rType.NumField(); i++ {
		field := rType.Field(i)
		if fieldType := extractStruct(field.Type); fieldType != nil && fieldType.PkgPath() == rType.PkgPath() {
			key := field.Type.Name()
			if typeName, ok := field.Tag.Lookup("typeName"); ok && typeName != "" {
				key = typeName
			}
			if key != "" {
				extraTypes[key] = fieldType
			}
		}
	}

	sbBefore := &bytes.Buffer{}
	sbBefore.WriteString(fmt.Sprintf("var PackageName = \"%v\"\n", packageName))

	var imports xreflect.Imports
	if xDatlyModURL == nil {
		sbBefore.WriteString(fmt.Sprintf(`
var %v = map[string]reflect.Type{
		"%v": reflect.TypeOf(%v{}),
}
`, dConfig.TypesName, name, name))

		imports = append(imports, "reflect")
		packageName = "main"
	} else {

		imports = append(imports,
			moduleCoreTypes,
			path.Join(xDatlyModURL.moduleName, checksumDirectory),
			"reflect",
		)
		var items = []string{
			expandRegisterType(name, name, checksumDirectory),
		}
		for k := range extraTypes {
			items = append(items, expandRegisterType(k, k, checksumDirectory))
		}

		sbBefore.WriteString(fmt.Sprintf(`
func init() {
	%s
}
`, strings.Join(items, "\n")))

	}

	sb := &bytes.Buffer{}
	generatedStruct := xreflect.GenerateStruct(name, rType, imports, xreflect.AppendBeforeType(sbBefore.String()), xreflect.PackageName(packageName))
	sb.WriteString(generatedStruct)
	sb.WriteString("\n")
	source, err := goFormat.Source(sb.Bytes())
	if err != nil {
		return "", nil, fmt.Errorf("faield to generate go code: %w, %s", err, sb.Bytes())
	}

	return packageName, source, nil
*/
