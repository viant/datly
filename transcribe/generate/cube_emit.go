package generate

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
)

func (p *Plan) cubeFileText() (string, error) {
	imports := append([]spec.ImportSpec(nil), p.holderImports()...)
	add := func(path, preferred string) string {
		for _, item := range imports {
			if item.Package == path {
				if item.Alias != "" {
					return item.Alias
				}
				return packageAlias(path)
			}
		}
		alias := availableImportAlias(imports, preferred)
		imports = append(imports, spec.ImportSpec{Alias: alias, Package: path})
		return alias
	}
	reportAlias := add("github.com/viant/datly/report", "report")
	handlerAlias := add("github.com/viant/datly/runtime/handler", "rhandler")
	reflectAlias := add("reflect", "reflect")
	componentAlias := add("github.com/viant/xdatly", "xdatly")
	var body, anchors strings.Builder
	for _, cube := range p.Cubes {
		inputName := cube.InputName
		if cube.GenerateInput {
			var sections strings.Builder
			body.WriteString("type " + inputName + " struct {\n")
			for i := 0; i < cube.InputType.NumField(); i++ {
				field := cube.InputType.Field(i)
				expression, err := cubeTypeExpression(field.Type, p.Package, add)
				if err != nil {
					return "", err
				}
				if field.Type.Kind() == reflect.Struct {
					sectionName := cube.Name + field.Name
					var section strings.Builder
					fmt.Fprintf(&section, "type %s struct {\n", sectionName)
					for j := 0; j < field.Type.NumField(); j++ {
						member := field.Type.Field(j)
						memberType, err := cubeTypeExpression(member.Type, p.Package, add)
						if err != nil {
							return "", err
						}
						if member.Tag.Get("setMarker") == "true" {
							markerName := sectionName + "Has"
							markerType, err := cubeTypeExpression(member.Type.Elem(), p.Package, add)
							if err != nil {
								return "", err
							}
							fmt.Fprintf(&sections, "type %s %s\n", markerName, markerType)
							memberType = "*" + markerName
						}
						fmt.Fprintf(&section, "%s %s %s\n", member.Name, memberType, strconv.Quote(string(member.Tag)))
					}
					section.WriteString("}\n")
					sections.WriteString(section.String())
					if _, ok := field.Type.FieldByName("Has"); ok {
						for j := 0; j < field.Type.NumField(); j++ {
							member := field.Type.Field(j)
							if member.Name == "Has" {
								continue
							}
							memberType, err := cubeTypeExpression(member.Type, p.Package, add)
							if err != nil {
								return "", err
							}
							fmt.Fprintf(&sections, "func (v *%s) Set%s(value %s) {v.%s=value;if v.Has==nil{v.Has=&%sHas{}};v.Has.%s=true}\n", sectionName, member.Name, memberType, member.Name, sectionName, member.Name)
						}
					}
					expression = sectionName
				}
				tag := string(field.Tag)
				for _, param := range cube.Definition.Parameters {
					if param != nil && param.Name == field.Name {
						tag = appendStructTag(tag, "parameter", param.Name+",kind=body,in="+param.Source.Name)
						break
					}
				}
				fmt.Fprintf(&body, "%s %s %s\n", field.Name, expression, strconv.Quote(tag))
			}
			body.WriteString("}\n")
			body.WriteString(sections.String())
		} else {
			expression, err := cubeTypeExpression(cube.InputType, p.Package, add)
			if err != nil {
				return "", err
			}
			inputName = expression
		}
		outputName := cube.Name + "Output"
		fmt.Fprintf(&body, "type %s = %s\n", outputName, p.contractType(p.Output))
		holder := cube.Name + "Component"
		sourceRoute := cube.Component.Routes[0]
		tag, err := (dtag.Component{Name: cube.Name, RouteName: sourceRoute.Name, Path: sourceRoute.Path, Method: sourceRoute.Method, Handler: "New" + cube.Name, Description: cube.Component.Description, Internal: sourceRoute.Internal, APIKeyHeader: sourceRoute.APIKeyHeader, APIKeyValue: sourceRoute.APIKeyValue, MCP: sourceRoute.MCP}).StructTag()
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&body, "type %s struct { Contract %s.Component[%s,%s] %s }\n", holder, componentAlias, inputName, outputName, strconv.Quote(tag))
		encoded, err := json.Marshal(cube.Definition)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&body, "func New%s() (%s.TypedHandler,error) { return %s.NewLinkedFacade[%s,%s,%s](%s) }\n", cube.Name, handlerAlias, reportAlias, inputName, outputName, p.HolderName(), strconv.Quote(string(encoded)))
		fmt.Fprintf(&body, "func (%s) DatlyHandler(name string) func() (%s.TypedHandler,error) { if name==%s {return New%s};return nil }\n", holder, handlerAlias, strconv.Quote("New"+cube.Name), cube.Name)
		fmt.Fprintf(&anchors, "var _anchor%s = %s.TypeFor[%s]()\n", holder, reflectAlias, holder)
		fmt.Fprintf(&body, "func %sDatlyType() %s.Type {return %s.TypeFor[%s]()}\nvar %sDatly=new(%s)\nvar %sHandler=%s{}.DatlyHandler\n", cube.Name, reflectAlias, reflectAlias, holder, cube.Name, holder, cube.Name, holder)
	}
	var result strings.Builder
	fmt.Fprintf(&result, "package %s\n\nimport (\n", p.PackageName())
	// Imports inherited from the holder are needed only when its output contract
	// is external; avoid accidentally importing the source input's package here.
	used := body.String()
	for _, item := range imports {
		alias := item.Alias
		if alias == "" {
			alias = packageAlias(item.Package)
		}
		if strings.Contains(used, alias+".") {
			fmt.Fprintf(&result, "%s %q\n", item.Alias, item.Package)
		}
	}
	result.WriteString(")\n\nfunc init() {}\n\n")
	result.WriteString(anchors.String())
	result.WriteString("\n")
	result.WriteString(used)
	return result.String(), nil
}

func cubeTypeExpression(typ reflect.Type, localPackage string, add func(string, string) string) (string, error) {
	if typ == nil {
		return "", fmt.Errorf("cube field type is required")
	}
	if typ.Name() != "" && typ.PkgPath() != "" {
		if typ.PkgPath() == localPackage {
			return typ.Name(), nil
		}
		return add(typ.PkgPath(), packageAlias(typ.PkgPath())) + "." + typ.Name(), nil
	}
	switch typ.Kind() {
	case reflect.Pointer, reflect.Slice, reflect.Array:
		element, err := cubeTypeExpression(typ.Elem(), localPackage, add)
		if err != nil {
			return "", err
		}
		switch typ.Kind() {
		case reflect.Pointer:
			return "*" + element, nil
		case reflect.Slice:
			return "[]" + element, nil
		default:
			return fmt.Sprintf("[%d]%s", typ.Len(), element), nil
		}
	case reflect.Map:
		key, err := cubeTypeExpression(typ.Key(), localPackage, add)
		if err != nil {
			return "", err
		}
		value, err := cubeTypeExpression(typ.Elem(), localPackage, add)
		return "map[" + key + "]" + value, err
	case reflect.Struct:
		var b strings.Builder
		b.WriteString("struct {\n")
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			if !field.IsExported() {
				return "", fmt.Errorf("cube field %s is not exported", field.Name)
			}
			expression, err := cubeTypeExpression(field.Type, localPackage, add)
			if err != nil {
				return "", err
			}
			if !field.Anonymous {
				b.WriteString(field.Name + " ")
			}
			b.WriteString(expression)
			if field.Tag != "" {
				b.WriteString(" " + strconv.Quote(string(field.Tag)))
			}
			b.WriteString("\n")
		}
		b.WriteString("}")
		return b.String(), nil
	case reflect.Interface:
		if typ.NumMethod() != 0 {
			return "", fmt.Errorf("cube field has unsupported interface %s", typ)
		}
		return "any", nil
	case reflect.Func, reflect.Chan, reflect.UnsafePointer:
		return "", fmt.Errorf("cube field has unsupported type %s", typ)
	default:
		return typ.String(), nil
	}
}
