package router

import (
	"math"
	"reflect"
	"strings"
)

func GenerateGoStruct(name string, structType reflect.Type) string {
	structBuilder := &strings.Builder{}
	structBuilder.WriteString("type ")
	structBuilder.WriteString(name)
	structBuilder.WriteString(" ")
	importsBuilder := &strings.Builder{}

	buildGoType(structBuilder, importsBuilder, structType)

	return build(importsBuilder, structBuilder)
}

func build(importsBuilder *strings.Builder, structBuilder *strings.Builder) string {
	result := strings.Builder{}
	result.WriteString("package generated \n\n")

	if importsBuilder.Len() > 0 {
		result.WriteString("import (\n")
		result.WriteString(importsBuilder.String())
		result.WriteString(")\n\n")
	}

	result.WriteString(structBuilder.String())
	return result.String()
}

func buildGoType(mainBuilder *strings.Builder, importsBuilder *strings.Builder, structType reflect.Type) {
	structType = appendElem(mainBuilder, structType)

	switch structType.Kind() {
	case reflect.Struct:
		numField := structType.NumField()

		maxNameLen := math.MinInt
		for i := 0; i < numField; i++ {
			fieldNameLen := len(structType.Field(i).Name)
			if fieldNameLen > maxNameLen {
				maxNameLen = fieldNameLen
			}
		}

		var structBuilders []*strings.Builder
		mainBuilder.WriteString("{")
		for i := 0; i < numField; i++ {
			mainBuilder.WriteString("\n    ")
			aField := structType.Field(i)
			mainBuilder.WriteString(aField.Name)
			mainBuilder.WriteByte(' ')
			for j := 0; j < maxNameLen-len(aField.Name); j++ {
				mainBuilder.WriteByte(' ')
			}

			actualType := appendElem(mainBuilder, aField.Type)
			mainBuilder.WriteByte(' ')

			if actualType.Kind() == reflect.Struct {
				if actualType.Name() == "" {
					mainBuilder.WriteString(aField.Name)
					nestedStruct := &strings.Builder{}
					structBuilders = append(structBuilders, nestedStruct)

					nestedStruct.WriteString("type ")
					nestedStruct.WriteString(aField.Name)
					nestedStruct.WriteByte(' ')
					buildGoType(nestedStruct, importsBuilder, actualType)
				} else {
					mainBuilder.WriteString(actualType.Name())
					importsBuilder.WriteString("  ")
					importsBuilder.WriteString(actualType.PkgPath())
					importsBuilder.WriteByte('\n')
				}
			} else {
				buildGoType(mainBuilder, importsBuilder, aField.Type)
			}

			if aField.Tag != "" {
				mainBuilder.WriteByte(' ')
				mainBuilder.WriteByte('`')
				mainBuilder.WriteString(string(aField.Tag))
				mainBuilder.WriteByte('`')
			}
		}
		mainBuilder.WriteString("\n}")

		for _, builder := range structBuilders {
			mainBuilder.WriteString("\n\n")
			mainBuilder.WriteString(builder.String())
		}

	default:
		mainBuilder.WriteString(structType.String())
	}
}

func appendElem(sb *strings.Builder, rType reflect.Type) reflect.Type {
	for rType.Kind() == reflect.Ptr || rType.Kind() == reflect.Slice {
		switch rType.Kind() {
		case reflect.Ptr:
			sb.WriteByte('*')
		case reflect.Slice:
			sb.WriteString("[]")
		}

		rType = rType.Elem()
	}

	return rType
}
