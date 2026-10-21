package view

import (
	"reflect"
	"strings"
)

func NormalizeRelationSQLTagURIsWithViews(typeDef string, views []*View) string {
	if strings.TrimSpace(typeDef) == "" || len(views) == 0 {
		return typeDef
	}
	uris := map[string]string{}
	for _, item := range views {
		if item == nil || item.Template == nil {
			continue
		}
		uri := strings.TrimSpace(item.Template.SourceURL)
		if uri == "" {
			continue
		}
		if name := strings.TrimSpace(item.Name); name != "" {
			uris[name] = uri
		}
		if item.Schema != nil {
			if name := strings.TrimSpace(item.Schema.Name); strings.HasSuffix(name, "View") {
				base := strings.TrimSuffix(name, "View")
				if base != "" {
					uris[base] = uri
				}
			}
		}
	}
	for holder, uri := range uris {
		typeDef = replaceRelationFieldSQLTagWithURI(typeDef, holder, uri)
	}
	return typeDef
}

func NormalizeRelationStructFields(fields []reflect.StructField, parent *View) []reflect.StructField {
	if len(fields) == 0 || parent == nil {
		return fields
	}
	result := make([]reflect.StructField, len(fields))
	copy(result, fields)
	uris := collectRelationSQLURIs(map[string]string{}, parent)
	for i := range result {
		uri := uris[result[i].Name]
		if uri == "" {
			continue
		}
		tag := replaceRelationFieldSQLTagWithURI(result[i].Name+" "+string(result[i].Tag), result[i].Name, uri)
		tag = strings.TrimPrefix(tag, result[i].Name+" ")
		result[i].Tag = reflect.StructTag(tag)
	}
	return result
}

func collectRelationSQLURIs(dest map[string]string, parent *View) map[string]string {
	for _, rel := range parent.With {
		if rel == nil || rel.Of == nil {
			continue
		}
		child := rel.Of.View
		if child.Template != nil {
			uri := strings.TrimSpace(child.Template.SourceURL)
			holder := strings.TrimSpace(rel.Holder)
			if holder == "" {
				holder = strings.TrimSpace(child.Name)
			}
			if holder != "" && uri != "" {
				dest[holder] = uri
			}
		}
		collectRelationSQLURIs(dest, &child)
	}
	return dest
}

func replaceRelationFieldSQLTagWithURI(typeDef string, fieldName string, uri string) string {
	fieldName = strings.TrimSpace(fieldName)
	uri = strings.TrimSpace(uri)
	if strings.TrimSpace(typeDef) == "" || fieldName == "" || uri == "" {
		return typeDef
	}
	search := fieldName + " "
	start := 0
	for {
		idx := strings.Index(typeDef[start:], search)
		if idx == -1 {
			return typeDef
		}
		idx += start
		sqlIdx := strings.Index(typeDef[idx:], `sql:"`)
		if sqlIdx == -1 {
			return typeDef
		}
		sqlIdx += idx
		valueStart := sqlIdx + len(`sql:"`)
		valueEnd := findTagValueEnd(typeDef, valueStart)
		if valueEnd == -1 {
			return typeDef
		}
		return typeDef[:valueStart] + "uri=" + uri + typeDef[valueEnd:]
	}
}

func findTagValueEnd(tagValue string, start int) int {
	escaped := false
	for i := start; i < len(tagValue); i++ {
		switch {
		case escaped:
			escaped = false
		case tagValue[i] == '\\':
			escaped = true
		case tagValue[i] == '"':
			return i
		}
	}
	return -1
}
