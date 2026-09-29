package holder

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestComponentHolder_CoversRequiredSemantics(t *testing.T) {
	required := []string{
		"route.uri", "route.method", "route.service", "route.viewRef",
		"component.sourceURL", "component.settings", "component.dependencies",
		"io.typeName", "io.parameters", "io.exclude", "io.caseFormat",
		"param.name", "param.kind", "param.in", "param.required",
		"param.dataType", "param.cardinality", "param.tag", "param.tagMeta",
		"param.codec.name", "param.codec.args", "param.predicates",
		"param.errorStatusCode", "param.cacheable", "param.scope", "param.connector", "param.limit", "param.value",
		"view.name", "view.mode", "view.table", "view.connector", "view.partitioner", "view.partitionedConcurrency", "view.relationalConcurrency", "view.sourceURL", "view.selector", "view.with",
		"selector.namespace", "selector.limit", "selector.criteria", "selector.projection", "selector.orderBy", "selector.offset",
		"relation.name", "relation.holder", "relation.cardinality", "relation.ref", "relation.on",
		"join.namespace", "join.column", "join.field",
		"deps.with", "deps.connectors", "deps.constants", "deps.substitutions",
	}

	got := collectShapeTags(reflect.TypeOf(ComponentHolder{}), map[string]struct{}{}, map[reflect.Type]bool{})
	for _, item := range required {
		_, ok := got[item]
		require.Truef(t, ok, "missing semantic tag %q in holder model", item)
	}
}

func collectShapeTags(t reflect.Type, acc map[string]struct{}, visited map[reflect.Type]bool) map[string]struct{} {
	for t.Kind() == reflect.Ptr || t.Kind() == reflect.Slice || t.Kind() == reflect.Array {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return acc
	}
	if visited[t] {
		return acc
	}
	visited[t] = true
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if tag := field.Tag.Get("shape"); tag != "" {
			acc[tag] = struct{}{}
		}
		collectShapeTags(field.Type, acc, visited)
	}
	return acc
}
