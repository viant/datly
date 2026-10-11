package writer

import (
	"github.com/viant/datly/constant"
	"github.com/viant/datly/spec"
	"testing"
)

func TestWriterFullTableConstantUsesImmutableInstance(t *testing.T) {
	authored := &spec.Component{Settings: &spec.Settings{Const: map[string]string{"Table": "default_records"}}}
	for _, target := range []string{"first_records", "second_records"} {
		instance, err := constant.New(map[string]string{"Table": target})
		if err != nil {
			t.Fatal(err)
		}
		actual, err := resolvedWriterTable(instance.Apply(authored), "$Unsafe.Table")
		if err != nil || actual != target {
			t.Fatalf("instance target %q resolved to %q: %v", target, actual, err)
		}
	}
	if authored.Settings.Const["Table"] != "default_records" {
		t.Fatal("instance modified authored target")
	}
	actual, err := resolvedWriterTable(authored, "$Unsafe.Table")
	if err != nil || actual != "default_records" {
		t.Fatalf("authored default changed: %q %v", actual, err)
	}
	for _, target := range []string{"", "records; DELETE FROM records", "records r"} {
		instance, _ := constant.New(map[string]string{"Table": target})
		if _, err := resolvedWriterTable(instance.Apply(authored), "$Unsafe.Table"); err == nil {
			t.Fatalf("invalid target admitted: %q", target)
		}
	}
	requestBound := &spec.Component{Settings: authored.Settings, Parameters: []*spec.Parameter{{Name: "Table", Source: spec.BindSource{Kind: "query", Name: "table"}}}}
	if _, err := resolvedWriterTable(requestBound, "$Unsafe.Table"); err == nil {
		t.Fatal("request-bound target acquired immutable constant authority")
	}
}
