package dql

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
)

func TestProtectedFlushTablesDQLRoundTrip(t *testing.T) {
	source := `#package('example.com/records')
#setting($_ = $route('/records','POST'))
#setting($_ = $protected_flush_tables('Records','Records/Attributes','Catalog.Records'))
SELECT id FROM records`
	component, err := parseComponentSource("example.com/records", "Records", source)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"records", "records/attributes", "catalog.records"}
	if component.Settings == nil || component.Settings.IsZero() || !reflect.DeepEqual(component.Settings.ProtectedFlushTables, want) {
		t.Fatalf("lost normalized component authority: %+v", component.Settings)
	}
	raw, err := dtag.SettingsFromSpec(component.Settings).StructTag()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := dtag.ParseSettings(reflect.StructTag(raw))
	if err != nil {
		t.Fatal(err)
	}
	var restored spec.Settings
	decoded.Apply(&restored)
	if !reflect.DeepEqual(restored.ProtectedFlushTables, want) {
		t.Fatal("package settings lost exact table authority")
	}
	export := (Serializer{}).Export(component, "")
	if !export.Supported || !strings.Contains(export.Source, "$protected_flush_tables(") {
		t.Fatalf("serializer discarded table authority: %+v", export)
	}
	rebuilt, err := parseComponentSource("example.com/records", "Records", export.Source)
	if err != nil || rebuilt.Settings == nil || !reflect.DeepEqual(rebuilt.Settings.ProtectedFlushTables, want) {
		t.Fatalf("reconstructed metadata: %+v error=%v", rebuilt, err)
	}
	retained := (Serializer{}).Export(component, source)
	if !retained.Original || !retained.Supported || retained.Source != source {
		t.Fatal("retained authorship changed")
	}
	ordinary, err := parseComponentSource("example.com/records", "Child", "#setting($_ = $route('/child','POST'))\nSELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	if ordinary.Settings != nil && ordinary.Settings.ProtectedFlushTables != nil {
		t.Fatal("another component acquired authored table authority")
	}
}

func TestProtectedFlushTablesDQLRejectsInvalidAuthorship(t *testing.T) {
	for _, body := range []string{
		`$protected_flush_tables()`, `$protected_flush_tables('')`, `$protected_flush_tables(records)`,
		`$protected_flush_tables($Input.Table)`, `$protected_flush_tables(true)`, `$protected_flush_tables(7)`,
		`$protected_flush_tables('records','RECORDS')`, `$protected_flush_tables('records/*')`,
		`$protected_flush_tables(' records')`, `$protected_flush_tables('records ')`,
		`$protected_flush_tables('records;DELETE')`, `$protected_flush_tables('records..attributes')`,
		`$protected_flush_tables('K')`, `$protected_flush_tables('records/Keys')`, `$protected_flush_tables('récords')`,
		`$protected_flush_tables('records').When(false)`, `$protected_flush_tables('records').Optional()`,
		`$protected_flush_tables('records')garbage`,
		"$protected_flush_tables('records'))\n#setting($_ = $protected_flush_tables('records')",
		"$protected_flush_tables('records'))\n#setting($_ = $protected_flush_tables('other_records')",
	} {
		_, err := parseComponentSource("example.com/records", "Records", "#setting($_ = "+body+")\nSELECT 1")
		if err == nil || !strings.Contains(err.Error(), "protected_flush_tables") {
			t.Fatalf("accepted or imprecisely rejected %s: %v", body, err)
		}
	}
	for _, body := range []string{
		`$protected_flush_tables('records').When(false)`, `$protected_flush_tables('records')garbage`,
	} {
		if _, err := parseComponentSettings([]directiveBlock{{kind: directiveKindSetting, body: "$_ = " + body}}); err == nil {
			t.Fatalf("parsed setting accepted invalid tail %s", body)
		}
	}
	invalid := &spec.Component{Routes: []*spec.Route{{Method: "POST", Path: "/records"}}, RootView: &spec.View{Source: &spec.ViewSource{SQL: "SELECT 1"}}, Settings: &spec.Settings{ProtectedFlushTables: []string{}}}
	if exported := (Serializer{}).Export(invalid, ""); exported.Supported {
		t.Fatal("serializer dropped invalid empty authority")
	}
}

func TestProtectedFlushTablesSpecProjectionIsIsolated(t *testing.T) {
	parsed, err := parseComponentSettings([]directiveBlock{{kind: directiveKindSetting, body: "$_ = $protected_flush_tables('records')"}})
	if err != nil {
		t.Fatal(err)
	}
	projected := toSpecSettings(parsed)
	parsed.ProtectedFlushTables[0] = "changed"
	if projected == nil || projected.ProtectedFlushTables[0] != "records" {
		t.Fatal("spec projection aliases parser metadata")
	}
}
