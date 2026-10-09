package tag

import (
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
)

func TestProtectedFlushTablesTagRoundTripAndIsolation(t *testing.T) {
	source := &spec.Settings{ProtectedFlushTables: []string{"records", "records/attributes"}}
	settings := SettingsFromSpec(source)
	source.ProtectedFlushTables[0] = "changed"
	raw, err := settings.StructTag()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := ParseSettings(reflect.StructTag(raw))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded.ProtectedFlushTables, []string{"records", "records/attributes"}) {
		t.Fatalf("table authority lost: %s", raw)
	}
	var target spec.Settings
	decoded.Apply(&target)
	decoded.ProtectedFlushTables[0] = "changed"
	if target.ProtectedFlushTables[0] != "records" {
		t.Fatal("Apply aliases decoded authority")
	}
	ordinary, err := ParseSettings("")
	if err != nil {
		t.Fatal(err)
	}
	ordinary.Apply(&target)
	if target.ProtectedFlushTables != nil {
		t.Fatal("ordinary component retained previous authority")
	}
}

func TestProtectedFlushTablesTagRejectsInvalidAuthority(t *testing.T) {
	for _, raw := range []reflect.StructTag{
		`protectedFlushTables:""`, `protectedFlushTables:"null"`, `protectedFlushTables:"[]"`,
		`protectedFlushTables:"[1]"`, `protectedFlushTables:"\"records\""`,
		`protectedFlushTables:"[\"records\",\"records\"]"`,
		`protectedFlushTables:"[\"Records\"]"`, `protectedFlushTables:"[\"records/*\"]"`,
	} {
		if _, err := ParseSettings(raw); err == nil {
			t.Fatalf("accepted invalid table tag %s", raw)
		}
	}
	for _, tables := range [][]string{{}, {""}, {"Records"}, {"records", "records"}} {
		if _, err := (Settings{ProtectedFlushTables: tables}).StructTag(); err == nil {
			t.Fatalf("emitted invalid table authority %#v", tables)
		}
	}
}
