package spec

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestProtectedFlushTablesCanonicalMetadata(t *testing.T) {
	want := []string{"records", "records/attributes", "catalog.records"}
	original := &Component{Settings: &Settings{ProtectedFlushTables: append([]string(nil), want...)}}
	if err := original.Settings.ValidateProtectedFlushTables(); err != nil {
		t.Fatal(err)
	}
	if original.Settings.IsZero() {
		t.Fatal("configured table authority is zero")
	}
	cloned := original.Clone()
	original.Settings.ProtectedFlushTables[0] = "changed"
	if !reflect.DeepEqual(cloned.Settings.ProtectedFlushTables, want) {
		t.Fatal("component clone aliases table authority")
	}
	data, err := json.Marshal(cloned)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Component
	if err = json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Settings == nil || !reflect.DeepEqual(decoded.Settings.ProtectedFlushTables, want) {
		t.Fatalf("JSON lost table authority: %s", data)
	}
	if err = decoded.Settings.ValidateProtectedFlushTables(); err != nil {
		t.Fatal(err)
	}
	if (&Settings{}).Clone().ProtectedFlushTables != nil || !(&Settings{}).IsZero() {
		t.Fatal("ordinary component gained table authority")
	}
	var absent *Settings
	if err = absent.ValidateProtectedFlushTables(); err != nil {
		t.Fatal(err)
	}
}

func TestProtectedFlushTablesRejectsInvalidCanonicalAuthority(t *testing.T) {
	for _, tables := range [][]string{
		{}, {""}, {"Records"}, {" records"}, {"records "}, {"records", "records"},
		{"*"}, {"records/*"}, {"records%"}, {"records."}, {".records"}, {"records//attributes"},
		{"records/"}, {"records..attributes"}, {"records\\attributes"}, {"records;delete"},
		{"`records`"}, {"records where id=1"}, {"1records"}, {"records,$Input.Table"},
	} {
		settings := &Settings{ProtectedFlushTables: tables}
		if err := settings.ValidateProtectedFlushTables(); err == nil {
			t.Fatalf("accepted invalid exact tables: %#v", tables)
		}
		cloned := settings.Clone()
		if cloned.ProtectedFlushTables == nil || cloned.IsZero() {
			t.Fatal("clone erased explicit invalid authority before validation")
		}
	}
}
