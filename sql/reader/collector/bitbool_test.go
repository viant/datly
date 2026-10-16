package collector

import (
	"database/sql"
	"reflect"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/sqlx/types"
)

func TestUnmappedMySQLBitPreservesScanner(t *testing.T) {
	view := newTestView(&data.View{}, reflect.TypeFor[parentPlaceholderRow]())
	var rows []parentPlaceholderRow
	collector := NewCollector(view, &rows, false)
	allocate := collector.Resolve(testColumn{name: "ARCHIVED", scanType: reflect.TypeFor[types.BitBool]()})
	for _, value := range []byte{0, 1} {
		collector.NewItem()()
		destination := allocate(nil)
		scanner, ok := destination.(sql.Scanner)
		if !ok {
			t.Fatalf("unmapped BIT lost its SQL scanner: %T", destination)
		}
		if err := scanner.Scan([]byte{value}); err != nil {
			t.Fatal(err)
		}
		if actual := bool(*destination.(*types.BitBool)); actual != (value == 1) {
			t.Fatalf("BIT %d decoded as %v", value, actual)
		}
	}
	collector.Fetched()
	for index, expected := range []types.BitBool{false, true} {
		actual, ok := collector.sqlKeyAt("ARCHIVED", index)
		if !ok || actual != expected {
			t.Fatalf("unmapped row %d lost its BIT value: %v", index, actual)
		}
	}
}

func TestUnmappedNativeBoolRemainsBool(t *testing.T) {
	view := newTestView(&data.View{}, reflect.TypeFor[parentPlaceholderRow]())
	var rows []parentPlaceholderRow
	collector := NewCollector(view, &rows, false)
	collector.NewItem()()
	destination := collector.Resolve(testColumn{name: "flag", scanType: reflect.TypeFor[bool]()})(nil)
	if _, ok := destination.(*bool); !ok {
		t.Fatalf("native bool destination changed: %T", destination)
	}
}
