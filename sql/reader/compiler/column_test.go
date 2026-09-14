package compiler

import (
	"reflect"
	"testing"

	"github.com/viant/datly/data"
)

func TestColumnsFromType(t *testing.T) {
	type Keys struct {
		TenantID int `sqlx:"tenant_id"`
	}
	type row struct {
		Keys
		ID       int                `sqlx:"id"`
		Name     string             `source:"full_name" groupable:"true" codec:"Upper"`
		Ignored  string             `sqlx:"-"`
		Children []struct{ ID int } `on:"ID=id"`
	}
	columns, err := columnsFromType(reflect.TypeOf(row{}))
	if err != nil {
		t.Fatalf("columnsFromType failed: %v", err)
	}
	if len(columns) != 3 {
		t.Fatalf("expected three scalar columns, got %+v", columns)
	}
	if columns[0].Name != "TenantID" || columns[0].Column != "tenant_id" || columns[0].SelectExpression(false) != "tenant_id AS TenantID" {
		t.Fatalf("unexpected embedded column: %+v", columns[0])
	}
	if columns[1].Name != "ID" || columns[1].Column != "id" || columns[1].SelectExpression(false) != "id" {
		t.Fatalf("unexpected ID column: %+v", columns[1])
	}
	if columns[2].Name != "Name" || columns[2].Column != "full_name" || !columns[2].Groupable {
		t.Fatalf("unexpected source-tag column: %+v", columns[2])
	}
	if columns[2].Codec == nil || columns[2].Codec.Body != "Upper" {
		t.Fatalf("expected declarative codec metadata: %+v", columns[2])
	}
}

func TestColumnsFromTypeAndCanonicalMetadataResolveNullFallback(t *testing.T) {
	type row struct {
		Name  *string `sqlx:"name"`
		Count int     `sqlx:"count"`
	}
	typed, err := columnsFromType(reflect.TypeOf(row{}))
	if err != nil {
		t.Fatalf("columnsFromType() error = %v", err)
	}
	if !typed[0].Nullable || typed[0].NullFallback != "" || typed[1].Nullable || typed[1].NullFallback != "" {
		t.Fatalf("typed columns = %+v", typed)
	}
	canonicalName := &data.Column{Name: "Name", Column: "name", Nullable: true, NullFallback: "''", Groupable: true}
	reconciled := reconcileColumns([]*data.Column{{Name: "Name", Column: "name"}}, []*data.Column{canonicalName})
	if !reconciled[0].Nullable || reconciled[0].NullFallback != "''" || !reconciled[0].Groupable {
		t.Fatalf("reconciled column = %+v", reconciled[0])
	}
	unmatched := reconcileColumns(
		[]*data.Column{{Name: "Count", Column: "count"}},
		[]*data.Column{{Nullable: true, NullFallback: "0"}},
	)
	if unmatched[0].Nullable || unmatched[0].NullFallback != "" {
		t.Fatalf("blank canonical identity matched typed column: %+v", unmatched[0])
	}
}

func TestColumnsFromTypeVisibility(t *testing.T) {
	type Embedded struct {
		Name string `sqlx:"embedded_name"`
	}
	type shadowed struct {
		Embedded
		Name string `sqlx:"outer_name"`
	}
	type Left struct{ Name string }
	type Right struct{ Name string }
	type ambiguous struct {
		Left
		Right
	}
	type embedded struct {
		ID int `sqlx:"embedded_id"`
	}
	type promoted struct{ embedded }
	type transient struct {
		Embedded
		Name string `sqlx:"-"`
	}
	tests := []struct {
		name       string
		rowType    reflect.Type
		wantLength int
		wantName   string
		wantColumn string
	}{
		{name: "outer shadows embedded", rowType: reflect.TypeOf(shadowed{}), wantLength: 1, wantName: "Name", wantColumn: "outer_name"},
		{name: "ambiguous embedded excluded", rowType: reflect.TypeOf(ambiguous{})},
		{name: "unexported embedding promotes exported field", rowType: reflect.TypeOf(promoted{}), wantLength: 1, wantName: "ID", wantColumn: "embedded_id"},
		{name: "transient outer suppresses promoted field", rowType: reflect.TypeOf(transient{})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			columns, err := columnsFromType(test.rowType)
			if err != nil {
				t.Fatalf("columnsFromType() error = %v", err)
			}
			if len(columns) != test.wantLength {
				t.Fatalf("expected %d columns, got %+v", test.wantLength, columns)
			}
			if test.wantLength > 0 && (columns[0].Name != test.wantName || columns[0].Column != test.wantColumn) {
				t.Fatalf("unexpected columns: %+v", columns)
			}
		})
	}
}
