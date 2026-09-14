package fragment

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/viant/sqlx/metadata/info"
	xshape "github.com/viant/x/shape"
)

type tupleKey struct {
	Tenant  int  `sqlx:"tenant_id"`
	ID      *int `sqlx:"id"`
	Enabled bool `sqlx:"enabled"`
}

type EmbeddedTupleKey struct {
	Tenant int `sqlx:"tenant_id"`
	ID     int `sqlx:"id"`
}

func TestCompositeInUsesEmbeddedPointerKeyValues(t *testing.T) {
	type key struct {
		Ignored int `sqlx:"-"`
		*EmbeddedTupleKey
	}
	bindings := &Bindings{}
	_, err := New(bindings).CompositeIn("r", []key{{Ignored: 99, EmbeddedTupleKey: &EmbeddedTupleKey{Tenant: 2, ID: 7}}})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(bindings.Args(), []any{int64(2), int64(7)}) {
		t.Fatalf("embedded tuple arguments=%#v", bindings.Args())
	}
}

func TestCompositeInPreservesFixedTupleValues(t *testing.T) {
	zero, id := 0, 7
	for _, test := range []struct {
		name    string
		rows    any
		dialect *info.Dialect
		wantSQL string
	}{
		{"values", []tupleKey{{Tenant: 0, ID: &zero}, {Tenant: 2, ID: &id, Enabled: true}}, nil, "(r.tenant_id, r.id, r.enabled) IN ((?, ?, ?), (?, ?, ?))"},
		{"pointers", []*tupleKey{{Tenant: 0, ID: &zero}, {Tenant: 2, ID: &id, Enabled: true}}, nil, "(r.tenant_id, r.id, r.enabled) IN ((?, ?, ?), (?, ?, ?))"},
		{"dialect", []tupleKey{{Tenant: 0, ID: &zero}, {Tenant: 2, ID: &id, Enabled: true}}, &info.Dialect{CompositeInRenderer: func(columns []string, rows int) string { return fmt.Sprintf("native(%s,%d)", columns[0], rows) }}, "native(r.tenant_id,2)"},
	} {
		t.Run(test.name, func(t *testing.T) {
			bindings := &Bindings{}
			criteria := New(bindings).WithDialect(test.dialect)
			actual, err := criteria.CompositeIn("r", test.rows)
			if err != nil || actual != test.wantSQL {
				t.Fatalf("SQL=%q err=%v", actual, err)
			}
			if !reflect.DeepEqual(bindings.Args(), []any{int64(0), int64(0), false, int64(2), int64(7), true}) {
				t.Fatalf("tuple arguments=%#v", bindings.Args())
			}
		})
	}
	bindings := &Bindings{}
	if _, err := New(bindings).CompositeIn("", []tupleKey{{ID: &id}}); err != nil {
		t.Fatal(err)
	}
	id = 99
	if bindings.Args()[1] != int64(7) {
		t.Fatal("pointer key binding aliases mutable source")
	}
}

func TestCompositeInRejectsMalformedRowsAtomically(t *testing.T) {
	for _, rows := range []any{1, []int{1}, []any{tupleKey{}}, []*tupleKey{{Tenant: 1}, nil}, []struct {
		Bad chan int `sqlx:"bad"`
	}{{Bad: make(chan int)}}} {
		bindings := &Bindings{}
		bindings.Append("prior")
		if _, err := New(bindings).CompositeIn("r", rows); err == nil {
			t.Fatalf("accepted malformed tuple rows %T", rows)
		}
		if !reflect.DeepEqual(bindings.Args(), []any{"prior"}) {
			t.Fatal("failed tuple conversion published partial args")
		}
	}
	for _, rows := range []any{nil, []tupleKey{}, ([]*tupleKey)(nil)} {
		bindings := &Bindings{}
		sql, err := New(bindings).CompositeIn("", rows)
		if err != nil || sql != "1 = 0" || len(bindings.Args()) != 0 {
			t.Fatalf("empty tuple=%q err=%v", sql, err)
		}
	}
}

func TestCompositeInHasNoPresenceBitmaskWidthLimit(t *testing.T) {
	fields := make([]xshape.RuntimeField, 70)
	for i := range fields {
		fields[i] = xshape.RuntimeField{Name: fmt.Sprintf("Key%d", i), Type: reflect.TypeFor[int](), Tag: reflect.StructTag(fmt.Sprintf(`sqlx:"key%d"`, i))}
	}
	typ, err := (xshape.Runtime{}).Struct(fields)
	if err != nil {
		t.Fatal(err)
	}
	rows := reflect.MakeSlice(reflect.SliceOf(typ), 1, 1)
	bindings := &Bindings{}
	if _, err := New(bindings).CompositeIn("", rows.Interface()); err != nil {
		t.Fatal(err)
	}
	if len(bindings.Args()) != 70 {
		t.Fatalf("arguments=%d", len(bindings.Args()))
	}
}

func TestDialectRenderingContextIsScoped(t *testing.T) {
	dialect := &info.Dialect{MaxPlaceholders: 7}
	base := context.Background()
	scoped := WithDialect(base, dialect)
	if Dialect(base) != nil || Dialect(scoped) != dialect || Dialect(WithDialect(scoped, nil)) != nil {
		t.Fatal("dialect metadata leaked across contexts")
	}
}
