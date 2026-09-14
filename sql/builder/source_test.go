package builder

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	"github.com/viant/sqlx/io/read/cache"
)

func TestBuilder_TableSource(t *testing.T) {
	view := &data.View{Spec: spec.View{Source: &spec.ViewSource{Table: "users"},

		Selector: &spec.Selector{AllowFields: true}}, Columns: []*data.Column{
		{Name: "ID", Column: "id"},
		{Name: "Name", Column: "full_name"},
	},
	}
	query, err := NewBuilder().Build(context.Background(),
		WithBuilderView(view),
		WithBuilderInput(reflect.ValueOf(struct{}{})),
		WithBuilderProjection([]string{"full_name"}),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}
	if query.SQL != "SELECT full_name FROM users" {
		t.Fatalf("unexpected table-source SQL: %s", query.SQL)
	}
}

func TestBuilder_TableSourceAppliesNullableScalarPolicy(t *testing.T) {
	name := &data.Column{Name: "Name", Column: "full_name"}
	name.ConfigureNullability(true, "string")
	view := &data.View{Spec: spec.View{Source: &spec.ViewSource{Table: "users"}}, Columns: []*data.Column{name}}
	query, err := NewBuilder().Build(context.Background(), WithBuilderView(view), WithBuilderInput(reflect.ValueOf(struct{}{})))
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if query.SQL != "SELECT COALESCE(full_name, '') AS full_name FROM users" {
		t.Fatalf("Build() SQL = %q", query.SQL)
	}
	allowNulls := true
	view.Spec.AllowNulls = &allowNulls
	query, err = NewBuilder().Build(context.Background(), WithBuilderView(view), WithBuilderInput(reflect.ValueOf(struct{}{})))
	if err != nil {
		t.Fatalf("Build() allow nulls error = %v", err)
	}
	if query.SQL != "SELECT full_name FROM users" {
		t.Fatalf("Build() allow nulls SQL = %q", query.SQL)
	}
}

func TestBuilder_CacheSQLPreservesTableSource(t *testing.T) {
	view := &data.View{Spec: spec.View{Source: &spec.ViewSource{Table: "users"}}, Columns: []*data.Column{{Name: "ID", Column: "id"}, {Name: "Name", Column: "full_name"}}}
	query, err := NewBuilder().CacheSQL(context.Background(),
		WithBuilderView(view),
		WithBuilderInput(reflect.ValueOf(struct{}{})),
	)
	if err != nil {
		t.Fatalf("cache build failed: %v", err)
	}
	if query.SQL != "SELECT id, full_name FROM users" {
		t.Fatalf("unexpected table-source cache SQL: %s", query.SQL)
	}
}

func TestBuilder_TableSourceRequiresColumns(t *testing.T) {
	_, err := NewBuilder().Build(context.Background(),
		WithBuilderView(&data.View{Spec: spec.View{Source: &spec.ViewSource{Table: "users"}}}),
		WithBuilderInput(reflect.ValueOf(struct{}{})),
	)
	if err == nil || !strings.Contains(err.Error(), "requires static view columns") {
		t.Fatalf("expected missing-column error, got %v", err)
	}
}

func TestBuilderTableSourceDeclaresRelationNamespace(t *testing.T) {
	view := &data.View{Spec: spec.View{Namespace: "i", Source: &spec.ViewSource{Table: "items"}}, Columns: []*data.Column{{Name: "OrderID", Column: "order_id"}}}
	query, err := NewBuilder().Build(context.Background(), WithBuilderView(view), WithBuilderInput(reflect.ValueOf(struct{}{})))
	if err != nil {
		t.Fatal(err)
	}
	if query.SQL != "SELECT order_id FROM items i" {
		t.Fatalf("table alias lost: %s", query.SQL)
	}
	view.Spec.Source.SQL = "SELECT order_id FROM items physical JOIN orders o ON physical.order_id=o.id AND 1=1"
	query, err = NewBuilder().Build(context.Background(), WithBuilderView(view), WithBuilderInput(reflect.ValueOf(struct{}{})))
	if err != nil {
		t.Fatal(err)
	}
	if strings.HasSuffix(query.SQL, " i") || !strings.Contains(query.SQL, "JOIN orders o") {
		t.Fatalf("authored physical SQL changed: %s", query.SQL)
	}
}

func TestBuilderTableSourceMatcherUsesEmittedNames(t *testing.T) {
	for _, composite := range []bool{false, true} {
		for _, authored := range []bool{false, true} {
			t.Run(fmt.Sprintf("composite=%v/authored=%v", composite, authored), func(t *testing.T) {
				view := &data.View{Spec: spec.View{Namespace: "i", Source: &spec.ViewSource{Table: "items"}}, Columns: []*data.Column{{Name: "OrderId", Column: "order_id"}, {Name: "TenantId", Column: "tenant_id"}}}
				relation := &data.Relation{Of: &data.RelationRef{On: data.Links{{Namespace: "i", Column: "order_id"}}}}
				opts := []BuilderOption{WithBuilderView(view), WithBuilderInput(reflect.ValueOf(struct{}{})), WithBuilderRelation(relation)}
				want := []string{"order_id", "tenant_id"}
				if authored {
					opts = append(opts, WithBuilderSQL("SELECT order_id,tenant_id FROM items i"))
					want = []string{"order_id", "tenant_id"}
				}
				if composite {
					opts = append(opts, WithBuilderCompositeArgs([]string{"i.order_id", "i.tenant_id"}, [][]interface{}{{1, 7}, {2, 8}}))
				} else {
					opts = append(opts, WithBuilderPositionalArgs([]any{1, 2}))
				}
				builder := NewBuilder()
				query, err := builder.Build(context.Background(), opts...)
				if err != nil {
					t.Fatal(err)
				}
				if !strings.Contains(query.SQL, "i.order_id") {
					t.Fatalf("physical filter changed: %s", query.SQL)
				}
				matcher, err := builder.QueryMatcher(context.Background(), query, opts...)
				if err != nil {
					t.Fatal(err)
				}
				cached, err := builder.CacheSQL(context.Background(), opts...)
				if err != nil {
					t.Fatal(err)
				}
				for _, actual := range []*cache.ParmetrizedQuery{matcher, cached} {
					if composite {
						if !reflect.DeepEqual(actual.ByColumns, want) {
							t.Fatalf("matcher names %v want %v", actual.ByColumns, want)
						}
					} else if actual.By != want[0] {
						t.Fatalf("matcher name %s want %s", actual.By, want[0])
					}
				}
			})
		}
	}
}

func TestBuilderTableSourceUsesEmittedRelationNamespace(t *testing.T) {
	view := &data.View{Spec: spec.View{Source: &spec.ViewSource{Table: "items"}}, Columns: []*data.Column{{Name: "OrderID", Column: "order_id"}}}
	relation := &data.Relation{Of: &data.RelationRef{On: data.Links{{Namespace: "i", Column: "order_id"}}}}
	options := []BuilderOption{WithBuilderView(view), WithBuilderInput(reflect.ValueOf(struct{}{})), WithBuilderRelation(relation), WithBuilderPositionalArgs([]any{1})}
	query, err := NewBuilder().Build(context.Background(), options...)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(query.SQL, "FROM items i WHERE i.order_id") {
		t.Fatalf("emitted relation namespace lost: %s", query.SQL)
	}
	view.Spec.Namespace = "conflicting"
	if _, err = NewBuilder().Build(context.Background(), options...); err == nil || !strings.Contains(err.Error(), "conflicting relation namespaces") {
		t.Fatalf("conflicting table alias accepted: %v", err)
	}
}
