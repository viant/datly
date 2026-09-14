package tag

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
)

func TestResolveParamFieldUsesBindlyAliasAndSource(t *testing.T) {
	type input struct {
		Tenant  int `parameter:"TenantID,kind=query,in=tenantID"`
		Account int `parameter:"ExternalAccount,kind=query,in=accountID"`
	}
	tests := []struct {
		name  string
		param *spec.Parameter
		want  string
	}{
		{
			name:  "logical alias",
			param: &spec.Parameter{Name: "TenantID", Source: spec.BindSource{Kind: "query", Name: "tenantID"}},
			want:  "Tenant",
		},
		{
			name:  "source location",
			param: &spec.Parameter{Name: "AccountID", Source: spec.BindSource{Kind: "query", Name: "accountID"}},
			want:  "Account",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			index, err := NewBindingIndex(reflect.TypeOf(input{}))
			if err != nil {
				t.Fatalf("NewBindingIndex() error = %v", err)
			}
			field, ok, err := index.Resolve(test.param)
			if err != nil {
				t.Fatalf("Resolve() error = %v", err)
			}
			if !ok || field.Name != test.want {
				t.Fatalf("Resolve() = %s, %v", field.Name, ok)
			}
		})
	}
}

func TestResolveParamFieldDisambiguatesAliasBySource(t *testing.T) {
	type input struct {
		QueryID int `parameter:"ID,kind=query,in=id"`
		PathID  int `parameter:"ID,kind=path,in=id"`
	}
	index, err := NewBindingIndex(reflect.TypeOf(input{}))
	if err != nil {
		t.Fatalf("NewBindingIndex() error = %v", err)
	}
	field, ok, err := index.Resolve(&spec.Parameter{
		Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"},
	})
	if err != nil || !ok || field.Name != "PathID" {
		t.Fatalf("Resolve() = %s, %v, %v", field.Name, ok, err)
	}
	_, _, err = index.Resolve(&spec.Parameter{Name: "ID"})
	if err == nil || !strings.Contains(err.Error(), "matches multiple input fields") {
		t.Fatalf("Resolve() error = %v", err)
	}
}

func TestSelectorBindingUsesCorrespondingViewSource(t *testing.T) {
	type input struct {
		Fields        []string `parameter:"Fields,kind=query,in=_fields"`
		ProductFields []string `parameter:"Fields,kind=query,in=product_fields"`
	}
	index, err := NewBindingIndex(reflect.TypeOf(input{}))
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct{ view, source, field string }{
		{"inventory", "_fields", "Fields"},
		{"products", "product_fields", "ProductFields"},
	} {
		t.Run(test.view, func(t *testing.T) {
			param := &spec.Parameter{Name: "Fields", Source: spec.BindSource{Kind: "query", Name: test.source}, QuerySelector: &spec.QuerySelectorBinding{View: test.view}}
			field, ok, err := index.Resolve(param)
			if err != nil || !ok || field.Name != test.field {
				t.Fatalf("selector resolved to %s, %v, %v; want %s", field.Name, ok, err, test.field)
			}
		})
	}
}

func TestSelectorBindingCannotFallBackToAnotherViewSource(t *testing.T) {
	type input struct {
		Fields []string `parameter:"Fields,kind=query,in=_fields"`
	}
	index, err := NewBindingIndex(reflect.TypeFor[input]())
	if err != nil {
		t.Fatal(err)
	}
	_, ok, err := index.Resolve(&spec.Parameter{Name: "Fields", Source: spec.BindSource{Kind: "query", Name: "product_fields"}, QuerySelector: &spec.QuerySelectorBinding{View: "products"}})
	if err == nil || ok {
		t.Fatalf("mismatched selector source accepted: ok=%v err=%v", ok, err)
	}
}

func TestSelectorBindingSourceIsCaseSensitive(t *testing.T) {
	type input struct {
		Root  []string `parameter:"Fields,kind=query,in=fields"`
		Child []string `parameter:"Fields,kind=query,in=Fields"`
	}
	index, err := NewBindingIndex(reflect.TypeFor[input]())
	if err != nil {
		t.Fatal(err)
	}
	field, ok, err := index.Resolve(&spec.Parameter{Name: "Fields", Source: spec.BindSource{Kind: "query", Name: "Fields"}, QuerySelector: &spec.QuerySelectorBinding{View: "child"}})
	if err != nil || !ok || field.Name != "Child" {
		t.Fatalf("case-sensitive source resolved to %s, %v, %v", field.Name, ok, err)
	}
}
