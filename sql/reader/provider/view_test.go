package provider

import (
	"strings"
	"testing"

	"github.com/viant/bindly/locator"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	sqlreader "github.com/viant/datly/sql/reader"
)

func TestViewProviderUsesDependentPriority(t *testing.T) {
	provider := &viewProvider{}
	if provider.Priority() != locator.PriorityDependent || provider.Priority() <= locator.PriorityTransform {
		t.Fatalf("view provider priority = %d", provider.Priority())
	}
}

func TestNewRejectsIncompleteViewProviderConfig(t *testing.T) {
	if _, err := New(Config{}); err == nil || !strings.Contains(err.Error(), "dependencies are required") {
		t.Fatalf("expected dependency error, got %v", err)
	}
	if _, err := New(Config{Dependencies: []*sqlreader.ViewDependency{{Name: "Rows"}}}); err == nil || !strings.Contains(err.Error(), "SQL component is required") {
		t.Fatalf("expected SQL component error, got %v", err)
	}
	if _, err := New(Config{
		Dependencies: []*sqlreader.ViewDependency{{Name: "Rows"}},
		SQL:          &dsql.SQLComponent{},
	}); err == nil || !strings.Contains(err.Error(), "component is required") {
		t.Fatalf("expected component error, got %v", err)
	}
	if _, err := New(Config{
		Dependencies: []*sqlreader.ViewDependency{{Name: "Rows", Component: &spec.Component{}}},
		SQL:          &dsql.SQLComponent{},
	}); err == nil || !strings.Contains(err.Error(), "input contract is required") {
		t.Fatalf("expected input contract error, got %v", err)
	}
}
