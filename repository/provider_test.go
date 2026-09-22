package repository

import (
	"context"
	"strings"
	"testing"

	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/repository/version"
)

func TestProviderComponentReportsNilFactoryResult(t *testing.T) {
	provider := NewProvider(*contract.NewPath("GET", "/broken"), &version.Control{}, func(context.Context, ...Option) (*Component, error) {
		return nil, nil
	})
	component, err := provider.Component(context.Background())
	if component != nil {
		t.Fatalf("expected nil component, got %#v", component)
	}
	if err == nil || !strings.Contains(err.Error(), "GET:/broken") {
		t.Fatalf("expected route-specific error, got %v", err)
	}
}
