package transcribe

import (
	"context"
	"testing"

	"github.com/viant/datly/internal/testharness"
)

func TestDiscoveryIncludesGoOnlyAndOverlayExactlyOnce(t *testing.T) {
	for _, overlay := range []bool{false, true} {
		name := "go-only"
		if overlay {
			name = "dql-overlay"
		}
		t.Run(name, func(t *testing.T) {
			base := t.TempDir()
			testharness.WriteGeneratedGoMod(t, base)
			writeSourceFile(t, base, "model/audit.go", "package model\n type AuditRow struct { ID int }")
			writeSourceFile(t, base, "svc/users/component.go", discoveredPackageComponent)
			if overlay {
				writeSourceFile(t, base, "svc/users/Users.dql", "#setting($_ = $route('/v1/users', 'GET'))\nSELECT ID FROM users")
			}
			project, err := (&Discovery{BaseDir: base, Include: []string{"example.com/generated/svc/users"}}).Compile(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(project.Components) != 1 {
				t.Fatalf("components = %d", len(project.Components))
			}
			result := project.Components[0]
			if result.Component.Name != "Users" || len(result.Component.Routes) != 1 || result.Component.Routes[0].Path != "/v1/users" {
				t.Fatalf("component = %+v", result.Component)
			}
			if result.Contracts.Input == nil || result.Contracts.Output == nil || result.Component.RootView == nil {
				t.Fatalf("missing linked contract or root: %+v", result)
			}
			if !overlay && result.Source.Text != "" {
				t.Fatal("Go-only source must not fabricate DQL")
			}
		})
	}
}
