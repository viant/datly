package transcribe

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
)

func TestValidatorStaticChecksDoNotPersist(t *testing.T) {
	for _, test := range []struct {
		name, source string
		valid        bool
	}{
		{"valid", "#setting($_ = $route('/v1/records', 'GET'))\nSELECT 1 AS ID", true},
		{"invalid", "#setting($_ = $route('/v1/records', 'GET'))\nSELECT (", false},
		{"empty-selection", "SELECT 1", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			base := t.TempDir()
			writeSourceFile(t, base, "go.mod", discoverGoMod)
			writeSourceFile(t, base, "svc/records/Records.dql", test.source)
			report, err := (&Validator{BaseDir: base, Include: []string{"example.com/app/svc/records"}}).Validate(context.Background())
			if (err == nil) != test.valid || report.Valid != test.valid {
				t.Fatalf("report=%+v err=%v", report, err)
			}
			if len(report.Skipped) == 0 {
				t.Fatal("unperformed checks were hidden")
			}
			if !test.valid && len(report.Diagnostics) == 0 {
				t.Fatal("missing diagnostics")
			}
			if _, statErr := os.Stat(filepath.Join(base, "generated")); !os.IsNotExist(statErr) {
				t.Fatalf("generated destination changed: %v", statErr)
			}
			content, readErr := os.ReadFile(filepath.Join(base, "svc/records/Records.dql"))
			if readErr != nil || string(content) != test.source {
				t.Fatal("source was changed")
			}
		})
	}
}

func TestValidatorGoOnlyAndLinkedPackages(t *testing.T) {
	for _, overlay := range []bool{false, true} {
		name := "go-only"
		if overlay {
			name = "linked-dql"
		}
		t.Run(name, func(t *testing.T) {
			base := t.TempDir()
			testharness.WriteGeneratedGoMod(t, base)
			writeSourceFile(t, base, "model/audit.go", "package model\n type AuditRow struct { ID int }")
			writeSourceFile(t, base, "svc/users/component.go", discoveredPackageComponent)
			if overlay {
				writeSourceFile(t, base, "svc/users/Users.dql", "#setting($_ = $route('/v1/users', 'GET'))\nSELECT ID FROM users")
			}
			report, err := (&Validator{BaseDir: base, Include: []string{"example.com/generated/svc/users"}}).Validate(context.Background())
			if err != nil || !report.Valid || len(report.Components) != 1 {
				t.Fatalf("report=%+v err=%v", report, err)
			}
			if _, err = os.Stat(filepath.Join(base, "generated")); !os.IsNotExist(err) {
				t.Fatalf("unexpected generated files: %v", err)
			}
		})
	}
}

func TestValidatorCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	report, err := (&Validator{BaseDir: t.TempDir(), Include: []string{"example.com/app"}}).Validate(ctx)
	if !errors.Is(err, context.Canceled) || report.Valid || len(report.Diagnostics) == 0 {
		t.Fatalf("report=%+v err=%v", report, err)
	}
}

func TestValidatorRejectsMalformedGoShapeSQL(t *testing.T) {
	base := t.TempDir()
	testharness.WriteGeneratedGoMod(t, base)
	writeSourceFile(t, base, "model/audit.go", "package model\n type AuditRow struct { ID int }")
	writeSourceFile(t, base, "svc/users/component.go", strings.ReplaceAll(discoveredPackageComponent, "SELECT ID FROM users", "SELECT ("))
	report, err := (&Validator{BaseDir: base, Include: []string{"example.com/generated/svc/users"}}).Validate(context.Background())
	if err == nil || report.Valid {
		t.Fatalf("malformed Go-shape SQL accepted: report=%+v err=%v", report, err)
	}
}
