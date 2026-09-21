package generate

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	smodel "github.com/viant/x/syntetic/model"
)

func TestPackageOwnershipDestination(t *testing.T) {
	key := spec.Key{Kind: spec.KindComponent, Scope: "example.com/app/records", Name: "Records"}
	for _, name := range []string{"absent", "single", "shared", "other-owner", "old-metadata", "wrong-identity", "moved-package", "invalid-version", "nil-owner"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			manifest := &scaffoldManifest{Version: scaffoldManifestVersion, Owner: key.Name, Identity: key.String(), ComponentPackage: key.Scope}
			want := key.Scope
			wantError := ""
			switch name {
			case "absent", "other-owner", "old-metadata", "wrong-identity":
				want = ""
			case "moved-package":
				wantError = "ownership does not match"
			case "invalid-version":
				wantError = "unsupported scaffold manifest version"
			case "nil-owner":
				wantError = "invalid component ownership manifest"
			}
			switch name {
			case "shared":
				manifest = &scaffoldManifest{Version: scaffoldManifestVersion, Owners: map[string]*scaffoldManifest{key.Name: manifest, "Other": {Owner: "Other"}}}
			case "other-owner":
				manifest.Owner = "Other"
			case "old-metadata":
				manifest.Identity, manifest.ComponentPackage = "", ""
			case "wrong-identity":
				manifest.Identity = "component:example.com/other:Records"
			case "moved-package":
				manifest.ComponentPackage = "example.com/app/elsewhere"
			case "invalid-version":
				manifest.Version = 999
			case "nil-owner":
				manifest = &scaffoldManifest{Version: scaffoldManifestVersion, Owners: map[string]*scaffoldManifest{key.Name: nil}}
			}
			path := filepath.Join(dir, scaffoldManifestName)
			before, err := json.Marshal(manifest)
			if err != nil {
				t.Fatal(err)
			}
			if name != "absent" {
				if err = os.WriteFile(path, before, 0600); err != nil {
					t.Fatal(err)
				}
			}
			got, err := (PackageOwnership{Directory: dir}).Destination(key)
			if wantError != "" {
				if err == nil || !strings.Contains(err.Error(), wantError) {
					t.Fatalf("destination=%q err=%v; want %s", got, err, wantError)
				}
			} else if err != nil || got != want {
				t.Fatalf("destination=%q err=%v; want %q", got, err, want)
			}
			if name != "absent" {
				after, err := os.ReadFile(path)
				if err != nil || string(after) != string(before) {
					t.Fatal("ownership read changed manifest", err)
				}
			}
		})
	}
}

func TestRegisterPackageTreatsResourceOnlyManifestAsAuthored(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, scaffoldManifestName), []byte(`{"resources":{"namespace":"app","files":["query.sql"]}}`), 0644); err != nil {
		t.Fatal(err)
	}
	catalog := typecatalog.NewCatalog()
	if err := (&Result{}).RegisterPackage(catalog, &smodel.Package{PkgPath: "example.com/app"}, dir); err != nil {
		t.Fatalf("resource-only package registration failed: %v", err)
	}
}

func TestPackageOwnershipResourceOnly(t *testing.T) {
	for _, tc := range []struct {
		name, source string
		reject       bool
	}{
		{"resources", `{"resources":{"namespace":"app","files":["query.sql"]}}`, false},
		{"shared resources", `{"owners":{"Read":{"resources":{"namespace":"app","files":["query.sql"]}}}}`, false},
		{"claimed identity", `{"identity":"component:app:Read","resources":{"namespace":"app","files":["query.sql"]}}`, true},
		{"claimed files", `{"files":["input.go"],"resources":{"namespace":"app","files":["query.sql"]}}`, true},
		{"unsupported version", `{"version":999,"resources":{"namespace":"app","files":["query.sql"]}}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, scaffoldManifestName)
			if err := os.WriteFile(path, []byte(tc.source), 0644); err != nil {
				t.Fatal(err)
			}
			got, err := (PackageOwnership{Directory: dir}).Destination(spec.Key{Kind: spec.KindComponent, Scope: "example.com/app", Name: "Read"})
			if (err != nil) != tc.reject || got != "" {
				t.Fatalf("destination=%q err=%v", got, err)
			}
			if _, err := readScaffoldManifest(dir); err == nil {
				t.Fatal("resource-only or invalid descriptor authorized generation")
			}
			after, err := os.ReadFile(path)
			if err != nil || string(after) != tc.source {
				t.Fatal("read changed resource descriptor", err)
			}
		})
	}
}
