package generate

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
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
