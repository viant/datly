package transcribe

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/transcribe/gobuild"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	"github.com/viant/xdatly/response"
)

func TestBorrowedFreshStandardSourceRuntimeOverlap(t *testing.T) {
	cwd, err := os.Getwd()
	require.NoError(t, err)
	root := filepath.Dir(cwd)
	text := "#package('example.com/borrowed/owner')\n#import('response','github.com/viant/xdatly/response')\n#define($_ = $Metrics<*response.Metrics>(param/Metrics))\n"
	source := &Source{Scope: "example.com/borrowed/owner", Path: filepath.Join(root, "probe.dql"), Text: text, GoBuild: &gobuild.Context{Dir: root, Env: []string{"GOWORK=off", "GOFLAGS=-mod=readonly"}}}
	catalog, err := freshBorrowedForeignTypes(t.Context(), source, text)
	require.NoError(t, err)
	typ, found, err := catalog.ResolveRuntimeType(typecatalog.PackageAuthority, "github.com/viant/xdatly/response.Metrics")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, reflect.TypeFor[response.Metrics](), typ)
}

func TestBorrowedForeignIdentityReuseRemainsStrict(t *testing.T) {
	const key = "github.com/viant/xdatly/response.Metrics"
	same := x.NewType(reflect.TypeFor[response.Metrics]())
	same.Location = "selected/metrics.go"
	for _, test := range []struct {
		name      string
		typ       *x.Type
		wantError bool
	}{
		{"exact identity", same, false},
		{"different identity", x.NewType(reflect.TypeFor[string](), x.WithPkgPath("github.com/viant/xdatly/response"), x.WithName("Metrics")), true},
		{"unlinked source", &x.Type{PkgPath: "github.com/viant/xdatly/response", Name: "Metrics", Definition: "struct{ Changed bool }"}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			catalog := typecatalog.NewCatalog()
			before, _, err := catalog.Resolve(typecatalog.PackageAuthority, key)
			require.NoError(t, err)
			err = registerBorrowedForeignType(catalog, typecatalog.TypeOriginPackage, test.typ)
			if test.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			after, _, err := catalog.Resolve(typecatalog.PackageAuthority, key)
			require.NoError(t, err)
			require.Equal(t, before.Type, after.Type)
			require.Equal(t, before.Location, after.Location, "existing authority is preserved")
		})
	}
}
