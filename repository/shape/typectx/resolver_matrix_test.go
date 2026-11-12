package typectx

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/x"
)

type matrixOrderDefault struct{}
type matrixOrderImport struct{}
type matrixOrderPkgPath struct{}
type matrixOrderAliasImport struct{}

func TestResolver_ResolutionMatrix(t *testing.T) {
	reg := x.NewRegistry()
	reg.Register(x.NewType(reflect.TypeOf(matrixOrderDefault{}), x.WithPkgPath("github.com/acme/default"), x.WithName("Order")))
	reg.Register(x.NewType(reflect.TypeOf(matrixOrderImport{}), x.WithPkgPath("github.com/acme/imported"), x.WithName("ImportedOrder")))
	reg.Register(x.NewType(reflect.TypeOf(matrixOrderPkgPath{}), x.WithPkgPath("github.com/acme/pkgpath"), x.WithName("Order")))
	reg.Register(x.NewType(reflect.TypeOf(matrixOrderAliasImport{}), x.WithPkgPath("github.com/acme/alias/import"), x.WithName("Order")))

	testCases := []struct {
		name      string
		context   *Context
		expr      string
		wantKey   string
		ambiguous bool
	}{
		{
			name: "only default/imports",
			context: &Context{
				DefaultPackage: "github.com/acme/default",
				Imports:        []Import{{Alias: "imp", Package: "github.com/acme/imported"}},
			},
			expr:    "Order",
			wantKey: "github.com/acme/default.Order",
		},
		{
			name: "only package triple",
			context: &Context{
				PackagePath: "github.com/acme/pkgpath",
				PackageName: "pkgpath",
				PackageDir:  "pkg/pkgpath",
			},
			expr:    "Order",
			wantKey: "github.com/acme/pkgpath.Order",
		},
		{
			name: "default and package path conflict",
			context: &Context{
				DefaultPackage: "github.com/acme/default",
				PackagePath:    "github.com/acme/pkgpath",
				PackageName:    "pkgpath",
			},
			expr:      "Order",
			ambiguous: true,
		},
		{
			name: "alias import wins over package-name fallback",
			context: &Context{
				PackagePath: "github.com/acme/pkgpath",
				PackageName: "same",
				Imports:     []Import{{Alias: "same", Package: "github.com/acme/alias/import"}},
			},
			expr:    "same.Order",
			wantKey: "github.com/acme/alias/import.Order",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			resolver := NewResolver(reg, testCase.context)
			key, err := resolver.Resolve(testCase.expr)
			if testCase.ambiguous {
				require.Error(t, err)
				_, ok := err.(*AmbiguityError)
				require.True(t, ok)
				require.Empty(t, key)
				return
			}
			require.NoError(t, err)
			require.Equal(t, testCase.wantKey, key)
		})
	}
}
