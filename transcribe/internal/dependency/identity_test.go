package dependency

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	_ "github.com/viant/datly/transcribe/testdata/linkedimports/auth"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	smodel "github.com/viant/x/syntetic/model"
)

func firstLocalImportType() reflect.Type {
	type Record struct{ ID int }
	return reflect.TypeOf(Record{})
}

func secondLocalImportType() reflect.Type {
	type Record struct{ ID int }
	return reflect.TypeOf(Record{})
}

func TestLinkSourcePackageIdentityValidation(t *testing.T) {
	first, second := firstLocalImportType(), secondLocalImportType()
	require.NotEqual(t, first, second)
	for _, test := range []struct {
		name     string
		linked   []reflect.Type
		explicit reflect.Type
		existing reflect.Type
		want     reflect.Type
		err      string
	}{
		{name: "linked", linked: []reflect.Type{first}, want: first},
		{name: "duplicate same identity", linked: []reflect.Type{first, first}, want: first},
		{name: "ambiguous local names", linked: []reflect.Type{first, second}, err: "ambiguous linked compiled identities"},
		{name: "registry precedence", linked: []reflect.Type{first, second}, explicit: second, want: second},
		{name: "existing identity", existing: first, want: first},
		{name: "existing conflict", linked: []reflect.Type{second}, existing: first, err: "different compiled identity"},
		{name: "explicit conflict", explicit: second, existing: first, err: "different compiled identity"},
		{name: "source only"},
	} {
		t.Run(test.name, func(t *testing.T) {
			catalog := typecatalog.NewCatalog()
			registry := x.NewRegistry()
			if test.explicit != nil {
				registry.Register(x.NewType(test.explicit))
			}
			if test.existing != nil {
				require.NoError(t, catalog.Register(typecatalog.TypeOriginPackage, x.NewType(test.existing)))
			}
			declared := &smodel.Type{Name: first.Name()}
			pkg := &smodel.Package{PkgPath: first.PkgPath(), Types: []*smodel.Type{declared}}
			err := (&Loader{Catalog: catalog, Registry: registry}).linkSourcePackage(pkg, test.linked)
			if test.err != "" {
				require.ErrorContains(t, err, test.err)
				require.Nil(t, declared.ReflectType)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.want, declared.ReflectType)
			}
			if test.existing != nil {
				actual, _, err := catalog.ResolveRuntimeType(typecatalog.PackageAuthority, first.PkgPath()+"."+first.Name())
				require.NoError(t, err)
				require.Equal(t, test.existing, actual, "linking must not mutate the active catalog")
			}
		})
	}
}
