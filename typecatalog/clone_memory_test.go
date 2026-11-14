package typecatalog

import (
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/x"
	smodel "github.com/viant/x/syntetic/model"
)

func populatedCloneCatalog(t testing.TB, count int) *Catalog {
	t.Helper()
	c := NewCatalog()
	for i := 0; i < count; i++ {
		require.NoError(t, c.Register(TypeOriginPackage, x.NewType(reflect.TypeFor[int](), x.WithPkgPath("example.com/large"), x.WithName(fmt.Sprintf("T%d", i)))))
	}
	return c
}

func TestCatalogCloneAllocationDoesNotScaleWithEntries(t *testing.T) {
	measure := func(count int) float64 {
		c := populatedCloneCatalog(t, count)
		var snapshot *Catalog
		allocations := testing.AllocsPerRun(10, func() {
			var err error
			snapshot, err = c.Clone()
			if err != nil {
				panic(err)
			}
		})
		runtime.KeepAlive(snapshot)
		return allocations
	}
	small, large := measure(1), measure(2048)
	require.LessOrEqual(t, large, small+4, "snapshot allocation must not grow with the application type inventory")
}

func BenchmarkCatalogLargeSnapshot(b *testing.B) {
	c := populatedCloneCatalog(b, 4096)
	b.ReportAllocs()
	b.ResetTimer()
	var snapshot *Catalog
	for i := 0; i < b.N; i++ {
		var err error
		snapshot, err = c.Clone()
		if err != nil {
			b.Fatal(err)
		}
	}
	runtime.KeepAlive(snapshot)
}

func TestCatalogCloneMutationIsolation(t *testing.T) {
	const key = "example.com/demo.Record"
	compiled := x.NewType(reflect.TypeFor[struct{ ID int }](), x.WithPkgPath("example.com/demo"), x.WithName("Record"))
	extra := x.NewType(reflect.TypeFor[string](), x.WithPkgPath("example.com/demo"), x.WithName("Extra"))
	for name, mutate := range map[string]func(*Catalog) error{
		"register origin": func(c *Catalog) error { return c.RegisterAll(TypeOriginDQL, compiled, extra) },
		"link all":        func(c *Catalog) error { return c.LinkRuntimeAll(TypeOriginPackage, compiled, extra) },
		"link types":      func(c *Catalog) error { return c.LinkRuntimeTypes(TypeOriginPackage, compiled, extra) },
		"replace package": func(c *Catalog) error {
			return c.RegisterPackage(TypeOriginPackage, &smodel.Package{PkgPath: "example.com/demo"})
		},
		"import": func(c *Catalog) error {
			source := NewCatalog()
			if err := source.RegisterAll(TypeOriginDQL, compiled, extra); err != nil {
				return err
			}
			return c.ImportReferences(source, []string{key, extra.Key()})
		},
	} {
		t.Run(name, func(t *testing.T) {
			for changed := 0; changed < 3; changed++ {
				original := NewCatalog()
				require.NoError(t, original.Register(TypeOriginPackage, &x.Type{PkgPath: "example.com/demo", Name: "Record", Definition: "struct{ ID int }"}))
				clone, err := original.Clone()
				require.NoError(t, err)
				second, err := clone.Clone()
				require.NoError(t, err)
				catalogs := []*Catalog{original, clone, second}
				require.NoError(t, mutate(catalogs[changed]))
				target, found, err := catalogs[changed].Resolve(TranscribeAuthority, key)
				require.NoError(t, err)
				if name == "replace package" {
					require.False(t, found)
				} else {
					require.True(t, found)
					require.Equal(t, compiled.Type, target.Type)
				}
				for index, c := range catalogs {
					if index == changed {
						continue
					}
					typ, found, err := c.Resolve(PackageAuthority, key)
					require.NoError(t, err)
					require.True(t, found)
					require.Nil(t, typ.Type)
					require.Equal(t, "struct{ ID int }", typ.Definition)
					_, found, err = c.Resolve(PackageAuthority, extra.Key())
					require.NoError(t, err)
					require.False(t, found)
					typ, found, err = c.Resolve(TranscribeAuthority, key)
					require.NoError(t, err)
					require.True(t, found)
					require.Nil(t, typ.Type, "new origin leaked into sibling")
				}
			}
		})
	}
}

func TestCatalogCloneNoopAndFailedUpdatesRetainSnapshot(t *testing.T) {
	compiled := x.NewType(reflect.TypeFor[int](), x.WithPkgPath("example.com/demo"), x.WithName("Record"))
	extra := x.NewType(reflect.TypeFor[string](), x.WithPkgPath("example.com/demo"), x.WithName("Extra"))
	conflict := x.NewType(reflect.TypeFor[string](), x.WithPkgPath("example.com/demo"), x.WithName("Record"))
	for name, update := range map[string]func(*Catalog, []*x.Type) error{
		"register":   func(c *Catalog, types []*x.Type) error { return c.RegisterAll(TypeOriginPackage, types...) },
		"link all":   func(c *Catalog, types []*x.Type) error { return c.LinkRuntimeAll(TypeOriginPackage, types...) },
		"link types": func(c *Catalog, types []*x.Type) error { return c.LinkRuntimeTypes(TypeOriginPackage, types...) },
		"import": func(c *Catalog, types []*x.Type) error {
			source := NewCatalog()
			if err := source.RegisterAll(TypeOriginPackage, types...); err != nil {
				return err
			}
			var keys []string
			for _, typ := range types {
				keys = append(keys, typ.Key())
			}
			return c.ImportReferences(source, keys)
		},
	} {
		t.Run(name, func(t *testing.T) {
			source := NewCatalog()
			require.NoError(t, source.Register(TypeOriginPackage, compiled))
			clone, err := source.Clone()
			require.NoError(t, err)
			require.NoError(t, update(clone, []*x.Type{compiled}))
			require.True(t, clone.shared, "no-op must not retain a duplicated index")
			require.Error(t, update(clone, []*x.Type{extra, conflict}))
			require.True(t, clone.shared, "rejected batch must not detach")
			for _, c := range []*Catalog{source, clone} {
				_, found, err := c.ResolveRuntimeType(PackageAuthority, extra.Key())
				require.NoError(t, err)
				require.False(t, found)
				actual, found, err := c.ResolveRuntimeType(PackageAuthority, compiled.Key())
				require.NoError(t, err)
				require.True(t, found)
				require.Equal(t, compiled.Type, actual)
			}
		})
	}
}

func TestCatalogCloneSyntheticRuntimeRepair(t *testing.T) {
	for _, previous := range []reflect.Type{nil, reflect.TypeFor[string]()} {
		for _, all := range []bool{false, true} {
			native := reflect.TypeFor[int]()
			compiled := x.NewType(native, x.WithPkgPath("example.com/demo"), x.WithName("Record"))
			typ := x.NewType(native, x.WithPkgPath("example.com/demo"), x.WithName("Record"), x.WithSyntheticType(&smodel.Type{Name: "Record", ReflectType: previous}))
			source := NewCatalog()
			require.NoError(t, source.Register(TypeOriginPackage, typ))
			clone, err := source.Clone()
			require.NoError(t, err)
			if all {
				err = clone.LinkRuntimeAll(TypeOriginPackage, compiled)
			} else {
				err = clone.LinkRuntimeTypes(TypeOriginPackage, compiled)
			}
			require.NoError(t, err)
			got, found, err := clone.Resolve(PackageAuthority, typ.Key())
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, native, got.SynteticType.ReflectType)
			got, found, err = source.Resolve(PackageAuthority, typ.Key())
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, previous, got.SynteticType.ReflectType)
		}
	}
}

func TestCatalogCloneRestoresRemovedStandardTypes(t *testing.T) {
	c := NewCatalog()
	standard := standardTypes()[0]
	require.NoError(t, c.RegisterPackage(TypeOriginPackage, &smodel.Package{PkgPath: standard.PkgPath}))
	_, found, err := c.Resolve(PackageAuthority, standard.Key())
	require.NoError(t, err)
	require.False(t, found)
	clone, err := c.Clone()
	require.NoError(t, err)
	for _, typ := range standardTypes() {
		actual, found, err := clone.ResolveRuntimeType(PackageAuthority, typ.Key())
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, typ.Type, actual)
	}
}

func TestCatalogCloneConcurrentWrites(t *testing.T) {
	original := populatedCloneCatalog(t, 64)
	var workers sync.WaitGroup
	for i := 0; i < 8; i++ {
		clone, err := original.Clone()
		require.NoError(t, err)
		if i == 0 {
			clone = original
		}
		workers.Add(1)
		go func(index int, c *Catalog) {
			defer workers.Done()
			for j := 0; j < 20; j++ {
				snapshot, err := c.Clone()
				if err != nil {
					t.Error(err)
					return
				}
				typ := x.NewType(reflect.TypeFor[int](), x.WithPkgPath("example.com/writer"), x.WithName(fmt.Sprintf("T%d_%d", index, j)))
				if err := c.Register(TypeOriginPackage, typ); err != nil {
					t.Error(err)
					return
				}
				if _, found, err := snapshot.Resolve(PackageAuthority, typ.Key()); err != nil || found {
					t.Errorf("snapshot changed after concurrent write: found=%v err=%v", found, err)
				}
			}
		}(i, clone)
	}
	workers.Wait()
}
