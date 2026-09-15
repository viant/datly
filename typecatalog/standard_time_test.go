package typecatalog

import (
	"github.com/viant/x"
	"reflect"
	"testing"
	"time"
)

func TestResolverDiscoveredTimestampAuthority(t *testing.T) {
	for _, ctx := range []*ResolutionContext{nil, {Imports: []PackageImport{{Alias: "clock", Package: "time"}}}} {
		resolver, err := NewResolver(NewCatalog(), TranscribeAuthority, ctx)
		if err != nil {
			t.Fatal(err)
		}
		descriptor, err := resolver.Descriptor("time.Time")
		if err != nil || descriptor == nil || descriptor.Type != reflect.TypeOf(time.Time{}) {
			t.Fatalf("native timestamp: %+v %v", descriptor, err)
		}
		if ctx == nil {
			if resolved, err := resolver.Resolve("Time"); err != nil || resolved != "" {
				t.Fatalf("implicit native authority changed unqualified name policy: %q %v", resolved, err)
			}
		} else {
			descriptor, err = resolver.Descriptor("clock.Time")
			if err != nil || descriptor == nil || descriptor.Type != reflect.TypeOf(time.Time{}) {
				t.Fatal("standard import alias lost", err)
			}
		}
	}
}

func TestTimestampAliasAuthority(t *testing.T) {
	type customTime struct{ Value string }
	type callerTime struct{ Value int }
	for _, authority := range []Authority{PackageAuthority, TranscribeAuthority} {
		for _, explicit := range []bool{false, true} {
			catalog := NewCatalog()
			custom := reflect.TypeOf(customTime{})
			caller := reflect.TypeOf(callerTime{})
			if err := catalog.Register(TypeOriginPackage, x.NewType(custom, x.WithName("Time"), x.WithPkgPath("example.com/customtime"))); err != nil {
				t.Fatal(err)
			}
			std := reflect.TypeOf(time.Time{})
			if explicit {
				std = caller
				if err := catalog.Register(TypeOriginPackage, x.NewType(caller, x.WithName("Time"), x.WithPkgPath("time"))); err != nil {
					t.Fatal(err)
				}
			}
			resolver, err := NewResolver(catalog, authority, &ResolutionContext{Imports: []PackageImport{{Alias: "time", Package: "example.com/customtime"}, {Alias: "clock", Package: "time"}}})
			if err != nil {
				t.Fatal(err)
			}
			for _, tc := range []struct {
				expression, key string
				typ             reflect.Type
			}{{"time.Time", "example.com/customtime.Time", custom}, {"clock.Time", "time.Time", std}, {"example.com/customtime.Time", "example.com/customtime.Time", custom}} {
				key, err := resolver.Resolve(tc.expression)
				if err != nil || key != tc.key {
					t.Fatalf("%s explicit=%v %s: %s %v", authority, explicit, tc.expression, key, err)
				}
				descriptor, err := resolver.Descriptor(tc.expression)
				if err != nil || descriptor == nil || descriptor.Type != tc.typ {
					t.Fatalf("descriptor %s: %+v %v", tc.expression, descriptor, err)
				}
				resolved, err := resolver.ResolveShape(tc.expression)
				if err != nil || resolved == nil || resolved.Descriptor == nil || resolved.Descriptor.Type != tc.typ {
					t.Fatalf("shape %s: %+v %v", tc.expression, resolved, err)
				}
			}
		}
	}
}
