package contract

import (
	"github.com/viant/bindly"
	"reflect"
	"testing"
)

// Include native header binding and policy selection on every hit: the cache
// stores types, never the current caller's authorization or callback closure.
func BenchmarkResolveComposite(b *testing.B) {
	for _, tc := range []struct {
		name, role string
		capacity   int
	}{
		{"linked", "admin", 8}, {"cached", "reader", 8}, {"uncached", "reader", 0},
	} {
		b.Run(tc.name, func(b *testing.B) {
			plan, err := New(reflect.TypeFor[[]*campaignPolicy](), tc.capacity)
			if err != nil {
				b.Fatal(err)
			}
			scope := policyBinder(b, tc.role)
			states := NewStatePlans(func(typ reflect.Type) (*bindly.Plan, error) { return scope.CompilePlan(typ) })
			binder := states.Binder(scope)
			if _, err = plan.Resolve(b.Context(), binder); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err = plan.Resolve(b.Context(), binder); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
