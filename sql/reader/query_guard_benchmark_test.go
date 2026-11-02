package reader

import (
	"context"
	"github.com/viant/datly/spec"
	"strconv"
	"testing"
)

func BenchmarkAbsentQueryGuard(b *testing.B) {
	type otherKey int
	for _, depth := range []int{0, 8, 32} {
		b.Run(strconv.Itoa(depth), func(b *testing.B) {
			ctx := context.Background()
			for i := 0; i < depth; i++ {
				ctx = context.WithValue(ctx, otherKey(i), i)
			}
			component := &spec.Component{}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if queryGuardFor(ctx, component) != nil {
					b.Fatal("unexpected guard")
				}
			}
		})
	}
}
