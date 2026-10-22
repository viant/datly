package validation

import (
	"fmt"
	"testing"

	xhandler "github.com/viant/xdatly/handler"
)

type preparationRow struct {
	ID                                 int `sqlx:"id,primaryKey"`
	A, B, C, D, E, F, G, H, I, J, K, L string
}

type preparationFields map[string]bool

func (f preparationFields) Has(name string) bool { return f[name] }

func BenchmarkValidationBatchPreparation(b *testing.B) {
	for _, count := range []int{1075, 9796} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			rows := make([]*preparationRow, count)
			policies := make([]xhandler.ValidationOptions, count)
			fields := preparationFields{"ID": true, "A": true, "B": true, "C": true, "D": true, "E": true, "F": true, "G": true, "H": true, "I": true, "J": true, "K": true, "L": true}
			for i := range rows {
				rows[i] = &preparationRow{ID: i + 1}
				policies[i] = xhandler.ValidationOptions{Action: xhandler.WriteUpdate, Shallow: true, Previous: &preparationRow{ID: i + 1}, Fields: fields, PreviousFields: fields, Location: fmt.Sprintf("Rows[%d]", i)}
			}
			s := New(nil)
			if _, err := s.batch(b.Context(), rows, policies); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := s.batch(b.Context(), rows, policies); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
