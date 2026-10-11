package sql

import (
	"os"
	"strings"
	"testing"
)

var normalizationScopeResult string

func BenchmarkDealNormalizationScope(b *testing.B) {
	source, err := os.ReadFile("testdata/deal_normalization_scope.sql")
	if err != nil {
		b.Fatal(err)
	}
	text := string(source)
	expected := strings.TrimSpace(normalizeParsedSelectProjection(text))
	if actual := NormalizeAuthoredSQL(text); actual != expected {
		b.Fatal("captured Deal normalization output changed")
	}
	b.Run("FullStatementParser", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			normalizationScopeResult = strings.TrimSpace(normalizeParsedSelectProjection(text))
		}
	})
	b.Run("OuterProjectionGate", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			normalizationScopeResult = NormalizeAuthoredSQL(text)
		}
	})
}
