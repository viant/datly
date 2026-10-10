package sql

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/sqlparser"
)

// Keep the parser-based implementation as the compatibility oracle.
func canonicalProjectionNameReference(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	parts, err := sqlparser.TableIdentifierParts(value)
	if err != nil {
		parts = []string{value}
	}
	for i := range parts {
		parts[i] = strings.ToLower(parts[i])
	}
	encoded, _ := json.Marshal(parts)
	return "identifier:" + string(encoded)
}

func TestCanonicalProjectionNameParserCompatibility(t *testing.T) {
	inputs := []string{
		"", " \t\n", "id", "ID", "user_id", "_", "_1", "a$1", "select", "NULL",
		"0id", "$id", "a-b", "*", "a.*", "a.b", "a . b", "a.", ".a", "a..b",
		"\"ID\"", "`user_id`.Total_Value", "[a.b]", "'label'", "\"a\"\"b\"", "[a]]b]",
		"`unterminated", "\"\"", "COUNT(*)", "price + tax", "x AS y", "a/*c*/", "a;b",
		"a\\b", "a\x00b", "a\n b", "a\xff", "日本語", "Ångström", "İD", "a١",
		"\u2003ID\u00a0", "<tag>", "a&b", "a\u2028b", "a\u2029b",
	}
	// Cover every byte in first and subsequent positions, including invalid UTF-8.
	for i := 0; i < 256; i++ {
		inputs = append(inputs, string([]byte{byte(i)}), string([]byte{'a', byte(i)}), string([]byte{byte(i), 'a'}))
	}
	random := rand.New(rand.NewSource(1))
	for i := 0; i < 5000; i++ {
		value := make([]byte, random.Intn(40))
		for j := range value {
			value[j] = byte(random.Intn(256))
		}
		inputs = append(inputs, string(value))
	}
	for _, input := range inputs {
		if actual, expected := canonicalProjectionName(input), canonicalProjectionNameReference(input); actual != expected {
			t.Fatalf("canonicalProjectionName(%q) = %q, parser reference = %q", input, actual, expected)
		}
	}
}

func FuzzCanonicalProjectionNameParserCompatibility(f *testing.F) {
	for _, input := range []string{"id", "ID", "_a$1", "a.b", "\"a.b\"", "SUM(x)", "Å", "a\xff"} {
		f.Add(input)
	}
	f.Fuzz(func(t *testing.T, input string) {
		if actual, expected := canonicalProjectionName(input), canonicalProjectionNameReference(input); actual != expected {
			t.Fatalf("input %q: got %q, parser reference %q", input, actual, expected)
		}
	})
}

var canonicalProjectionBenchmarkResult string

func BenchmarkCanonicalProjectionName(b *testing.B) {
	for _, input := range []string{"campaign_id", "Total_Value", "a$1", "`user_id`.Total_Value", "SUM(price)", "日本語"} {
		for _, implementation := range []struct {
			name         string
			canonicalize func(string) string
		}{{"current", canonicalProjectionName}, {"parser_reference", canonicalProjectionNameReference}} {
			b.Run(input+"/"+implementation.name, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					canonicalProjectionBenchmarkResult = implementation.canonicalize(input)
				}
			})
		}
	}
}

// Exercise repeated name membership checks in a metadata-backed native reader.
func BenchmarkSelectorProjectionWideIdentifiers(b *testing.B) {
	view := &data.View{}
	var selected []string
	for i := 0; i < 64; i++ {
		name := fmt.Sprintf("campaign_field_%d", i)
		view.Columns = append(view.Columns, &data.Column{Name: name, Column: name})
		if i%2 == 0 {
			selected = append(selected, name)
		}
	}
	p := SelectorProjection{SQL: "SELECT * FROM campaigns", View: view}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		projection, err := p.Prepare(selected)
		if err != nil {
			b.Fatal(err)
		}
		if len(projection.outer) != len(selected) {
			b.Fatalf("unexpected output count: %d", len(projection.outer))
		}
	}
}
