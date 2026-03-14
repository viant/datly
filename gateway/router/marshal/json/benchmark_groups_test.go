package json

import (
	"testing"
	"time"

	"github.com/viant/datly/gateway/router/marshal/config"
	"github.com/viant/tagly/format/text"
)

type benchBasic struct {
	ID    int
	Name  string
	Score float64
	On    bool
}

type benchAdvancedChild struct {
	Code  string
	Value int
}

type benchAdvanced struct {
	ID        int
	CreatedAt time.Time
	Tags      []string
	Meta      map[string]string
	Items     []*benchAdvancedChild
	Any       interface{}
}

func benchmarkMarshaller() *Marshaller {
	return New(&config.IOConfig{
		CaseFormat: text.CaseFormatLowerCamel,
		TimeLayout: time.RFC3339,
	})
}

func benchmarkBasicData() []benchBasic {
	return []benchBasic{
		{ID: 1, Name: "a", Score: 1.5, On: true},
		{ID: 2, Name: "b", Score: 2.5, On: false},
		{ID: 3, Name: "c", Score: 3.5, On: true},
	}
}

func benchmarkAdvancedData() []benchAdvanced {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	return []benchAdvanced{
		{
			ID:        10,
			CreatedAt: now,
			Tags:      []string{"x", "y", "z"},
			Meta:      map[string]string{"count": "3", "ok": "true"},
			Items:     []*benchAdvancedChild{{Code: "a", Value: 1}, {Code: "b", Value: 2}},
			Any:       map[string]interface{}{"kind": "demo", "n": 1},
		},
	}
}

func BenchmarkMarshaller_Marshal_Basic(b *testing.B) {
	m := benchmarkMarshaller()
	data := benchmarkBasicData()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := m.Marshal(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMarshaller_Unmarshal_Basic(b *testing.B) {
	m := benchmarkMarshaller()
	seed := benchmarkBasicData()
	encoded, err := m.Marshal(seed)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var out []benchBasic
		if err = m.Unmarshal(encoded, &out); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMarshaller_Marshal_Advanced(b *testing.B) {
	m := benchmarkMarshaller()
	data := benchmarkAdvancedData()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := m.Marshal(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMarshaller_Unmarshal_Advanced(b *testing.B) {
	m := benchmarkMarshaller()
	seed := benchmarkAdvancedData()
	encoded, err := m.Marshal(seed)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var out []benchAdvanced
		if err = m.Unmarshal(encoded, &out); err != nil {
			b.Fatal(err)
		}
	}
}
