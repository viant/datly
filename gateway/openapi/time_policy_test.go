package openapi

import (
	"reflect"
	"testing"
	"time"

	"github.com/xeipuuv/gojsonschema"
)

func TestDeclaredTimeInputSchema(t *testing.T) {
	type row struct {
		Stamp *time.Time `format:"timeLayout=2006-01-02T15:04:05Z07:00,timeInputLayouts=2006-01-02T15:04:05.999999999Z07:00|2006-01-02T15:04:05|2006-01-02 15:04:05|2006-01-02,timeEpoch=milliseconds,timeEmpty=preserve,timezone=UTC,nullable=true"`
	}
	schema, err := timeInputSchema(reflect.TypeFor[row]().Field(0))
	if err != nil {
		t.Fatal(err)
	}
	if last := schema.AnyOf[len(schema.AnyOf)-1]; !last.Nullable || len(last.Enum) != 1 || last.Enum[0] != nil {
		t.Fatal("missing OpenAPI nullable alternative")
	}
	for _, tc := range []struct {
		raw   string
		valid bool
	}{
		{`0`, true}, {`-1`, true}, {`"2026-11-07"`, true}, {`"2026-11-07T08:00:00-08:00"`, true}, {`"2026-11-07 08:00:00"`, true}, {`""`, true}, {`"123"`, false}, {`"bad"`, false}, {`true`, false}, {`{}`, false}, {`"2026-11-07tail"`, false},
	} {
		result, err := gojsonschema.Validate(gojsonschema.NewGoLoader(schema), gojsonschema.NewStringLoader(tc.raw))
		if err != nil {
			t.Fatal(err)
		}
		if result.Valid() != tc.valid {
			t.Errorf("%s valid=%v errors=%v", tc.raw, result.Valid(), result.Errors())
		}
	}
}
