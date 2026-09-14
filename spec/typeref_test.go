package spec

import (
	"encoding/json"
	"testing"
)

func TestCardinalityUnmarshalJSONNormalizesLegacyCase(t *testing.T) {
	for _, testCase := range []struct {
		encoded string
		want    Cardinality
	}{{`"One"`, CardinalityOne}, {`"Many"`, CardinalityMany}, {`"one"`, CardinalityOne}, {`"many"`, CardinalityMany}} {
		var actual Cardinality
		if err := json.Unmarshal([]byte(testCase.encoded), &actual); err != nil {
			t.Fatalf("Unmarshal(%s) error = %v", testCase.encoded, err)
		}
		if actual != testCase.want {
			t.Fatalf("Unmarshal(%s) = %q, want %q", testCase.encoded, actual, testCase.want)
		}
	}
}

func TestCardinalityUnmarshalJSONRejectsUnknownValue(t *testing.T) {
	var actual Cardinality
	if err := json.Unmarshal([]byte(`"several"`), &actual); err == nil {
		t.Fatal("expected unsupported cardinality error")
	}
}
