package report

import (
	"encoding/json"
	"reflect"
	"testing"
)

type selectorJSONFixture struct {
	Metric25 bool `json:"metric25,omitempty"`
	Other    bool `json:"other,omitempty"`
}

func (v *selectorJSONFixture) UnmarshalJSON(data []byte) error {
	type plain selectorJSONFixture
	return UnmarshalSelectorJSON(data, (*plain)(v), map[string]string{"metric_25": "metric25"})
}

func TestSelectorJSONPreservesOrderedBooleanSemantics(t *testing.T) {
	for _, tc := range []struct {
		body          string
		initial, want bool
	}{
		{`{"metric_25":true}`, false, true}, {`{"metric25":true}`, false, true},
		{`{"metric_25":true,"metric25":false}`, false, false}, {`{"metric25":false,"metric_25":true}`, false, true},
		{`{"metric_25":true,"metric_25":false}`, false, false},
		{`{"metric_25":true,"metric25":null}`, false, true}, {`{"metric_25":null}`, true, true},
		{`{}`, true, true}, {`{"unknown":false}`, true, true}, {`null`, true, true},
	} {
		t.Run(tc.body, func(t *testing.T) {
			value := selectorJSONFixture{Metric25: tc.initial}
			if err := json.Unmarshal([]byte(tc.body), &value); err != nil {
				t.Fatal(err)
			}
			if value.Metric25 != tc.want {
				t.Fatalf("value=%v want %v", value.Metric25, tc.want)
			}
			encoded, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			var keys map[string]any
			if err = json.Unmarshal(encoded, &keys); err != nil {
				t.Fatal(err)
			}
			if _, ok := keys["metric_25"]; ok {
				t.Fatalf("alias marshaled: %s", encoded)
			}
		})
	}
}

func TestSelectorJSONRetainsStandardDecodeErrors(t *testing.T) {
	type plain selectorJSONFixture
	for _, body := range []string{`{"metric25":1}`, `{"metric25":"true"}`, `{"metric25":`, `{"metric25":true} {}`, `[]`, `true`, `1`, `"x"`} {
		var actual selectorJSONFixture
		var ordinary plain
		got := json.Unmarshal([]byte(body), &actual)
		want := json.Unmarshal([]byte(body), &ordinary)
		if got == nil || want == nil || reflect.TypeOf(got) != reflect.TypeOf(want) {
			t.Fatalf("%s: got %T %v want %T %v", body, got, got, want, want)
		}
	}
	var value selectorJSONFixture
	if err := json.Unmarshal([]byte(`{"metric_25":"true"}`), &value); err == nil {
		t.Fatal("wrong alias type accepted")
	}
}
