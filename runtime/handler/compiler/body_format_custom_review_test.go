package compiler

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

type reviewCustomJSONDate struct {
	When *time.Time `format:"timeLayout=2006-01-02T15:04:05Z07"`
	Raw  json.RawMessage
}

func (v *reviewCustomJSONDate) UnmarshalJSON(raw []byte) error {
	v.Raw = append(v.Raw[:0], raw...)
	stamp := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	v.When = &stamp
	return nil
}

type reviewCustomDateBody struct {
	Date   *time.Time `format:"timeLayout=2006-01-02T15:04:05Z07"`
	Custom *reviewCustomJSONDate
}

func TestBodyFormatReviewPreservesCustomJSONAuthority(t *testing.T) {
	for _, raw := range []string{
		`{"Date":"2026-10-01T04:20:27Z","Custom":"external-domain-value"}`,
		`{"Date":"2026-10-01T04:20:27Z","Custom":{"When":"custom-decoder-owns-this"}}`,
	} {
		t.Run(raw, func(t *testing.T) {
			var expected reviewCustomDateBody
			if err := json.Unmarshal([]byte(raw), &expected); err != nil {
				t.Fatal(err)
			}
			actual := reviewTransform(t, reflect.TypeFor[*reviewCustomDateBody](), raw).(*reviewCustomDateBody)
			if actual.Custom == nil || !reflect.DeepEqual(actual.Custom, expected.Custom) {
				t.Fatal("formatted body changed opaque custom JSON contract")
			}
		})
	}
}
