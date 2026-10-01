package compiler

import (
	"context"
	"encoding/json"
	"github.com/viant/bindly"
	"reflect"
	"testing"
	"time"
)

type reviewNamedDate struct {
	LastSeen *time.Time `json:"when" format:"timeLayout=2006-01-02T15:04:05Z07"`
	Count    int        `json:"count"`
}
type ReviewEmbeddedDate struct {
	When *time.Time `format:"timeLayout=2006-01-02T15:04:05Z07"`
}
type reviewEmbeddedBody struct {
	ReviewEmbeddedDate
	Count int
}
type reviewRecursiveBody struct {
	When  *time.Time `format:"timeLayout=2006-01-02T15:04:05Z07"`
	Child *reviewRecursiveBody
}

func reviewTransform(t *testing.T, target reflect.Type, raw string) any {
	t.Helper()
	field := reflect.StructField{Name: "Body", Type: target}
	binding := bindly.BindingSpec{}
	binding.Location.Kind = "body"
	if err := applyBodyFormat(field, &binding); err != nil {
		t.Fatal(err)
	}
	if binding.Transformer == nil {
		t.Fatal("formatted body transformer missing")
	}
	actual, err := binding.Transformer.Transform(context.Background(), nil, json.RawMessage(raw))
	if err != nil {
		t.Fatal(err)
	}
	return actual
}
func TestBodyFormatReviewPreservesCanonicalJSON(t *testing.T) {
	t.Run("case variant duplicate retains ordinary last wins", func(t *testing.T) {
		raw := `{"count":1,"Count":2,"when":"2026-10-01T04:20:27Z"}`
		var expected reviewNamedDate
		if err := json.Unmarshal([]byte(raw), &expected); err != nil {
			t.Fatal(err)
		}
		actual := reviewTransform(t, reflect.TypeFor[*reviewNamedDate](), raw).(*reviewNamedDate)
		if actual.Count != expected.Count {
			t.Fatalf("count changed from ordinary decoder %d to %d", expected.Count, actual.Count)
		}
	})
	t.Run("explicit json name does not authorize unknown Go name", func(t *testing.T) {
		raw := `{"LastSeen":"ignored-not-a-date","when":"2026-10-01T04:20:27Z"}`
		var expected reviewNamedDate
		if err := json.Unmarshal([]byte(raw), &expected); err != nil {
			t.Fatal(err)
		}
		actual := reviewTransform(t, reflect.TypeFor[*reviewNamedDate](), raw).(*reviewNamedDate)
		if actual.LastSeen == nil || !actual.LastSeen.Equal(*expected.LastSeen) {
			t.Fatal("explicit JSON alias changed")
		}
	})
	t.Run("anonymous embedding retains flattened date", func(t *testing.T) {
		actual := reviewTransform(t, reflect.TypeFor[*reviewEmbeddedBody](), `{"when":"2026-10-01T04:20:27+00"}`).(*reviewEmbeddedBody)
		if actual.When == nil || actual.When.UTC().Format(time.RFC3339) != "2026-10-01T04:20:27Z" {
			t.Fatal("flattened date missing")
		}
	})
	t.Run("finite recursive body applies declared child format", func(t *testing.T) {
		actual := reviewTransform(t, reflect.TypeFor[*reviewRecursiveBody](), `{"when":"2026-10-01T04:20:27Z","child":{"when":"2026-10-01T04:20:27+00"}}`).(*reviewRecursiveBody)
		if actual.Child == nil || actual.Child.When == nil || actual.Child.When.UTC().Format(time.RFC3339) != "2026-10-01T04:20:27Z" {
			t.Fatal("recursive child date missing")
		}
	})
}
