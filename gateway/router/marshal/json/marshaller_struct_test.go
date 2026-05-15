package json

import (
	stdjson "encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/viant/datly/gateway/router/marshal/config"
	"github.com/viant/tagly/format/text"
)

// Session represents a user session document.
type Session struct {
	// UserID is the PK of the session set.
	UserID int `aerospike:"user_id,pk"`
	// LastSeen is the last activity timestamp. Stored as unix seconds.
	LastSeen *time.Time `aerospike:"last_seen,unixsec"`
	// Disabled marks the session as inactive.
	Disabled *bool `aerospike:"disabled"`
	// Attribute holds session attributes entries.
	Attribute []Attribute
}

// Attribute represents a single attribute entry stored within the session's attributes map bin.
// The PK is still `user_id`, and attribute entries are keyed by `name`.
type Attribute struct {
	// UserID is the session owner and record key.
	UserID int `aerospike:"user_id,pk"`
	// Name is the attribute key (map key).
	Name *string `aerospike:"name,mapKey"`
	// Value is the attribute payload; supports native Aerospike types.
	Value stdjson.RawMessage `aerospike:"value"`
}

func newMarshaller() *Marshaller {
	// We force lowerCamel JSON keys and a time layout that matches the sample payload offset (e.g. "-08").
	cfg := &config.IOConfig{
		CaseFormat: text.CaseFormatLowerCamel,
		TimeLayout: "2006-01-02T15:04:05-07",
	}
	return New(cfg)
}

func TestUnmarshal_SessionWithAttributes(t *testing.T) {
	payload := `[{"attribute":[{"name":"theme","userId":252,"value":{"color":"dark"}}],"disabled":false,"lastSeen":"2025-11-05T17:00:07-08","userId":252}]`

	var got []Session
	err := newMarshaller().Unmarshal([]byte(payload), &got)
	if err != nil {
		t.Fatalf("unexpected unmarshal error: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 session, got %d", len(got))
	}

	s := got[0]
	if s.UserID != 252 {
		t.Fatalf("expected userId=252, got %d", s.UserID)
	}
	if s.Disabled == nil || *s.Disabled != false {
		t.Fatalf("expected disabled=false, got %v", s.Disabled)
	}
	if s.LastSeen == nil {
		t.Fatalf("expected lastSeen to be set")
	}
	// Verify attributes
	if len(s.Attribute) != 1 {
		t.Fatalf("expected 1 attribute, got %d", len(s.Attribute))
	}
	a := s.Attribute[0]
	if a.UserID != 252 {
		t.Fatalf("expected attribute.userId=252, got %d", a.UserID)
	}
	if a.Name == nil || *a.Name != "theme" {
		if a.Name == nil {
			t.Fatalf("expected attribute.name=theme, got <nil>")
		}
		t.Fatalf("expected attribute.name=theme, got %s", *a.Name)
	}
	// Ensure raw value round-trips as expected JSON
	var valueObj map[string]string
	if err := stdjson.Unmarshal(a.Value, &valueObj); err != nil {
		t.Fatalf("unexpected attribute.value unmarshal error: %v", err)
	}
	expected := map[string]string{"color": "dark"}
	if !reflect.DeepEqual(valueObj, expected) {
		t.Fatalf("unexpected attribute.value: got %+v want %+v", valueObj, expected)
	}
}

func TestMarshal_SessionWithAttributes(t *testing.T) {
	name := "theme"
	disabled := false
	ts, err := time.Parse("2006-01-02T15:04:05-07", "2025-11-05T17:00:07-08")
	if err != nil {
		t.Fatalf("invalid test time: %v", err)
	}
	raw := stdjson.RawMessage(`{"color":"dark"}`)
	data := []Session{
		{
			UserID:   252,
			LastSeen: &ts,
			Disabled: &disabled,
			Attribute: []Attribute{
				{UserID: 252, Name: &name, Value: raw},
			},
		},
	}

	out, err := newMarshaller().Marshal(data)
	if err != nil {
		t.Fatalf("unexpected marshal error: %v", err)
	}

	// Compare semantically by decoding both expected and actual into generic values.
	expected := `[{"attribute":[{"name":"theme","userId":252,"value":{"color":"dark"}}],"disabled":false,"lastSeen":"2025-11-05T17:00:07-08","userId":252}]`

	var gotVal, wantVal interface{}
	if err := stdjson.Unmarshal(out, &gotVal); err != nil {
		t.Fatalf("unexpected result json: %v, body=%s", err, string(out))
	}
	if err := stdjson.Unmarshal([]byte(expected), &wantVal); err != nil {
		t.Fatalf("invalid expected json: %v", err)
	}
	if !reflect.DeepEqual(gotVal, wantVal) {
		t.Fatalf("mismatch json:\n got: %s\nwant: %s", string(out), expected)
	}
}

func TestBoolPointer_NullAndPresent(t *testing.T) {
	// Case 1: disabled is null -> Disabled == nil
	payloadNull := `[{"userId":1,"disabled":null}]`
	var s1 []Session
	if err := newMarshaller().Unmarshal([]byte(payloadNull), &s1); err != nil {
		t.Fatalf("unmarshal null disabled: %v", err)
	}
	if len(s1) != 1 || s1[0].Disabled != nil {
		t.Fatalf("expected Disabled=nil, got %+v", s1)
	}

	// Case 2: disabled false -> Disabled != nil and false
	payloadFalse := `[{"userId":1,"disabled":false}]`
	var s2 []Session
	if err := newMarshaller().Unmarshal([]byte(payloadFalse), &s2); err != nil {
		t.Fatalf("unmarshal false disabled: %v", err)
	}
	if len(s2) != 1 || s2[0].Disabled == nil || *s2[0].Disabled != false {
		t.Fatalf("expected Disabled=false pointer, got %+v", s2)
	}

	// Case 3: marshal with Disabled=nil -> emits null
	data := []Session{{UserID: 3}}
	out, err := newMarshaller().Marshal(data)
	if err != nil {
		t.Fatalf("marshal nil disabled: %v", err)
	}
	// verify null present for disabled if not omitted by config
	var v []map[string]interface{}
	if err := stdjson.Unmarshal(out, &v); err != nil {
		t.Fatalf("decode marshalled: %v", err)
	}
	if _, ok := v[0]["disabled"]; !ok {
		t.Fatalf("expected disabled key present; got %s", string(out))
	}
	if v[0]["disabled"] != nil {
		t.Fatalf("expected disabled=null, got %v", v[0]["disabled"])
	}
}
