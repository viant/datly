package content

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/francoispqt/gojay"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/gateway/router/marshal/config"
	legacyjson "github.com/viant/datly/gateway/router/marshal/json"
	"github.com/viant/tagly/format/text"
	"github.com/viant/xreflect"
)

type runtimeEngineCodec struct{}

var legacyJSONEngineTypeName = reflect.TypeOf(LegacyJSONRuntime{}).PkgPath() + "/" + reflect.TypeOf(LegacyJSONRuntime{}).Name()

func (r *runtimeEngineCodec) Marshal(src interface{}, _ ...interface{}) ([]byte, error) {
	return []byte(`{"engine":"custom"}`), nil
}

func (r *runtimeEngineCodec) Unmarshal(bytes []byte, dest interface{}, _ ...interface{}) error {
	target := dest.(*map[string]interface{})
	*target = map[string]interface{}{"engine": "custom"}
	return nil
}

func TestNewJSONMarshaller_DefaultsToStructology(t *testing.T) {
	cfg := &config.IOConfig{}
	legacy := legacyjson.New(cfg)
	lookup := func(name string, _ ...xreflect.Option) (reflect.Type, error) {
		switch name {
		case legacyJSONEngineTypeName:
			return reflect.TypeOf(LegacyJSONRuntime{}), nil
		default:
			return nil, fmt.Errorf("unknown type %s", name)
		}
	}

	marshaller, unmarshaller, err := newJSONMarshaller(cfg, "", legacy, lookup)

	require.NoError(t, err)
	_, ok := marshaller.(*StructologyJSONRuntime)
	require.True(t, ok)
	_, ok = unmarshaller.(*StructologyJSONRuntime)
	require.True(t, ok)
}

func TestNewJSONMarshaller_UsesExplicitLegacyEngine(t *testing.T) {
	cfg := &config.IOConfig{}
	legacy := legacyjson.New(cfg)
	lookup := func(name string, _ ...xreflect.Option) (reflect.Type, error) {
		switch name {
		case legacyJSONEngineTypeName:
			return reflect.TypeOf(LegacyJSONRuntime{}), nil
		default:
			return nil, fmt.Errorf("unknown type %s", name)
		}
	}

	marshaller, unmarshaller, err := newJSONMarshaller(cfg, legacyJSONEngineTypeName, legacy, lookup)

	require.NoError(t, err)
	_, ok := marshaller.(*LegacyJSONRuntime)
	require.True(t, ok)
	_, ok = unmarshaller.(*LegacyJSONRuntime)
	require.True(t, ok)
}

func TestStructologyMarshaller_MarshalParity(t *testing.T) {
	type eventType struct {
		ID    int
		Type  string
		Extra string `json:"-"`
	}
	type payload struct {
		UserID    int
		CreatedAt time.Time
		Items     []int
		Name      string
		EventType eventType
		Internal  string `internal:"true"`
	}

	cfg := &config.IOConfig{
		CaseFormat: text.CaseFormatLowerUnderscore,
		TimeLayout: "2006-01-02",
	}
	legacy := legacyjson.New(cfg)
	lookup := func(name string, _ ...xreflect.Option) (reflect.Type, error) {
		switch name {
		default:
			return nil, fmt.Errorf("unknown type %s", name)
		}
	}
	marshaller, _, err := newJSONMarshaller(cfg, DefaultJSONEngineTypeName, legacy, lookup)
	require.NoError(t, err)

	value := payload{
		UserID:    7,
		CreatedAt: time.Date(2026, time.March, 5, 0, 0, 0, 0, time.UTC),
		EventType: eventType{ID: 11, Type: "alpha", Extra: "ignored"},
	}
	filters := []*legacyjson.FilterEntry{
		{Fields: []string{"UserID", "CreatedAt", "Items", "EventType"}},
		{Path: "EventType", Fields: []string{"Type"}},
	}

	actual, err := marshaller.Marshal(value, filters)
	require.NoError(t, err)

	expected, err := legacy.Marshal(value, filters)
	require.NoError(t, err)

	require.JSONEq(t, string(expected), string(actual))
	require.JSONEq(t, `{"user_id":7,"created_at":"2026-03-05","items":[],"event_type":{"type":"alpha"}}`, string(actual))
}

func TestStructologyMarshaller_UnmarshalUsesStructologyForBasicCase(t *testing.T) {
	cfg := &config.IOConfig{CaseFormat: text.CaseFormatLowerCamel}
	legacy := legacyjson.New(cfg)
	lookup := func(name string, _ ...xreflect.Option) (reflect.Type, error) {
		switch name {
		default:
			return nil, fmt.Errorf("unknown type %s", name)
		}
	}
	_, unmarshaller, err := newJSONMarshaller(cfg, DefaultJSONEngineTypeName, legacy, lookup)
	require.NoError(t, err)

	type payload struct {
		BuildTimeMs int `json:",omitempty"`
		Changed     bool
	}

	var actual payload
	err = unmarshaller.Unmarshal([]byte(`{"buildTimeMs":12,"changed":true}`), &actual)

	require.NoError(t, err)
	require.Equal(t, 12, actual.BuildTimeMs)
	require.True(t, actual.Changed)
}

func TestStructologyMarshaller_MarshalRejectsLegacyInterceptors(t *testing.T) {
	legacy := legacyjson.New(&config.IOConfig{})
	lookup := func(name string, _ ...xreflect.Option) (reflect.Type, error) {
		switch name {
		default:
			return nil, fmt.Errorf("unknown type %s", name)
		}
	}
	marshaller, _, err := newJSONMarshaller(&config.IOConfig{}, DefaultJSONEngineTypeName, legacy, lookup)
	require.NoError(t, err)

	type payload struct {
		Total int
	}

	_, err = marshaller.Marshal(payload{Total: 3}, legacyjson.MarshalerInterceptors{
		"Total": func() ([]byte, error) {
			return []byte(`3`), nil
		},
	})

	require.ErrorContains(t, err, "does not support legacy marshal interceptors")
}

func TestStructologyMarshaller_UnmarshalRejectsLegacyInterceptors(t *testing.T) {
	legacy := legacyjson.New(&config.IOConfig{})
	lookup := func(name string, _ ...xreflect.Option) (reflect.Type, error) {
		switch name {
		default:
			return nil, fmt.Errorf("unknown type %s", name)
		}
	}
	_, unmarshaller, err := newJSONMarshaller(&config.IOConfig{}, DefaultJSONEngineTypeName, legacy, lookup)
	require.NoError(t, err)

	type payload struct {
		Total int
	}

	var actual payload
	err = unmarshaller.Unmarshal([]byte(`{"Total":3}`), &actual, legacyjson.UnmarshalerInterceptors{
		"Total": func(dst interface{}, decoder *gojay.Decoder, options ...interface{}) error {
			return decoder.Int(dst.(*int))
		},
	})

	require.ErrorContains(t, err, "does not support legacy unmarshal interceptors")
}

func TestNewJSONMarshaller_UsesRegisteredEngineType(t *testing.T) {
	cfg := &config.IOConfig{}
	legacy := legacyjson.New(cfg)
	lookup := func(name string, _ ...xreflect.Option) (reflect.Type, error) {
		switch name {
		case "pkg.CustomJSONEngine":
			return reflect.TypeOf(runtimeEngineCodec{}), nil
		default:
			return nil, fmt.Errorf("unknown type %s", name)
		}
	}

	marshaller, unmarshaller, err := newJSONMarshaller(cfg, "pkg.CustomJSONEngine", legacy, lookup)

	require.NoError(t, err)
	actual, err := marshaller.Marshal(struct{}{})
	require.NoError(t, err)
	require.JSONEq(t, `{"engine":"custom"}`, string(actual))

	var out map[string]interface{}
	err = unmarshaller.Unmarshal([]byte(`{}`), &out)
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{"engine": "custom"}, out)
}
