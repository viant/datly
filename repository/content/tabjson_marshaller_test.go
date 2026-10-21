package content

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/xreflect"
)

var legacyTabularJSONEngineTypeName = reflect.TypeOf(LegacyTabularJSONRuntime{}).PkgPath() + "/" + reflect.TypeOf(LegacyTabularJSONRuntime{}).Name()

func TestNewTabularJSONMarshaller_DefaultsToStructology(t *testing.T) {
	lookup := func(name string, _ ...xreflect.Option) (reflect.Type, error) {
		switch name {
		case legacyTabularJSONEngineTypeName:
			return reflect.TypeOf(LegacyTabularJSONRuntime{}), nil
		default:
			return nil, fmt.Errorf("unknown type %s", name)
		}
	}

	marshaller, unmarshaller, err := newTabularJSONMarshaller(&TabularJSONConfig{}, reflect.TypeOf(struct{}{}), reflect.TypeOf(struct{}{}), nil, lookup)

	require.NoError(t, err)
	_, ok := marshaller.(*StructologyTabularJSONRuntime)
	require.True(t, ok)
	_, ok = unmarshaller.(*StructologyTabularJSONRuntime)
	require.True(t, ok)
}

func TestNewTabularJSONMarshaller_UsesExplicitLegacyEngine(t *testing.T) {
	lookup := func(name string, _ ...xreflect.Option) (reflect.Type, error) {
		switch name {
		case legacyTabularJSONEngineTypeName:
			return reflect.TypeOf(LegacyTabularJSONRuntime{}), nil
		default:
			return nil, fmt.Errorf("unknown type %s", name)
		}
	}

	cfg := &TabularJSONConfig{Engine: legacyTabularJSONEngineTypeName}
	marshaller, unmarshaller, err := newTabularJSONMarshaller(cfg, reflect.TypeOf(struct{}{}), reflect.TypeOf(struct{}{}), nil, lookup)

	require.NoError(t, err)
	_, ok := marshaller.(*LegacyTabularJSONRuntime)
	require.True(t, ok)
	_, ok = unmarshaller.(*LegacyTabularJSONRuntime)
	require.True(t, ok)
}

func TestStructologyTabularJSONRuntime_MarshalPrecisionAndNested(t *testing.T) {
	type child struct {
		ID int `csvName:"id"`
	}
	type row struct {
		ID       int     `csvName:"id"`
		Price    float64 `csvName:"price"`
		Children []child `csvName:"children"`
	}
	cfg := &TabularJSONConfig{FloatPrecision: "4"}
	runtime := &StructologyTabularJSONRuntime{}
	require.NoError(t, runtime.InitTabularJSONRuntime(cfg, nil, nil, nil))

	actual, err := runtime.Marshal([]row{{ID: 1, Price: 1.123456, Children: []child{{ID: 10}, {ID: 11}}}})
	require.NoError(t, err)
	require.JSONEq(t, `[["id","price","children"],[1,1.1235,[["id"],[10],[11]]]]`, string(actual))
}

func TestStructologyTabularJSONRuntime_Unmarshal(t *testing.T) {
	type row struct {
		ID   int    `csvName:"id"`
		Name string `csvName:"name"`
	}
	runtime := &StructologyTabularJSONRuntime{}
	require.NoError(t, runtime.InitTabularJSONRuntime(&TabularJSONConfig{}, nil, nil, nil))

	var actual []row
	err := runtime.Unmarshal([]byte(`[["id","name"],[1,"alpha"],[2,"beta"]]`), &actual)
	require.NoError(t, err)
	require.Equal(t, []row{{ID: 1, Name: "alpha"}, {ID: 2, Name: "beta"}}, actual)
}

func TestContentInitTabJSONIfNeeded_DefaultStructology(t *testing.T) {
	type row struct {
		ID int `csvName:"id"`
	}
	content := &Content{}

	err := content.initTabJSONIfNeeded(nil, reflect.TypeOf([]row{}), reflect.TypeOf([]row{}), nil)
	require.NoError(t, err)

	payload, err := content.TabularJSON.OutputMarshaller.Marshal([]row{{ID: 7}})
	require.NoError(t, err)

	var actual [][]interface{}
	require.NoError(t, json.Unmarshal(payload, &actual))
	require.Equal(t, "id", actual[0][0])
	require.EqualValues(t, 7, actual[1][0])
}
