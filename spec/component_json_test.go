package spec

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestComponentJSONPreservesParamsKey(t *testing.T) {
	data, err := json.Marshal(Component{
		Name:       "Search",
		Parameters: []*Parameter{{Name: "Term", Source: BindSource{Kind: "query", Name: "q"}}},
	})
	if err != nil {
		t.Fatalf("marshal component: %v", err)
	}
	actual := string(data)
	if !strings.Contains(actual, `"params":[`) {
		t.Fatalf("component JSON must retain the params key: %s", actual)
	}
	if strings.Contains(actual, `"parameters"`) {
		t.Fatalf("component JSON must not rename its serialized params key: %s", actual)
	}
}
