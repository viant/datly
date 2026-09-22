package output

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	structjson "github.com/viant/structology/encoding/json"
	"github.com/viant/xdatly/response"
)

func TestCaseFormatSelectionUsesCanonicalFieldIdentity(t *testing.T) {
	type row struct {
		SampleSeen_1Day int
		SampleSeen_7Day int
		Null            *string
		Zero            int
		False           bool
		Empty           string
	}
	type envelope struct {
		response.Status
		Data    []row
		Metrics []string
	}
	value := &envelope{Status: response.Status{Status: "ok"}, Data: []row{{SampleSeen_1Day: 11, SampleSeen_7Day: 77}}, Metrics: []string{}}
	plan, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeOf(value), Component: &spec.Component{Settings: &spec.Settings{CaseFormat: "lc"}}})
	require.NoError(t, err)
	for _, tc := range []struct{ name, field, want string }{
		{"day", "SampleSeen_1Day", `{"status":"ok","data":[{"sampleSeen_Day":11,"null":null,"zero":0,"false":false,"empty":""}],"metrics":[]}`},
		{"week", "SampleSeen_7Day", `{"status":"ok","data":[{"sampleSeen_Day":77,"null":null,"zero":0,"false":false,"empty":""}],"metrics":[]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			filter, err := structjson.NewFieldFilter(reflect.TypeOf(value), []structjson.FieldSelection{{Path: []string{"Data"}, Fields: []string{tc.field, "Null", "Zero", "False", "Empty"}}})
			require.NoError(t, err)
			ctx := dexec.CaptureOutputSelection(context.Background())
			scoped, complete := dexec.ScopeOutputSelection(ctx)
			dexec.BeginOutputSelection(scoped)
			dexec.PublishOutputSelection(scoped, value, filter)
			complete(value, nil)
			encoded, err := plan.Encode(ctx, "json", value)
			require.NoError(t, err)
			require.JSONEq(t, tc.want, string(encoded.Data))
			require.Equal(t, 1, strings.Count(string(encoded.Data), `"sampleSeen_Day":`))
		})
	}
}
