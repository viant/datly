package http

import (
	"context"
	"net/http/httptest"
	"reflect"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/output"
	"github.com/viant/datly/spec"
)

func TestTypedErrorUsesDeclaredJSONContract(t *testing.T) {
	type violation struct {
		Field   string
		Message string
	}
	type result struct {
		Violations []violation
		Items      []string
	}
	for _, casing := range []string{"", "lc"} {
		for _, compressed := range []bool{false, true} {
			t.Run(casing+"/gzip="+strconv.FormatBool(compressed), func(t *testing.T) {
				component := &spec.Component{Settings: &spec.Settings{CaseFormat: casing}}
				if compressed {
					component.Settings.ResponseCompression = &spec.ResponseCompression{Encoding: "gzip", MinSizeBytes: 1}
				}
				plan, err := (output.Compiler{}).Compile(output.CompileInput{Component: component, Type: reflect.TypeFor[result]()})
				require.NoError(t, err)
				rec := httptest.NewRecorder()
				(&Handler{}).writeHTTPJSON(context.Background(), rec, httptest.NewRequest("GET", "/records", nil), 400, &result{Violations: []violation{{Field: "Name", Message: "required"}}}, plan)
				require.Equal(t, 400, rec.Code)
				require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
				body := rec.Body.Bytes()
				if compressed {
					require.Equal(t, "gzip", rec.Header().Get("Content-Encoding"))
					require.Equal(t, strconv.Itoa(len(body)), rec.Header().Get("Content-Length"))
					body = decodeGzip(t, body)
				}
				want := `{"Violations":[{"Field":"Name","Message":"required"}],"Items":null}`
				if casing == "lc" {
					want = `{"violations":[{"field":"Name","message":"required"}],"items":null}`
				}
				require.JSONEq(t, want, string(body))
			})
		}
	}
}
