package report

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	httpgateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/x"
	"github.com/xeipuuv/gojsonschema"
)

type wrapperControls struct {
	Threshold int64  `json:"threshold"`
	Metric    int    `json:"metric" choice:"1" choice:"2" validate:"choice(1,2)"`
	Label     string `json:"label" choice:"alpha" choice:"beta" validate:"choice(alpha,beta)"`
	Optional  *int   `json:"optional,omitempty"`
}

const authoredWrapperControls = "struct { Threshold int64 `json:\"threshold\"`; Metric int `json:\"metric\" choice:\"1\" choice:\"2\" validate:\"choice(1,2)\"`; Label string `json:\"label\" choice:\"alpha\" choice:\"beta\" validate:\"choice(alpha,beta)\"`; Optional *int `json:\"optional,omitempty\"` }"

func TestComposeScalarValuesNativeTransports(t *testing.T) {
	t.Run("linked", func(t *testing.T) { testComposeScalarValuesNativeTransports(t, false) })
	t.Run("authored shape", func(t *testing.T) { testComposeScalarValuesNativeTransports(t, true) })
}

func testComposeScalarValuesNativeTransports(t *testing.T, inline bool) {
	h := (groupedReportHarnessConfig{report: []func(*spec.ReportSettings){func(s *spec.ReportSettings) {
		s.Compose = &spec.CubeComposeSettings{Enabled: true, ValuesType: "WrapperControls"}
		if inline {
			s.Compose.ValuesType = authoredWrapperControls
		}
	}}, source: func(c *spec.Component, types *typecatalog.Catalog) {
		if inline {
			return
		}
		if err := types.Register(typecatalog.TypeOriginPackage, &x.Type{Type: reflect.TypeFor[wrapperControls](), PkgPath: c.Key.Scope, Name: "WrapperControls"}); err != nil {
			t.Fatal(err)
		}
	}}).build(t)
	if _, err := h.sql.DB.ExecContext(context.Background(), `INSERT INTO report_spend VALUES ('acme',1,'ZZ','web','active',600),('acme',1,'ZZ','web','active',600),('other',1,'ZZ','web','active',100000)`); err != nil {
		t.Fatal(err)
	}
	tool, ok := h.service.Registry().ToolRegistry.Get("SpendCubeCompose")
	if !ok {
		t.Fatal("missing tool")
	}
	target, ok := h.runtime.ComponentTargetByMethodPath("POST", "/spend/acme/cube/compose")
	if !ok {
		t.Fatal("missing native target")
	}
	registered, err := h.runtime.LoadComponent(context.Background(), target.Component)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := PrepareComposition(registered.Handler, CompositionDefinition{SQL: "SELECT t1.TotalSpend AS amount FROM $CubeSQL1 AS t1 WHERE t1.TotalSpend > $Values.threshold ORDER BY amount DESC LIMIT 5", FrameCount: 1})
	if err != nil || prepared.ValuesType() == nil || prepared.ValuesType().Kind() != reflect.Struct || prepared.ValuesType().Field(0).Type != reflect.TypeFor[int64]() {
		t.Fatalf("prepared scalar contract lost: %v %+v", err, prepared)
	}
	names := prepared.ValueNames()
	if !reflect.DeepEqual(names, []string{"threshold"}) {
		t.Fatalf("unexpected referenced values: %v", names)
	}
	names[0] = "mutated"
	if !reflect.DeepEqual(prepared.ValueNames(), []string{"threshold"}) {
		t.Fatal("prepared value names share mutable storage")
	}
	casePrepared, caseErr := PrepareComposition(registered.Handler, CompositionDefinition{SQL: "SELECT CASE WHEN $Values.metric = 1 THEN t1.TotalSpend ELSE 0 - t1.TotalSpend END AS amount FROM $CubeSQL1 AS t1 WHERE t1.TotalSpend > $Values.threshold ORDER BY amount DESC LIMIT 5", FrameCount: 1})
	if caseErr != nil || !reflect.DeepEqual(casePrepared.ValueNames(), []string{"metric", "threshold"}) {
		t.Fatalf("CASE references: %v %v", casePrepared, caseErr)
	}
	if _, err = PrepareComposition(registered.Handler, CompositionDefinition{SQL: "SELECT t1.TotalSpend FROM $CubeSQL1 AS t1 WHERE t1.TotalSpend > $Values.unknown", FrameCount: 1}); err == nil {
		t.Fatal("undeclared prepared value admitted")
	}
	const sql = `SELECT t1.Region AS key,t1.TotalSpend AS amount,$Values.label AS label,$Values.metric AS metric FROM $CubeSQL1 AS t1 WHERE t1.TotalSpend >= $Values.threshold ORDER BY amount DESC LIMIT 10`
	for _, protocol := range []string{"HTTP", "MCP", "MCP number tokens"} {
		t.Run(protocol, func(t *testing.T) {
			for _, tc := range []struct {
				values string
				count  int
				bad    bool
			}{
				{`{"threshold":1000,"metric":1,"label":"alpha"}`, 1, false},
				{`{"threshold":1300,"metric":2,"label":"beta"}`, 0, false},
				{`{"threshold":9007199254740993,"metric":1,"label":"alpha"}`, 0, false},
				{`{"threshold":0,"metric":1,"label":"alpha"}`, 1, false},
				{`{"threshold":1000,"metric":3,"label":"alpha"}`, 0, true},
				{`{"threshold":1000,"metric":1,"label":"invalid"}`, 0, true},
				{`{"threshold":"1000","metric":1,"label":"alpha"}`, 0, true},
				{`{"threshold":1000.5,"metric":1,"label":"alpha"}`, 0, true},
				{`{"metric":1,"label":"alpha"}`, 0, true},
				{`{"threshold":null,"metric":1,"label":"alpha"}`, 0, true},
				{`{"threshold":1000,"metric":1,"label":"alpha","unknown":7}`, 0, true},
				{`{"threshold":1000,"metric":1,"label":"alpha","optional":null}`, 0, true},
				{`{"threshold":1000,"metric":1,"label":"alpha","Threshold":7}`, 0, true},
			} {
				args := map[string]any{"cubes": []any{map[string]any{"filters": map[string]any{"tenant": "acme", "accountIDs": "1", "region": "ZZ", "channel": "web", "status": "active"}}}, "sql": sql, "values": json.RawMessage(tc.values)}
				if protocol == "MCP number tokens" {
					var decoded any
					decoder := json.NewDecoder(strings.NewReader(tc.values))
					decoder.UseNumber()
					if err := decoder.Decode(&decoded); err != nil {
						t.Fatal(err)
					}
					args["values"] = decoded
				}
				var payload []byte
				var bad bool
				if protocol == "HTTP" {
					body, _ := json.Marshal(args)
					req := httptest.NewRequest("POST", "/spend/acme/cube/compose", strings.NewReader(string(body)))
					req.Header.Set("Content-Type", "application/json")
					rec := httptest.NewRecorder()
					httpgateway.NewHandler(h.runtime, nil, "").ServeHTTP(rec, req)
					payload = rec.Body.Bytes()
					bad = rec.Code != 200
					if tc.bad && rec.Code != 400 {
						t.Fatalf("invalid scalar returned %d: %s", rec.Code, payload)
					}
				} else {
					result, err := tool.Handler(context.Background(), &schema.CallToolRequest{Params: schema.CallToolRequestParams{Name: "SpendCubeCompose", Arguments: args}})
					bad = err != nil || result == nil || result.IsError != nil && *result.IsError
					if !bad {
						payload, _ = json.Marshal(result.StructuredContent)
					}
				}
				if bad != tc.bad {
					t.Fatalf("%s bad=%v want=%v payload=%s", tc.values, bad, tc.bad, payload)
				}
				if bad {
					continue
				}
				var out struct {
					Data []struct {
						Amount float64 `json:"amount"`
						Metric int     `json:"metric"`
						Label  string  `json:"label"`
					}
				}
				if err := json.Unmarshal(payload, &out); err != nil {
					t.Fatal(err)
				}
				if len(out.Data) != tc.count {
					t.Fatalf("%s got=%s", tc.values, payload)
				}
				if tc.count > 0 && out.Data[0].Amount != 1200 {
					t.Fatalf("aggregation/authorization changed: %s", payload)
				}
			}
		})
	}
	for _, protocol := range []string{"HTTP", "MCP"} {
		for _, metric := range []int{1, 2} {
			args := map[string]any{"cubes": []any{map[string]any{"filters": map[string]any{"tenant": "acme", "accountIDs": "1", "region": "ZZ", "channel": "web", "status": "active"}}}, "sql": "SELECT CASE $Values.metric WHEN 1 THEN t1.TotalSpend WHEN 2 THEN t1.TotalSpend / 2 ELSE 0 END AS chosen,t1.TotalSpend + $Values.threshold AS adjusted FROM $CubeSQL1 AS t1 WHERE t1.TotalSpend > 0 ORDER BY chosen DESC LIMIT 5", "values": map[string]any{"metric": metric, "threshold": int64(1000)}}
			var payload []byte
			if protocol == "HTTP" {
				body, _ := json.Marshal(args)
				req := httptest.NewRequest("POST", "/spend/acme/cube/compose", strings.NewReader(string(body)))
				req.Header.Set("Content-Type", "application/json")
				rec := httptest.NewRecorder()
				httpgateway.NewHandler(h.runtime, nil, "").ServeHTTP(rec, req)
				if rec.Code != 200 {
					t.Fatalf("CASE scalar HTTP %d %s", rec.Code, rec.Body.String())
				}
				payload = rec.Body.Bytes()
			} else {
				result, err := tool.Handler(context.Background(), &schema.CallToolRequest{Params: schema.CallToolRequestParams{Name: "SpendCubeCompose", Arguments: args}})
				if err != nil || result == nil || result.IsError != nil && *result.IsError {
					t.Fatalf("CASE scalar MCP %v %+v", err, result)
				}
				payload, _ = json.Marshal(result.StructuredContent)
			}
			var out struct {
				Data []struct {
					Chosen   float64 `json:"chosen"`
					Adjusted float64 `json:"adjusted"`
				}
			}
			if err := json.Unmarshal(payload, &out); err != nil {
				t.Fatal(err)
			}
			if len(out.Data) != 1 || out.Data[0].Chosen != 1200/float64(metric) || out.Data[0].Adjusted != 2200 {
				t.Fatalf("CASE/arithmetic scalar result differs: %s", payload)
			}
		}
	}
	// Existing wireType discovery exposes the authored scalar shape and choices,
	// while native execution receives lossless raw JSON for strict admission.
	encoded, _ := json.Marshal(tool.Metadata.InputSchema)
	for _, name := range []string{`"threshold"`, `"metric"`, `"label"`, `"enum"`} {
		if !strings.Contains(string(encoded), name) {
			t.Fatalf("missing %s in %s", name, encoded)
		}
	}
	args := map[string]any{"cubes": []any{map[string]any{"filters": map[string]any{"tenant": "acme"}}}, "sql": sql, "values": map[string]any{"threshold": int64(1000), "metric": 1, "label": "alpha"}} // Static discovery cannot know which SQL references will be used. All
	// controls are optional in discovery; supplied pointer values are nonnull.
	minimal := map[string]any{"cubes": args["cubes"], "sql": "SELECT t1.TotalSpend FROM $CubeSQL1 AS t1", "values": map[string]any{}}
	checked, checkErr := gojsonschema.Validate(gojsonschema.NewGoLoader(tool.Metadata.InputSchema), gojsonschema.NewGoLoader(minimal))
	if checkErr != nil || !checked.Valid() {
		t.Fatalf("unused controls required by schema: %v %+v", checkErr, checked)
	}
	minimal["values"] = map[string]any{"optional": nil}
	checked, checkErr = gojsonschema.Validate(gojsonschema.NewGoLoader(tool.Metadata.InputSchema), gojsonschema.NewGoLoader(minimal))
	if checkErr != nil || checked.Valid() {
		t.Fatalf("schema admitted null scalar: %v %+v", checkErr, checked)
	}

	valid, err := gojsonschema.Validate(gojsonschema.NewGoLoader(tool.Metadata.InputSchema), gojsonschema.NewGoLoader(args))
	if err != nil || !valid.Valid() {
		t.Fatalf("schema does not admit typed controls: %v %+v schema=%s", err, valid, encoded)
	}
}
