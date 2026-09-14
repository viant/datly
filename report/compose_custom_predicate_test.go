package report

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"

	httpgateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/report/cubecompose"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/x"
	xpredicate "github.com/viant/xdatly/predicate"
)

type composeObservationKey struct{}
type composeObservation struct {
	sync.Mutex
	frames   []cubecompose.FrameContext
	channels []string
}
type composeChannelPredicate struct {
	Input *groupedSpendInput `bind:"kind=input,required"`
}

func (p *composeChannelPredicate) Compute(ctx context.Context, value any) (*xpredicate.Criteria, error) {
	frame, ok := cubecompose.FrameContextFrom(ctx)
	if !ok || frame.Snapshot.IsZero() {
		return nil, fmt.Errorf("missing composition frame context")
	}
	if p.Input == nil || p.Input.Channel != value || p.Input.Tenant != "acme" {
		return nil, fmt.Errorf("wrong frame component input")
	}
	observation, ok := ctx.Value(composeObservationKey{}).(*composeObservation)
	if !ok {
		return nil, fmt.Errorf("caller context lost")
	}
	observation.Lock()
	observation.frames = append(observation.frames, frame)
	observation.channels = append(observation.channels, p.Input.Channel)
	observation.Unlock()
	return &xpredicate.Criteria{Expression: "s.channel = ?", Placeholders: []any{p.Input.Channel}}, nil
}

func TestCubeComposeCustomPredicateInputAndFrameContext(t *testing.T) {
	h := (groupedReportHarnessConfig{
		report: []func(*spec.ReportSettings){func(s *spec.ReportSettings) { s.Compose = &spec.CubeComposeSettings{Enabled: true} }},
		source: func(component *spec.Component, types *typecatalog.Catalog) {
			if err := types.Register(typecatalog.TypeOriginPackage, &x.Type{Type: reflect.TypeOf(composeChannelPredicate{}), PkgPath: "example.com/acme/reporting", Name: "ComposeChannelPredicate"}); err != nil {
				t.Fatal(err)
			}
			for _, p := range component.Parameters {
				if p.Name == "Channel" {
					p.Predicates = []*spec.Predicate{{Name: "handler", Args: []string{"example.com/acme/reporting.ComposeChannelPredicate"}}}
				}
			}
		},
	}).build(t)
	const body = `{"cubes":[{"filters":{"accountIDs":"1","tenant":"acme","region":"EU","channel":"web","status":"active"}},{"inheritFrom":1,"align":"elapsed","filters":{"channel":"store"}}],"sql":"SELECT t1.AccountID, t1.TotalSpend AS web, t2.TotalSpend AS store FROM $CubeSQL1 AS t1 JOIN $CubeSQL2 AS t2 ON t1.AccountID = t2.AccountID"}`
	for _, protocol := range []string{"HTTP", "MCP"} {
		t.Run(protocol, func(t *testing.T) {
			observation := &composeObservation{}
			ctx := context.WithValue(context.Background(), composeObservationKey{}, observation)
			if protocol == "HTTP" {
				request := httptest.NewRequest("POST", "/spend/acme/cube/compose", strings.NewReader(body)).WithContext(ctx)
				request.Header.Set("Content-Type", "application/json")
				recorder := httptest.NewRecorder()
				httpgateway.NewHandler(h.runtime, nil, "").ServeHTTP(recorder, request)
				if recorder.Code != 200 {
					t.Fatalf("HTTP %d: %s", recorder.Code, recorder.Body.String())
				}
			} else {
				var arguments map[string]any
				if err := json.Unmarshal([]byte(body), &arguments); err != nil {
					t.Fatal(err)
				}
				tool, ok := h.service.Registry().ToolRegistry.Get("SpendCubeCompose")
				if !ok {
					t.Fatal("missing MCP compose tool")
				}
				result, err := tool.Handler(ctx, &schema.CallToolRequest{Params: schema.CallToolRequestParams{Name: "SpendCubeCompose", Arguments: arguments}})
				if err != nil || result == nil || result.IsError != nil && *result.IsError {
					t.Fatalf("MCP result=%+v error=%v", result, err)
				}
			}
			observation.Lock()
			defer observation.Unlock()
			if len(observation.frames) != 2 || !reflect.DeepEqual(observation.channels, []string{"web", "store"}) {
				t.Fatalf("frames=%+v channels=%v", observation.frames, observation.channels)
			}
			a, b := observation.frames[0], observation.frames[1]
			if a.Frame != 1 || b.Frame != 2 || a.Alignment != "" || b.Alignment != "elapsed" || !a.Snapshot.Equal(b.Snapshot) {
				t.Fatalf("inconsistent frame context: %+v %+v", a, b)
			}
		})
	}
}
