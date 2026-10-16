package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/viant/datly/bootstrap"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	druntime "github.com/viant/datly/runtime"
	runtimeauth "github.com/viant/datly/runtime/auth"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/sqlx/testutil/sqlfault"
	xh "github.com/viant/xdatly/handler"
	legacy "github.com/viant/xdatly/handler/response"
	modern "github.com/viant/xdatly/response"
)

type legacyStatusProbe struct {
	init, finalize, reads int
	observed              string
	cause                 error
}

func (*legacyStatusProbe) Debug(string, ...any) {}
func (*legacyStatusProbe) Info(string, ...any)  {}
func (*legacyStatusProbe) Warn(string, ...any)  {}
func (*legacyStatusProbe) Error(string, ...any) {}

type legacyStatusInput struct {
	Jwt      *jwt.Claims `parameter:"Jwt,kind=header,in=Authorization,dataType=string,required=true,errorCode=401" codec:"JwtClaim"`
	Scenario string      `parameter:"Scenario,kind=query,in=scenario"`
	Logger   xh.Logger   `parameter:"Logger,kind=logger,required" json:"-"`
}

func (i *legacyStatusInput) Init(context.Context) error {
	p := i.Logger.(*legacyStatusProbe)
	p.init++
	if i.Scenario == "forbidden" {
		return &modern.Error{Code: 403, Cause: errors.New("source access denied"), Payload: &legacyStatusOutput{Status: legacy.Status{Status: "error", Message: "source access denied", Errors: []string{"source auth"}}}}
	}
	return nil
}

type legacyStatusRow struct {
	ID int `sqlx:"id" json:"id"`
}
type legacyStatusOutput struct {
	legacy.Status `parameter:",kind=output,in=status"`
	Data          []*legacyStatusRow `parameter:"Data,kind=output,in=view" json:"data"`
	Messages      []string           `json:"messages"`
	Logger        xh.Logger          `parameter:"Logger,kind=logger,required" json:"-"`
}

func (o *legacyStatusOutput) Finalize(ctx context.Context, cause error) error {
	p := o.Logger.(*legacyStatusProbe)
	p.finalize++
	p.observed = o.Status.Status
	p.cause = cause
	if cause != nil {
		if p.reads == 0 {
			return cause
		}
		o.Status.Status = "error"
		o.Message = "source SQL failure"
		return &modern.Error{Code: 500, Cause: errors.New(o.Message), Payload: o}
	}
	input := ctx.Value(reflect.TypeFor[*legacyStatusInput]()).(*legacyStatusInput)
	if input.Scenario == "not-found" {
		o.Status.Status = "error"
		o.Message = "not found"
		return &modern.Error{Code: 404, Cause: errors.New(o.Message), Payload: o}
	}
	if input.Scenario == "business-error" {
		o.Status.Status = "error"
		o.Message = "source business failure"
		o.Errors = []string{"source validation"}
		return &modern.Error{Code: 400, Cause: errors.New(o.Message), Payload: o}
	}
	o.Errors = false
	o.Warning = []*legacy.Warning{{Message: "original", Reason: "reason"}}
	var typednil *int
	o.Extras = map[string]interface{}{"sourceZero": 0, "sourceFalse": false, "sourceEmpty": "", "sourceNil": nil, "sourceTypedNil": typednil, "Data": "collision", "Messages": "collision"}
	o.Messages = []string{"source"}
	return nil
}

func TestLegacyStatusOneOwnerNativeGETLifecycleAuthAndWireSQLite(t *testing.T) {
	key := testharness.NewJWT(t)
	other := testharness.NewJWT(t)
	auth, e := runtimeauth.New(t.Context(), &runtimeauth.Config{RetainFailedCredential: true, JWTValidator: key.Config()})
	if e != nil {
		t.Fatal(e)
	}
	good := key.Sign(t, "source-user", time.Now().Add(time.Hour))
	for _, tc := range []struct {
		name, token, query string
		code               int
		early              bool
	}{
		{"populated", good, "SELECT id FROM records", 200, false}, {"empty", good, "SELECT id FROM records WHERE id=0", 200, false},
		{"SQL-failure", good, "SELECT id FROM absent", 500, false}, {"business-error", good, "SELECT id FROM records", 400, false}, {"not-found", good, "SELECT id FROM records WHERE id=0", 404, false},
		{"forbidden", good, "SELECT id FROM records", 403, true}, {"missing", "", "SELECT id FROM records", 401, true}, {"malformed", "Bearer invalid", "SELECT id FROM records", 401, true},
		{"expired", key.Sign(t, "source-user", time.Now().Add(-time.Hour)), "SELECT id FROM records", 401, true}, {"wrong-key", other.Sign(t, "source-user", time.Now().Add(time.Hour)), "SELECT id FROM records", 401, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := sqlite.New(t)
			if e = h.ExecStatements(t.Context(), "CREATE TABLE records(id INTEGER)", "INSERT INTO records VALUES(1)"); e != nil {
				t.Fatal(e)
			}
			probe := &legacyStatusProbe{}
			db := h.FaultDB(t, func(_ context.Context, call sqlfault.Call) error {
				if strings.Contains(strings.ToLower(call.SQL), "select") {
					probe.reads++
				}
				return nil
			})
			c := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "legacy-status-proof", Name: "records"}, Settings: &spec.Settings{CaseFormat: "lc"}, Routes: []*spec.Route{{Method: "GET", Path: "/records"}}, RootView: &spec.View{Name: "records", Source: &spec.ViewSource{SQL: tc.query}}, Parameters: []*spec.Parameter{{Name: "Status", Source: spec.BindSource{Kind: "output", Name: "status"}}, {Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}}}
			a, e := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: c, InputType: reflect.TypeFor[legacyStatusInput](), OutputType: reflect.TypeFor[legacyStatusOutput](), CodecFactory: auth})
			if e != nil {
				t.Fatal(e)
			}
			if a.Reader.OutputStatusField != "Status" {
				t.Fatal("sole original owner lost")
			}
			r, e := reader.NewExecution(reader.Config{Component: a.Component, InputType: reflect.TypeFor[legacyStatusInput](), OutputType: reflect.TypeFor[legacyStatusOutput](), Plan: a.Reader, SQL: &dsql.SQLComponent{DB: db}})
			if e != nil {
				t.Fatal(e)
			}
			rt, e := druntime.NewRuntime([]*registry.RegisteredComponent{{Component: a.Component, Input: a.Input, Output: a.Output, OutputType: reflect.TypeFor[legacyStatusOutput](), Reader: r, Capabilities: rhandler.InvocationCapabilities{Logger: probe}}})
			if e != nil {
				t.Fatal(e)
			}
			defer rt.Shutdown(t.Context())
			rec := httptest.NewRecorder()
			req := httptest.NewRequest("GET", "/records?scenario="+tc.name, nil)
			if tc.token != "" {
				req.Header.Set("Authorization", tc.token)
			}
			gateway.NewHandler(rt, nil, "legacy-status-proof").ServeHTTP(rec, req)
			if rec.Code != tc.code {
				t.Fatalf("HTTP%d: %s", rec.Code, rec.Body.String())
			}
			var wire map[string]json.RawMessage
			if e = json.Unmarshal(rec.Body.Bytes(), &wire); e != nil {
				t.Fatal(e)
			}
			if tc.early {
				if probe.reads != 0 || probe.finalize > 1 || probe.observed == "ok" {
					t.Fatalf("early auth failure read/finalize: %+v", probe)
				}
				if tc.name == "forbidden" && (string(wire["message"]) != `"source access denied"` || string(wire["errors"]) != `["source auth"]`) {
					t.Fatalf("original forbidden payload lost: %s", rec.Body.String())
				}
				return
			}
			if probe.init != 1 || probe.finalize != 1 {
				t.Fatalf("lifecycle duplication: %+v", probe)
			}
			if tc.name == "SQL-failure" {
				if probe.observed == "ok" || probe.cause == nil || string(wire["status"]) != `"error"` || string(wire["message"]) != `"source SQL failure"` {
					t.Fatalf("SQL error/status order lost: %+v %s", probe, rec.Body.String())
				}
				return
			}
			if probe.observed != "ok" || probe.cause != nil {
				t.Fatalf("success before-finalize order lost: %+v", probe)
			}
			if tc.code != 200 {
				if string(wire["status"]) != `"error"` {
					t.Fatalf("business error overwritten: %s", rec.Body.String())
				}
				return
			}
			for k, want := range map[string]string{"status": `"ok"`, "errors": "false", "sourceZero": "0", "sourceFalse": "false", "sourceEmpty": `""`, "sourceNil": "null", "sourceTypedNil": "null", "messages": `["source"]`} {
				if string(wire[k]) != want {
					t.Fatalf("source %s lost: %s", k, rec.Body.String())
				}
			}
			for _, key := range []string{"extras", "nativeStatus", "error", "logger"} {
				if _, ok := wire[key]; ok {
					t.Fatalf("non-source/private %s appeared", key)
				}
			}
			var rows []map[string]int
			if e = json.Unmarshal(wire["data"], &rows); e != nil {
				t.Fatalf("outer Data precedence lost: %s", rec.Body.String())
			}
			if tc.name == "populated" && (len(rows) != 1 || rows[0]["id"] != 1) || tc.name == "empty" && len(rows) != 0 {
				t.Fatal("row result changed")
			}
		})
	}
}
