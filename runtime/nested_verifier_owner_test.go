package runtime_test

import (
	"context"
	"encoding/json"
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
	custom "github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/scy/auth/jwt/verifier"
	"github.com/viant/sqlx/testutil/sqlfault"
	xh "github.com/viant/xdatly/handler"
	legacy "github.com/viant/xdatly/handler/response"
)

type nestedOwnerCredentialInput struct {
	Credential *jwt.Claims `parameter:"Credential,kind=header,in=X-Credential,dataType=string,required=true,errorCode=401" codec:"JwtClaim"`
	Logger     xh.Logger   `parameter:"Logger,kind=logger,required" json:"-"`
}

func (i *nestedOwnerCredentialInput) Init(context.Context) error {
	i.Logger.(*legacyStatusProbe).init++
	return nil
}

type nestedOwnerCredentialOutput struct{}
type nestedOwnerInput struct {
	Auth   *nestedOwnerCredentialOutput `parameter:"Auth,kind=component,in=GET:/identity"`
	Logger xh.Logger                    `parameter:"Logger,kind=logger,required" json:"-"`
}

func (i *nestedOwnerInput) Init(context.Context) error {
	i.Logger.(*legacyStatusProbe).init++
	return nil
}

type nestedOwnerOutput struct {
	legacy.Status
	Data []*legacyStatusRow `json:"data"`
}

func TestNestedVerifierActualNativeCodecHTTP(t *testing.T) {
	key, wrong := testharness.NewJWT(t), testharness.NewJWT(t)
	for _, retain := range []bool{false, true} {
		for _, tc := range []struct {
			name, credential string
			fetch            bool
		}{
			{"missing", "", false},
			{"malformed", "Bearer invalid", false},
			{"expired", key.Sign(t, "record-user", time.Now().Add(-time.Hour)), false},
			{"wrong signature", wrong.Sign(t, "record-user", time.Now().Add(time.Hour)), false},
			{"configured certificate fetch", "non-secret-generic-input", true},
		} {
			t.Run(tc.name+map[bool]string{false: "/discard", true: "/retain"}[retain], func(t *testing.T) {
				cfg := key.Config()
				if tc.fetch {
					cfg = &verifier.Config{CertURL: "private-diagnostic://invalid/GENERIC_PRIVATE_SENTINEL"}
				}
				auth, err := runtimeauth.New(t.Context(), &runtimeauth.Config{RetainFailedCredential: retain, JWTValidator: cfg})
				if err != nil {
					t.Fatal(err)
				}
				h := sqlite.New(t)
				if err := h.ExecStatements(t.Context(), "CREATE TABLE records(id INTEGER)", "INSERT INTO records VALUES(1)"); err != nil {
					t.Fatal(err)
				}
				probe := &legacyStatusProbe{}
				db := h.FaultDB(t, func(_ context.Context, _ sqlfault.Call) error { probe.reads++; return nil })
				c := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/orders", Name: "records"}, Settings: &spec.Settings{CaseFormat: "lc", Output: &spec.OutputSettings{NilSlicePolicy: "empty_array"}}, Routes: []*spec.Route{{Method: "GET", Path: "/records"}}, RootView: &spec.View{Name: "OrderRecordsCase", Source: &spec.ViewSource{SQL: "SELECT id FROM records"}}, Parameters: []*spec.Parameter{{Name: "Status", Source: spec.BindSource{Kind: "body", Name: "status"}, EmitOutput: true}, {Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}}}
				artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: c, InputType: reflect.TypeFor[nestedOwnerInput](), OutputType: reflect.TypeFor[nestedOwnerOutput](), CodecFactory: auth})
				if err != nil {
					t.Fatal(err)
				}
				read, err := reader.NewExecution(reader.Config{Component: artifact.Component, InputType: reflect.TypeFor[nestedOwnerInput](), OutputType: reflect.TypeFor[nestedOwnerOutput](), Plan: artifact.Reader, SQL: &dsql.SQLComponent{DB: db}})
				if err != nil {
					t.Fatal(err)
				}
				child := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/orders", Name: "identity"}, Routes: []*spec.Route{{Method: "GET", Path: "/identity"}}}
				ca, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: child, InputType: reflect.TypeFor[nestedOwnerCredentialInput](), OutputType: reflect.TypeFor[nestedOwnerCredentialOutput](), CodecFactory: auth})
				if err != nil {
					t.Fatal(err)
				}
				native, err := druntime.NewRuntime([]*registry.RegisteredComponent{{Component: ca.Component, Input: ca.Input, Output: ca.Output, OutputType: reflect.TypeFor[nestedOwnerCredentialOutput](), Capabilities: rhandler.InvocationCapabilities{Logger: probe}, Handler: custom.NewFunc[nestedOwnerCredentialInput, nestedOwnerCredentialOutput](func(context.Context, *nestedOwnerCredentialInput) (*nestedOwnerCredentialOutput, error) {
					t.Fatal("rejected child executed")
					return nil, nil
				})}, {Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: reflect.TypeFor[nestedOwnerOutput](), Reader: read, Capabilities: rhandler.InvocationCapabilities{Logger: probe}}})
				if err != nil {
					t.Fatal(err)
				}
				defer native.Shutdown(t.Context())
				request := httptest.NewRequest("GET", "/records", nil)
				if tc.name != "missing" {
					request.Header.Set("X-Credential", tc.credential)
				}
				result := httptest.NewRecorder()
				gateway.NewHandler(native, nil, "opaque-owner").ServeHTTP(result, request)
				var body struct {
					Status, Message string
					Data            []any
					Errors          []*legacy.Error
				}
				wantMessage := "Unauthorized"
				if tc.name == "missing" {
					wantMessage = "parameter Credential is required"
				}
				wantRootMessage := wantMessage
				if tc.name == "missing" {
					wantRootMessage = "Bad Request"
				}
				if result.Code != 401 || json.Unmarshal(result.Body.Bytes(), &body) != nil || body.Status != "error" || body.Message != wantRootMessage || body.Data == nil || len(body.Data) != 0 || len(body.Errors) != 1 {
					t.Fatal("actual nested native envelope failed")
				}
				detail := body.Errors[0]
				if detail.Parameter != "Auth" || detail.View != "OrderRecordsCase" || detail.Code != 401 || detail.Message != wantMessage || detail.Object == nil {
					t.Fatal("actual generic compiled owner metadata changed")
				}
				nested, ok := detail.Object.([]interface{})
				if !ok || len(nested) != 1 {
					t.Fatal("nested credential missing")
				}
				record := nested[0].(map[string]interface{})
				if record["parameter"] != "Credential" || record["message"] != wantMessage || record["statusCode"] != float64(401) {
					t.Fatal("nested credential metadata wrong")
				}
				for _, private := range []string{tc.credential, "GENERIC_PRIVATE_SENTINEL", "private-diagnostic", "failed to transform", "failed to parse"} {
					if private != "" && strings.Contains(result.Body.String(), private) {
						t.Fatal("new public message/detail disclosed private verifier data")
					}
				}
				if probe.reads != 0 || probe.init != 0 {
					t.Fatal("actual codec rejection reached SQL/Init")
				}
				var count int
				if err := h.DB.QueryRow("SELECT count(*) FROM records WHERE id=1").Scan(&count); err != nil || count != 1 {
					t.Fatal("native codec failure changed physical record")
				}
			})
		}
	}
}
