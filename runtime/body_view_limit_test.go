package runtime

import (
	"context"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly/locator"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	customhandler "github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	viewprovider "github.com/viant/datly/sql/reader/provider"
)

type recordViewRequest struct {
	RecordID int `json:"recordId"`
}
type recordViewRow struct {
	Id     int `sqlx:"ID"`
	Status int `sqlx:"STATUS"`
}
type recordViewZeroInput struct {
	Request *recordViewRequest `parameter:",kind=body,required=true"`
	Records []*recordViewRow   `parameter:",kind=view,in=Records" view:"Records,limit=0,selectorDefaultLimit=1" sql:"SELECT d.ID,d.STATUS FROM records d WHERE d.ID >= $criteria.AppendBinding($Unsafe.Request.RecordID) ORDER BY d.ID"`
}
type recordViewUnsetInput struct {
	Request *recordViewRequest `parameter:",kind=body,required=true"`
	Records []*recordViewRow   `parameter:",kind=view,in=Records" view:"Records" sql:"SELECT d.ID,d.STATUS FROM records d WHERE d.ID >= $criteria.AppendBinding($Unsafe.Request.RecordID) ORDER BY d.ID"`
}
type recordViewPositiveInput struct {
	Request *recordViewRequest `parameter:",kind=body,required=true"`
	Records []*recordViewRow   `parameter:",kind=view,in=Records" view:"Records,limit=1" sql:"SELECT d.ID,d.STATUS FROM records d WHERE d.ID >= $criteria.AppendBinding($Unsafe.Request.RecordID) ORDER BY d.ID"`
}
type recordViewLiteralZeroInput struct {
	Request *recordViewRequest `parameter:",kind=body,required=true"`
	Records []*recordViewRow   `parameter:",kind=view,in=Records" view:"Records" sql:"SELECT d.ID,d.STATUS FROM records d WHERE d.ID >= $criteria.AppendBinding($Unsafe.Request.RecordID) ORDER BY d.ID LIMIT 0"`
}
type recordViewResult struct {
	RequestID int
	IDs       []int
}

func TestBodyBoundIndependentViewLimits(t *testing.T) {
	t.Run("zero_limit", func(t *testing.T) {
		runBodyViewProbe(t, func(i *recordViewZeroInput) *recordViewResult { return recordViewResultOf(i.Request, i.Records) }, []int{420, 421})
	})
	t.Run("unset_limit", func(t *testing.T) {
		runBodyViewProbe(t, func(i *recordViewUnsetInput) *recordViewResult { return recordViewResultOf(i.Request, i.Records) }, []int{420, 421})
	})
	t.Run("positive_limit", func(t *testing.T) {
		runBodyViewProbe(t, func(i *recordViewPositiveInput) *recordViewResult { return recordViewResultOf(i.Request, i.Records) }, []int{420})
	})
	t.Run("literal_sql_zero", func(t *testing.T) {
		runBodyViewProbe(t, func(i *recordViewLiteralZeroInput) *recordViewResult { return recordViewResultOf(i.Request, i.Records) }, nil)
	})

}
func recordViewResultOf(request *recordViewRequest, rows []*recordViewRow) *recordViewResult {
	o := &recordViewResult{}
	if request != nil {
		o.RequestID = request.RecordID
	}
	for _, r := range rows {
		o.IDs = append(o.IDs, r.Id)
	}
	return o
}
func runBodyViewProbe[I any](t *testing.T, inspect func(*I) *recordViewResult, expected []int) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(), "CREATE TABLE records(ID INTEGER PRIMARY KEY,STATUS INTEGER)", "INSERT INTO records(ID,STATUS) VALUES(420,1),(421,1)"); err != nil {
		t.Fatal(err)
	}
	c := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/record-view", Name: "BodyView"}, Name: "BodyView", Routes: []*spec.Route{{Method: http.MethodPost, Path: "/record-view"}}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: c, InputType: reflect.TypeFor[I](), OutputType: reflect.TypeFor[recordViewResult]()})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("compiled views=%+v parameters=%+v", artifact.Component.Views, artifact.Component.Parameters)
	provider, err := viewprovider.New(viewprovider.Config{Dependencies: artifact.ViewDependencies, Input: artifact.Input, SQL: &dsql.SQLComponent{DB: h.DB}})
	if err != nil {
		t.Fatal(err)
	}
	registered := &registry.RegisteredComponent{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeFor[recordViewResult](), Providers: []locator.Provider{provider}, Handler: customhandler.NewFunc(func(_ context.Context, input *I) (*recordViewResult, error) { return inspect(input), nil })}
	service := independentViewRuntime(t, artifact.Component, registered)
	actual, err := executeTestRoute(t, service, context.Background(), testharness.NewRequest(http.MethodPost, "/record-view").WithBody([]byte(`{"recordId":420}`), "application/json"))
	if err != nil {
		t.Fatal(err)
	}
	output := actual.(*recordViewResult)
	t.Logf("handler requestID=%d readIDs=%v", output.RequestID, output.IDs)
	if output.RequestID != 420 || !reflect.DeepEqual(output.IDs, expected) {
		t.Fatalf("body→view result=%+v want request420 and expected rows", output)
	}
}

func TestExplicitSQLControlsPreserveLiteralZero(t *testing.T) {
	zero, one := 0, 1
	for _, tc := range []struct {
		name, source string
		controls     *spec.ViewControls
		want         string
	}{
		{"spec_zero", "SELECT ID FROM records", &spec.ViewControls{Limit: &zero}, "LIMIT 0"},
		{"pagination_token_zero", "SELECT ID FROM records $PAGINATION", &spec.ViewControls{Limit: &zero}, "LIMIT 0"},
		{"authored_sql_zero", "SELECT ID FROM records LIMIT 0", nil, "LIMIT 0"},
		{"positive_control", "SELECT ID FROM records", &spec.ViewControls{Limit: &one}, "LIMIT 1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := dsql.PrepareExecutableSQL(tc.source, tc.controls)
			t.Logf("SQL=%s", got)
			if !strings.Contains(got, tc.want) {
				t.Fatalf("SQL=%q want %q", got, tc.want)
			}
		})
	}
}
