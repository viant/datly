package reader

import (
	"context"
	"net/url"
	"reflect"
	"strconv"
	"testing"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/transcribe"
)

type derivedAuthoringInput struct {
	TenantID int `parameter:"TenantID,kind=query,in=tenantId,required=true"`
	Offset   int `parameter:"Offset,kind=query,in=offset" querySelector:"Records"`
}
type derivedAuthoringRow struct {
	ID   int    `json:"id" sqlx:"id"`
	Name string `json:"name" sqlx:"name"`
}
type derivedAuthoringTotals struct {
	Count int `json:"count" sqlx:"count"`
}
type derivedAuthoringBounds struct {
	Minimum *int `json:"minimum" sqlx:"minimum"`
	Maximum *int `json:"maximum" sqlx:"maximum"`
}
type derivedAuthoringOutput struct {
	Data   []*derivedAuthoringRow  `json:"data" parameter:"Data,kind=output,in=view" view:"Records,limit=1,selectorOffset=true" sql:"SELECT id,name FROM records WHERE tenant_id=:TenantID ORDER BY id"`
	Totals *derivedAuthoringTotals `json:"totals" parameter:"Totals,kind=output,in=derived" view:"Totals" sql:"SELECT COUNT(*) AS count FROM ($View.Records.NonWindowSQL) parent"`
	Bounds *derivedAuthoringBounds `json:"bounds" parameter:"Bounds,kind=output,in=derived" view:"Bounds,allowNulls=true" sql:"SELECT MIN(id) AS minimum,MAX(id) AS maximum FROM ($View.Records.NonWindowSQL) parent"`
}

func TestDerivedAuthoringKeepsCountOutsidePageSQLite(t *testing.T) {
	for _, mode := range []string{"Go shapes", "DQL with linked output", "DQL with authored source limit"} {
		for _, tc := range []struct {
			name                                 string
			tenant, offset, row, count, min, max int
		}{
			{"first page", 7, 0, 1, 3, 1, 3},
			{"second page", 7, 1, 2, 3, 1, 3},
			{"empty page, nonempty match", 7, 10, 0, 3, 1, 3},
			{"different tenant", 8, 0, 20, 1, 20, 20},
			{"no matching records", 9, 0, 0, 0, 0, 0},
		} {
			// Isolate the authored-source-window contract from dynamic offset
			// precedence: the page/offset cases above exercise view pagination.
			if mode == "DQL with authored source limit" && tc.name != "first page" {
				continue
			}
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				ctx := context.Background()
				h := sqlite.New(t)
				if err := h.ExecStatements(ctx, "CREATE TABLE records(tenant_id INTEGER,id INTEGER,name TEXT)", "INSERT INTO records VALUES(7,1,'one'),(7,2,'two'),(7,3,'three'),(8,20,'other')"); err != nil {
					t.Fatal(err)
				}
				component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/app/records", Name: "Records"}, Name: "Records"}
				if mode != "Go shapes" {
					source := &transcribe.Source{Scope: component.Key.Scope, Name: "Records", Connector: "main", Text: `#setting($_ = $route('/records','GET'))
#setting($_ = $connector('main'))
#define($_ = $TenantID<int>(query/tenantId).Required())
#define($_ = $Offset<int>(query/offset).Optional().QuerySelector('Records'))
#define($_ = $Data<?>(output/view))
#define($_ = $Totals<Totals>(output/derived) /* SELECT COUNT(*) AS count FROM ($View.Records.NonWindowSQL) parent */)
#define($_ = $Bounds<Bounds>(output/derived) /* SELECT MIN(id) AS minimum,MAX(id) AS maximum,allow_nulls(parent) FROM ($View.Records.NonWindowSQL) parent */)
SELECT r.id,r.name,set_limit(r,1) FROM records r WHERE r.tenant_id=:TenantID ORDER BY r.id`}
					if mode == "DQL with authored source limit" {
						source.Text += " LIMIT 1"
					}
					compiled, err := transcribe.NewCompiler().Compile(ctx, source)
					if err != nil {
						t.Fatal(err)
					}
					component = compiled.Component
				}
				artifact, err := buildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeOf(derivedAuthoringInput{}), OutputType: reflect.TypeOf(derivedAuthoringOutput{})})
				if err != nil {
					t.Fatal(err)
				}
				if artifact.Component.RootView.Name != "Records" || len(artifact.Component.RootView.Relations) != 2 {
					t.Fatalf("root=%+v", artifact.Component.RootView)
				}
				actual, err := NewService().Read(ctx, &Session{Component: artifact.Component, OutputType: reflect.TypeOf(derivedAuthoringOutput{}), Input: routeInput(t, artifact), Artifact: artifact.Reader, SQL: &dsql.SQLComponent{DB: h.DB}, Scope: testharness.Request{}.WithQuery(url.Values{"tenantId": {strconv.Itoa(tc.tenant)}, "offset": {strconv.Itoa(tc.offset)}})})
				if err != nil {
					t.Fatal(err)
				}
				out := actual.(*derivedAuthoringOutput)
				count, maximum := tc.count, tc.max
				if mode == "DQL with authored source limit" && tc.tenant == 7 {
					count, maximum = 1, 1
				}
				if out.Totals == nil || out.Totals.Count != count || out.Bounds == nil {
					t.Fatalf("output=%+v", out)
				}
				if tc.row == 0 {
					if len(out.Data) != 0 {
						t.Fatalf("rows=%+v", out.Data)
					}
				} else if len(out.Data) != 1 || out.Data[0].ID != tc.row {
					t.Fatalf("rows=%+v", out.Data)
				}
				if tc.count == 0 {
					if out.Bounds.Minimum != nil || out.Bounds.Maximum != nil {
						t.Fatal("aggregate NULL was lost")
					}
				} else if out.Bounds.Minimum == nil || out.Bounds.Maximum == nil || *out.Bounds.Minimum != tc.min || *out.Bounds.Maximum != maximum {
					t.Fatalf("bounds=%+v", out.Bounds)
				}
				h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT COUNT(*) AS count FROM records"}, []struct{ Count int }{{4}})
			})
		}
	}
}
