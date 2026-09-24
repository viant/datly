package runtime

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/handler/provider/clients"
	"github.com/viant/datly/runtime/registry"
	remotecore "github.com/viant/datly/runtime/remote"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	sqlreader "github.com/viant/datly/sql/reader"
	xhttp "github.com/viant/xdatly/client/http"
	xhandler "github.com/viant/xdatly/handler"
)

type criteriaAllowed struct{ Entity []int }
type criteriaAuthContext struct{ Allowed *criteriaAllowed }
type criteriaAuthOutput struct{ Context *criteriaAuthContext }
type criteriaAuthInput struct {
	Token  string            `parameter:"Token,kind=header,in=Authorization,required"`
	Remote remotecore.Config `parameter:"Remote,kind=const,in=Remote"`
	HTTP   xhttp.Provider    `bind:"kind=http_client,required"`
}
type criteriaGatedInput struct {
	Auth    *criteriaAuthOutput  `parameter:"Auth,kind=component,in=GET:/criteria-auth,required"`
	Context *criteriaAuthContext `parameter:"Context,kind=param,in=Auth.Context,required"`
}

func TestRemoteContextDirectCriteriaInSQLite(t *testing.T) {
	ctx := context.Background()
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE projects(id INTEGER)", "INSERT INTO projects VALUES(101),(102),(103),(104)"); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Header.Get("Authorization") {
		case "Bearer alice":
			fmt.Fprint(w, `{"ids":[101,102]}`)
		case "Bearer bob":
			fmt.Fprint(w, `{"ids":[103]}`)
		case "Bearer empty":
			fmt.Fprint(w, `{"ids":[]}`)
		case "Bearer malformed":
			fmt.Fprint(w, `{"ids":["101) OR 1=1 --"]}`)
		default:
			w.WriteHeader(http.StatusForbidden)
		}
	}))
	defer server.Close()
	configuration := fmt.Sprintf(`{"client":{"transport":"http","http":{"url":%q,"method":"GET"}},"request":[{"input":"Token","header":"Authorization"}],"response":{"mappings":[{"path":"/ids","output":"Context.Allowed.Entity"}]}}`, server.URL)
	auth := componentSpec("CriteriaAuth", "GET", "/criteria-auth", []*spec.Parameter{
		{Name: "Remote", Source: spec.BindSource{Kind: "const", Name: "Remote"}, Value: &configuration},
	})
	authArtifact := componentArtifact(t, auth, reflect.TypeFor[criteriaAuthInput](), reflect.TypeFor[criteriaAuthOutput]())
	type row struct {
		ID int `sqlx:"id"`
	}
	type output struct{ Data []row }
	consumer := componentSpec("CriteriaProjects", "GET", "/criteria-projects", []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}})
	consumer.RootView = &spec.View{Source: &spec.ViewSource{SQL: `SELECT t.id FROM projects t WHERE $criteria.In("t.id", $Context.Allowed.Entity) ORDER BY t.id`}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: consumer, InputType: reflect.TypeFor[criteriaGatedInput](), OutputType: reflect.TypeFor[output](), DirectViewField: "Data"})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := sqlreader.NewExecution(sqlreader.Config{Component: artifact.Component, InputType: reflect.TypeFor[criteriaGatedInput](), OutputType: reflect.TypeFor[output](), Plan: artifact.Reader, SQL: &dsql.SQLComponent{DB: h.DB}})
	if err != nil {
		t.Fatal(err)
	}
	// Explicit composition: the test owns the default registry and registers
	// its public providers on the component; the handler only borrows them.
	owned := clients.New()
	defer owned.Close()
	mapper := remotecore.NewMapper()
	handler := custom.New(xhandler.ContractFunc[criteriaAuthInput, criteriaAuthOutput](
		func(ctx context.Context, _ xhandler.Session, input *criteriaAuthInput, output *criteriaAuthOutput) error {
			return mapper.HTTP(ctx, &input.Remote, input.HTTP, nil, input, output)
		}))
	rt, err := NewRuntime([]*registry.RegisteredComponent{
		{Component: authArtifact.Component, Input: authArtifact.Input, OutputType: reflect.TypeFor[criteriaAuthOutput](), Handler: handler, Providers: owned.Providers()},
		{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeFor[output](), Reader: reader},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Shutdown(ctx)
	for _, test := range []struct {
		principal string
		want      []row
		denied    bool
	}{
		{"alice", []row{{101}, {102}}, false},
		{"bob", []row{{103}}, false},
		{"empty", nil, false},
		{"malformed", nil, true},
		{"denied", nil, true},
	} {
		t.Run(test.principal, func(t *testing.T) {
			for _, forged := range []bool{false, true} {
				request := testharness.NewRequest(http.MethodGet, "/criteria-projects").WithHeaders(http.Header{"Authorization": {"Bearer " + test.principal}})
				if forged {
					request = request.WithQuery(url.Values{"Auth": {`{"Context":{"Allowed":{"Entity":[104]}}}`}, "Context": {`{"Allowed":{"Entity":[104]}}`}, "Context.Allowed.Entity": {"104"}})
				}
				result, err := executeTestRoute(t, rt, ctx, request)
				if test.denied {
					if err == nil {
						t.Fatal("denied or malformed context reached SQL")
					}
					continue
				}
				if err != nil {
					t.Fatal(err)
				}
				rows := result.(*output).Data
				if len(rows) != len(test.want) || (len(rows) > 0 && !reflect.DeepEqual(rows, test.want)) {
					t.Fatalf("forged=%v rows=%v want=%v", forged, rows, test.want)
				}
			}
		})
	}
}
