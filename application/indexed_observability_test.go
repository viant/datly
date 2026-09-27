package application_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/application"
	"github.com/viant/datly/bootstrap"
	bootstrapindex "github.com/viant/datly/bootstrap/index"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/typecatalog"
)

type observedParentInput struct {
	Child *httpReloadOutput `parameter:"Child,kind=component,in=GET:/child"`
}

type readerSummary struct {
	message string
	fields  map[string]any
}

type readerSummaryLogger struct {
	mu      sync.Mutex
	records []readerSummary
}

func (l *readerSummaryLogger) Debug(string, ...any) {}
func (l *readerSummaryLogger) Warn(string, ...any)  {}
func (l *readerSummaryLogger) Error(string, ...any) {}
func (l *readerSummaryLogger) Info(message string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fields := map[string]any{}
	for i := 0; i+1 < len(args); i += 2 {
		fields[args[i].(string)] = args[i+1]
	}
	l.records = append(l.records, readerSummary{message, fields})
}
func (l *readerSummaryLogger) snapshot() []readerSummary {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]readerSummary(nil), l.records...)
}

func TestApplicationReaderSummariesEagerAndIndexed(t *testing.T) {
	t.Setenv("XDATLY_TRACING_HEADER", "X-Trace-ID")
	for _, indexed := range []bool{false, true} {
		for _, summaries := range []bool{false, true} {
			t.Run(fmt.Sprintf("indexed=%v/summaries=%v", indexed, summaries), func(t *testing.T) {
				ctx := context.Background()
				db := sqlite.New(t)
				require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER)", "INSERT INTO records VALUES(11)"))
				root := t.TempDir()
				(testharness.GeneratedModule{Path: "example.com/observed"}).Write(t, root)
				for _, name := range []string{"parent", "child"} {
					writeFixture(t, root, name+"/holder.go", `package `+name+`
import "github.com/viant/xdatly"
type Input struct{}
type Output struct{}
type Holder struct { Read xdatly.Component[Input,Output] `+"`"+`component:"`+name+`,path=/`+name+`,method=GET"`+"`"+` }
`)
				}
				snapshot, err := (bootstrapindex.Builder{Config: bootstrapindex.Config{BaseDir: root, Include: []string{"example.com/observed/..."}}}).Build(ctx)
				require.NoError(t, err)
				snapshot, err = snapshot.Transform(func(c *spec.Component) {
					if c.Key.Name == "child" {
						c.Routes[0].Internal = true
					}
				})
				require.NoError(t, err)
				loads := 0
				cache := t.TempDir()
				materializer := bootstrapindex.MaterializeFunc(func(_ context.Context, entry *bootstrapindex.Entry, _ bootstrapindex.Resolver) (*bootstrapindex.Loaded, error) {
					loads++
					component := entry.Component.Clone()
					component.Settings = &spec.Settings{Cache: &spec.CacheSettings{Enabled: true, Name: component.Key.Name, Location: cache, TTL: "1m"}}
					component.RootView = &spec.View{Name: component.Key.Name, Source: &spec.ViewSource{SQL: "SELECT id FROM records"}}
					component.Parameters = []*spec.Parameter{{Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}}}
					input := reflect.TypeFor[struct{}]()
					if component.Key.Name == "parent" {
						input = reflect.TypeFor[observedParentInput]()
					}
					artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: input, OutputType: reflect.TypeFor[httpReloadOutput](), DirectViewField: "Rows"})
					if err != nil {
						return nil, err
					}
					reader, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}})
					return &bootstrapindex.Loaded{Registration: &registry.RegisteredComponent{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: reflect.TypeFor[httpReloadOutput](), Reader: reader}}, err
				})
				logger := &readerSummaryLogger{}
				observation := runtime.ObservabilityConfig{}
				if summaries {
					observation.Logger = logger
				}
				manager, err := application.New(nil, application.WithObservability(observation))
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, manager.Shutdown(ctx)) })
				require.NoError(t, manager.Reload(ctx, application.Request{Revision: 1, Compile: func(context.Context, *typecatalog.Catalog) (*application.Build, error) {
					if indexed {
						return &application.Build{Index: snapshot, Materializer: materializer}, nil
					}
					build := &application.Build{}
					for _, entry := range snapshot.Entries() {
						loaded, err := materializer.Materialize(ctx, entry, nil)
						if err != nil {
							return nil, err
						}
						build.Components = append(build.Components, loaded.Registration)
					}
					return build, nil
				}}))
				if indexed {
					require.Zero(t, loads, "readers must remain lazy until invocation")
				}
				for attempt := 0; attempt < 2; attempt++ {
					traceID := fmt.Sprintf("reader-summary-%d", attempt)
					before := len(logger.snapshot())
					res := httptest.NewRecorder()
					req := httptest.NewRequest("GET", "/parent", nil)
					req.Header.Set("X-Trace-ID", traceID)
					manager.ServeHTTP(res, req)
					require.Equal(t, 200, res.Code, res.Body.String())
					require.JSONEq(t, `{"rows":[{"id":11}]}`, res.Body.String())
					require.Equal(t, 2, loads)
					logs := logger.snapshot()[before:]
					if !summaries {
						require.Empty(t, logs)
						continue
					}
					views, hits := map[string]bool{}, map[string]bool{}
					for _, log := range logs {
						if log.message != "datly view read" && log.message != "datly cache read" {
							continue
						}
						require.Equal(t, traceID, log.fields["reqTraceId"])
						view := log.fields["view"].(string)
						if log.message == "datly view read" {
							views[view] = true
							require.EqualValues(t, 1, log.fields["rows"])
							require.NotEmpty(t, log.fields["elapsed"])
							require.Equal(t, "ok", log.fields["status"])
						} else if log.fields["found_lazy"] == true {
							hits[view] = true
						}
					}
					require.Equal(t, map[string]bool{"parent": true, "child": true}, views)
					if attempt == 1 {
						require.Equal(t, views, hits, "repeat reads must report lazy cache hits")
					}
				}
			})
		}
	}
}
