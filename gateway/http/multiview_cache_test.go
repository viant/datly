package http

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"github.com/xuri/excelize/v2"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/transcribe"
	"github.com/viant/sqlx/io/read/cache/aerospike"
)

type warmupMaskInput struct {
	Fields []string `parameter:"Fields,kind=query,in=_fields"`
}
type warmupMaskRow struct {
	Region string `sqlx:"region" json:"region"`
	Total  int    `sqlx:"total" json:"total"`
	Count  int    `sqlx:"count" json:"count"`
}
type warmupMaskOutput struct {
	Rows []warmupMaskRow `json:"rows"`
}

// Both providers execute the same authored reader and HTTP assertions after the
// source table is dropped. Aerospike is an explicit dedicated local fixture.
func TestHTTPWarmupProjectionMasks(t *testing.T) {
	for _, backend := range []string{"afs", "aerospike"} {
		for _, cube := range []bool{false, true} {
			for _, explicit := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/cube=%v/explicit=%v", backend, cube, explicit), func(t *testing.T) {
					if backend == "aerospike" && os.Getenv("DATLY_TEST_AEROSPIKE") == "" {
						t.Skip("set DATLY_TEST_AEROSPIKE=aerospike://127.0.0.1:3000/test")
					}
					ctx := context.Background()
					db := sqlite.New(t)
					require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE sales(region TEXT,amount INTEGER)", "INSERT INTO sales VALUES('west',0),('west',0),('east',7)"))
					sql := "SELECT region,amount AS total,1 AS count FROM sales WHERE region='east'"
					want := `{"rows":[{"region":"east","total":7}]}`
					if cube {
						sql = "SELECT region,SUM(amount) AS total,COUNT(*) AS count FROM sales GROUP BY region ORDER BY region"
						want = `{"rows":[{"region":"east","total":7},{"region":"west","total":0}]}`
					}
					source := `#setting($_ = $route('/warmup-mask','GET'))
#define($_ = $Fields<[]string>(query/_fields).Optional().QuerySelector('WarmupMask'))
#define($_ = $Rows<?>(output/view))
` + sql
					compiled, err := transcribe.NewCompiler().Compile(ctx, &transcribe.Source{Scope: "example.com/cache", Name: "WarmupMask", Text: source})
					require.NoError(t, err)
					compiled.Component.RootView.Selector = &spec.Selector{AllowFields: true}
					compiled.Component.RootView.Groupable = &cube
					location := t.TempDir()
					provider := "afs"
					if backend == "aerospike" {
						location = fmt.Sprintf("multiview_%d", time.Now().UnixNano())
						provider = os.Getenv("DATLY_TEST_AEROSPIKE")
						require.Equal(t, "aerospike://127.0.0.1:3000/test", provider, "live fixture is restricted to the dedicated local namespace")
					}
					if compiled.Component.Settings == nil {
						compiled.Component.Settings = &spec.Settings{}
					}
					var warmFields []string
					if explicit {
						warmFields = []string{"region", "total", "count"}
					}
					compiled.Component.Settings.Cache = &spec.CacheSettings{Enabled: true, Provider: provider, Location: location, TTL: "60s", TotalTimeoutInMs: 1000, SocketTimeoutInMs: 500, Warmup: &spec.CacheWarmupSettings{FieldNames: warmFields}}
					artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: compiled.Component, InputType: reflect.TypeFor[warmupMaskInput](), OutputType: reflect.TypeFor[warmupMaskOutput](), DirectViewField: "Rows"})
					require.NoError(t, err)
					var pool aerospike.Pool
					defer pool.Close()
					reader, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}, Aerospike: &pool})
					require.NoError(t, err)
					count, err := reader.(dexec.ReaderWarmer).Warmup(ctx, dexec.ReaderWarmupInvocation{Input: &warmupMaskInput{}})
					require.NoError(t, err)
					require.Greater(t, count, 0)
					rt, err := druntime.NewRuntime([]*druntime.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeFor[warmupMaskOutput](), Reader: reader}})
					require.NoError(t, err)
					defer rt.Shutdown(ctx)
					require.NoError(t, db.ExecStatements(ctx, "DROP TABLE sales"))
					h := NewHandler(rt, nil, "test")
					response := httptest.NewRecorder()
					h.ServeHTTP(response, httptest.NewRequest("GET", "/warmup-mask?_fields=region&_fields=total", nil))
					require.Equal(t, 200, response.Code, response.Body.String())
					require.JSONEq(t, want, response.Body.String())
					// Replay in a different projection order, then replay the full
					// entry. Native scan metadata must stay scoped to each read.
					for _, check := range []struct{ query, want string }{
						{"_fields=count&_fields=region", map[bool]string{false: `{"rows":[{"region":"east","count":1}]}`, true: `{"rows":[{"region":"east","count":1},{"region":"west","count":2}]}`}[cube]},
						{"", map[bool]string{false: `{"rows":[{"region":"east","total":7,"count":1}]}`, true: `{"rows":[{"region":"east","total":7,"count":1},{"region":"west","total":0,"count":2}]}`}[cube]},
					} {
						replay := httptest.NewRecorder()
						h.ServeHTTP(replay, httptest.NewRequest("GET", "/warmup-mask?"+check.query, nil))
						require.Equal(t, 200, replay.Code, replay.Body.String())
						require.JSONEq(t, check.want, replay.Body.String())
					}
					for _, format := range []string{"csv", "xml", "tabular", "xlsx"} {
						replay := httptest.NewRecorder()
						h.ServeHTTP(replay, httptest.NewRequest("GET", "/warmup-mask?_fields=region&_fields=total&_format="+format, nil))
						require.Equal(t, 200, replay.Code, replay.Body.String())
						expected := [][]string{{"Region", "Total"}, {"east", "7"}}
						if cube {
							expected = append(expected, []string{"west", "0"})
						}
						switch format {
						case "csv":
							rows, err := csv.NewReader(bytes.NewReader(replay.Body.Bytes())).ReadAll()
							require.NoError(t, err)
							require.Equal(t, expected, rows)
						case "xml":
							require.NotContains(t, replay.Body.String(), "<Count>")
							require.Contains(t, replay.Body.String(), "<Total>7</Total>")
							if cube {
								require.Contains(t, replay.Body.String(), "<Total>0</Total>")
							}
						case "tabular":
							expectedJSON := `{"rows":[["Region","Total"],["east",7]]}`
							if cube {
								expectedJSON = strings.Replace(expectedJSON, `["east",7]`, `["east",7],["west",0]`, 1)
							}
							require.JSONEq(t, expectedJSON, replay.Body.String())
						default:
							book, err := excelize.OpenReader(bytes.NewReader(replay.Body.Bytes()))
							require.NoError(t, err)
							rows, err := book.GetRows(book.GetSheetList()[0])
							require.NoError(t, err)
							require.Equal(t, expected, rows)
							require.NoError(t, book.Close())
						}
					}
					if cube {
						response = httptest.NewRecorder()
						h.ServeHTTP(response, httptest.NewRequest("GET", "/warmup-mask?_fields=total", nil))
						require.NotEqual(t, 200, response.Code, "a changed grouping must not use the warmed cube after database removal")
					}
				})
			}
		}
	}
}
