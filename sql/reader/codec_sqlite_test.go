package reader_test

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/viant/datly/bootstrap/cacheconfig"
	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"github.com/viant/datly/sql/reader/compiler"
	"github.com/viant/sqlx/io/read/cache"
	xcodec "github.com/viant/xdatly/codec"
)

type codecTestRow struct {
	ID      int      `sqlx:"id"`
	Tags    []string `sqlx:"tags,type=string" codec:"split"`
	Fetched bool     `sqlx:"-"`
	Seen    string   `sqlx:"-"`
}

func (r *codecTestRow) OnFetch(context.Context) error {
	r.Fetched = true
	r.Seen = strings.Join(r.Tags, "|")
	return nil
}

type splitCodecFactory struct {
	calls  atomic.Int32
	builds atomic.Int32
}

func (f *splitCodecFactory) New(config *xcodec.Config, _ ...xcodec.Option) (xcodec.Instance, error) {
	f.builds.Add(1)
	if config.Body != "split" || config.SourceType != reflect.TypeOf("") || config.DestinationType != reflect.TypeOf([]string{}) {
		return nil, fmt.Errorf("unexpected codec contract: %+v", config)
	}
	return splitCodec{owner: f}, nil
}

type splitCodec struct{ owner *splitCodecFactory }

func (c splitCodec) Value(_ context.Context, raw any, options ...xcodec.Option) (any, error) {
	c.owner.calls.Add(1)
	if xcodec.NewOptions(options).Record == nil {
		return nil, fmt.Errorf("record option missing")
	}
	if raw == nil {
		return []string{"<null>"}, nil
	}
	value, ok := raw.(string)
	if !ok {
		return nil, fmt.Errorf("raw type %T", raw)
	}
	if value == "bad" {
		return nil, fmt.Errorf("invalid encoded value")
	}
	if value == "wrong" {
		return 7, nil
	}
	return strings.Split(value, ","), nil
}

func TestColumnCodecSQLite(t *testing.T) {
	type output struct{ Rows []codecTestRow }
	for _, tc := range []struct {
		name, SQL, insert string
		fail              bool
		want              []codecTestRow
		calls             int32
	}{
		{"decode_and_null", "SELECT id,tags FROM records ORDER BY id", "INSERT INTO records VALUES(1,'a,b'),(2,NULL),(3,'')", false, []codecTestRow{{ID: 1, Tags: []string{"a", "b"}, Fetched: true, Seen: "a|b"}, {ID: 2, Fetched: true}, {ID: 3, Tags: []string{""}, Fetched: true}}, 2},
		{"selected_away", "SELECT id FROM records ORDER BY id", "INSERT INTO records VALUES(1,'a,b')", false, []codecTestRow{{ID: 1, Fetched: true}}, 0},
		{"decode_error", "SELECT id,tags FROM records", "INSERT INTO records VALUES(1,'bad')", true, nil, 1},
		{"wrong_destination", "SELECT id,tags FROM records", "INSERT INTO records VALUES(1,'wrong')", true, nil, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER,tags TEXT)", tc.insert); err != nil {
				t.Fatal(err)
			}
			allowNulls := true
			component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Records"}, RootView: &spec.View{Name: "records", AllowNulls: &allowNulls, Source: &spec.ViewSource{SQL: tc.SQL}}}
			factory := &splitCodecFactory{}
			plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeOf(struct{}{}), OutputType: reflect.TypeOf(output{}), DirectViewField: "Rows", CodecFactory: factory})
			if err != nil {
				t.Fatal(err)
			}
			service, err := (cacheconfig.Config{Identity: "codec/raw", Settings: &spec.CacheSettings{Enabled: true, Location: t.TempDir(), TTL: "1m"}}).New()
			if err != nil {
				t.Fatal(err)
			}
			execution, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeOf(struct{}{}), OutputType: reflect.TypeOf(output{}), Plan: plan, SQL: &dsql.SQLComponent{DB: db.DB}, ReadCaches: map[*data.View]cache.Cache{plan.Root.View: service}})
			if err != nil {
				t.Fatal(err)
			}
			for pass := 1; pass <= 2; pass++ {
				actual, err := execution.Read(ctx, &struct{}{}, nil, nil)
				if tc.fail {
					if err == nil || !strings.Contains(err.Error(), "decode column Tags") {
						t.Fatalf("error=%v", err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(actual.(*output).Rows, tc.want) {
					t.Fatalf("pass %d rows=%#v,want=%#v", pass, actual, tc.want)
				}
				if factory.calls.Load() != tc.calls*int32(pass) {
					t.Fatalf("codec calls=%d", factory.calls.Load())
				}
				if factory.builds.Load() != 1 {
					t.Fatalf("codec was not compiled once: %d", factory.builds.Load())
				}
				if pass == 1 {
					if err := db.ExecStatements(ctx, "DROP TABLE records"); err != nil {
						t.Fatal(err)
					}
				}
			}
			var workers sync.WaitGroup
			for i := 0; i < 8; i++ {
				workers.Add(1)
				go func() {
					defer workers.Done()
					actual, err := execution.Read(ctx, &struct{}{}, nil, nil)
					if err != nil {
						t.Error(err)
						return
					}
					if !reflect.DeepEqual(actual.(*output).Rows, tc.want) {
						t.Errorf("concurrent rows=%#v", actual)
					}
				}()
			}
			workers.Wait()
			if factory.calls.Load() != tc.calls*10 || factory.builds.Load() != 1 {
				t.Fatalf("shared codec lifetime: builds=%d calls=%d", factory.builds.Load(), factory.calls.Load())
			}
		})
	}
}
