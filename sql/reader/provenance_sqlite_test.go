package reader_test

import (
	"context"
	"reflect"
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

type EvidenceEmbedded struct {
	ID int `sqlx:"id|identity"`
}
type evidenceNames struct {
	Name *string `sqlx:"name"`
}
type evidenceReadRow struct {
	EvidenceEmbedded
	Left   evidenceNames  `sqlx:"ns=left_"`
	Right  *evidenceNames `sqlx:"ns=right_"`
	Unused string         `sqlx:"-"`
}

func TestReadResultActualColumnEvidenceSQLite(t *testing.T) {
	type plainOutput struct{ Rows []evidenceReadRow }
	type codecOutput struct{ Rows []codecTestRow }
	for _, tc := range []struct {
		name, query    string
		output         reflect.Type
		codec          bool
		loaded, absent []string
	}{
		{"native namespaces and promoted aliases", "SELECT id AS identity,NULL AS left_name FROM records ORDER BY id", reflect.TypeOf(plainOutput{}), false, []string{"ID", "EvidenceEmbedded.ID", "Left.Name"}, []string{"Right.Name", "Unused", "missing"}},
		{"native pointer namespace", "SELECT id,NULL AS right_name FROM records ORDER BY id", reflect.TypeOf(plainOutput{}), false, []string{"ID", "Right.Name"}, []string{"Left.Name", "Unused"}},
		{"codec decoded and null", "SELECT id,tags FROM records ORDER BY id", reflect.TypeOf(codecOutput{}), true, []string{"ID", "Tags"}, []string{"Fetched", "Seen", "Raw.Codec0"}},
		{"codec selected away", "SELECT id FROM records ORDER BY id", reflect.TypeOf(codecOutput{}), true, []string{"ID"}, []string{"Tags", "Fetched", "Seen"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			h := sqlite.New(t)
			if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER,tags TEXT)", "INSERT INTO records VALUES(1,'a,b'),(2,NULL)"); err != nil {
				t.Fatal(err)
			}
			allowNulls := true
			component := &spec.Component{RootView: &spec.View{Name: "records", AllowNulls: &allowNulls, Source: &spec.ViewSource{SQL: tc.query}}}
			var factory xcodec.Factory
			if tc.codec {
				factory = &splitCodecFactory{}
			}
			plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeOf(struct{}{}), OutputType: tc.output, DirectViewField: "Rows", CodecFactory: factory})
			if err != nil {
				t.Fatal(err)
			}
			nativeCache, err := (cacheconfig.Config{Identity: "read-evidence", Settings: &spec.CacheSettings{Enabled: true, Location: t.TempDir(), TTL: "1m"}}).New()
			if err != nil {
				t.Fatal(err)
			}
			execution, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeOf(struct{}{}), OutputType: tc.output, Plan: plan, SQL: &dsql.SQLComponent{DB: h.DB}, ReadCaches: map[*data.View]cache.Cache{plan.Root.View: nativeCache}})
			if err != nil {
				t.Fatal(err)
			}
			for pass := 0; pass < 2; pass++ {
				result, err := execution.ReadResult(ctx, &struct{}{}, nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				if result.Data == nil || result.Projection == nil || result.Projection.RootHolder() != "Rows" || result.Projection.DirectOutput() {
					t.Fatalf("result=%+v", result)
				}
				rows := result.Projection.Rows()
				if len(rows) != 2 {
					t.Fatalf("rows=%d", len(rows))
				}
				for _, row := range rows {
					if !row.Fields().Known() {
						t.Fatal("actual schema unknown")
					}
					for _, name := range tc.loaded {
						if !row.Fields().Has(name) {
							t.Fatalf("pass%d missing loaded %s", pass, name)
						}
					}
					for _, name := range tc.absent {
						if row.Fields().Has(name) {
							t.Fatalf("pass%d falsely loaded %s", pass, name)
						}
					}
				}
				if pass == 0 {
					if err = h.ExecStatements(ctx, "DROP TABLE records"); err != nil {
						t.Fatal(err)
					}
				}
			}
		})
	}
}

func TestReadResultEmptyInvocationDoesNotReusePriorEvidenceSQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER)", "INSERT INTO records VALUES(1)"); err != nil {
		t.Fatal(err)
	}
	component := &spec.Component{RootView: &spec.View{Name: "records", Source: &spec.ViewSource{SQL: "SELECT id FROM records"}}}
	outputType := reflect.TypeOf([]EvidenceEmbedded{})
	plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeOf(struct{}{}), OutputType: outputType, DirectViewType: outputType})
	if err != nil {
		t.Fatal(err)
	}
	execution, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeOf(struct{}{}), OutputType: outputType, Plan: plan, SQL: &dsql.SQLComponent{DB: h.DB}})
	if err != nil {
		t.Fatal(err)
	}
	for _, count := range []int{1, 0} {
		result, err := execution.ReadResult(ctx, &struct{}{}, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !result.Projection.DirectOutput() || result.Projection.RootHolder() != "" || len(result.Projection.Rows()) != count || len(result.Data.([]EvidenceEmbedded)) != count {
			t.Fatalf("count%d result=%+v", count, result)
		}
		if err = h.ExecStatements(ctx, "DELETE FROM records"); err != nil {
			t.Fatal(err)
		}
	}
}
