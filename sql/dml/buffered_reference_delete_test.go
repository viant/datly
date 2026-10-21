package dml

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
	xhandler "github.com/viant/xdatly/handler"
)

func TestBufferedReferenceDistinctDeleteTargets(t *testing.T) {
	mysql := &info.Dialect{Product: database.Product{Name: "MySQL"}}
	sqliteDialect := &info.Dialect{Product: database.Product{Name: "SQLite"}}
	for _, tc := range []struct {
		name, parent, deleted string
		dialect               *info.Dialect
		want                  bool
	}{
		{"actual daypart", "CI_AD_ORDER", "CI_DAYPART", mysql, true},
		{"actual range", "CI_AD_ORDER", "CI_DAYPART_UTC_RANGE", mysql, true},
		{"quoted native range", "`CI_AD_ORDER`", "`CI_DAYPART_UTC_RANGE`", mysql, true},
		{"same schema", "`ci_ads`.`CI_AD_ORDER`", "ci_ads.CI_DAYPART", mysql, true},
		{"sqlite", "parents", "ranges", sqliteDialect, true},
		{"same", "CI_AD_ORDER", "CI_AD_ORDER", mysql, false},
		{"quoted equivalent", "CI_AD_ORDER", "`CI_AD_ORDER`", mysql, false},
		{"qualified equivalent", "ci_ads.CI_AD_ORDER", "`ci_ads`.`CI_AD_ORDER`", mysql, false},
		{"qualification ambiguity", "CI_AD_ORDER", "ci_ads.CI_DAYPART", mysql, false},
		{"different namespace", "one.CI_AD_ORDER", "two.CI_DAYPART", mysql, false},
		{"namespace case ambiguity", "CI_ADS.CI_AD_ORDER", "ci_ads.CI_DAYPART", mysql, false},
		{"case alias", "CI_AD_ORDER", "ci_ad_order", mysql, false},
		{"quoted dotted same", `"parent.with.dot"`, "`parent.with.dot`", sqliteDialect, false},
		{"quoted dotted qualification ambiguity", `"parent.with.dot"`, "parent.with.dot", sqliteDialect, false},
		{"invalid target", "parents", "ranges; DELETE FROM parents", sqliteDialect, false},
		{"unclosed target", "parents", "`ranges", mysql, false},
		{"unresolved", "parents", "$Data.Ranges", mysql, false},
		{"nil dialect", "parents", "ranges", nil, false},
		{"unsupported dialect", "parents", "ranges", &info.Dialect{Product: database.Product{Name: "ANSI"}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := distinctBufferedDeleteTarget(tc.parent, tc.deleted, tc.dialect); got != tc.want {
				t.Fatalf("distinct=%v want %v", got, tc.want)
			}
		})
	}
}

func TestBufferedReferenceDistinctDeleteNestedConsumer(t *testing.T) {
	for _, placement := range []string{"prefix", "late consumer"} {
		for _, mode := range []string{"commit", "tail failure", "external"} {
			t.Run(placement+"/"+mode, func(t *testing.T) {
				ctx := context.Background()
				h := sqlite.New(t)
				h.DB.SetMaxOpenConns(1)
				if err := h.ExecStatements(ctx, "PRAGMA foreign_keys=ON", "CREATE TABLE parents(id INTEGER PRIMARY KEY)", "CREATE TABLE children(id INTEGER PRIMARY KEY,parent_id INTEGER NOT NULL REFERENCES parents(id))", "CREATE TABLE ranges(id INTEGER PRIMARY KEY)", "INSERT INTO ranges VALUES(999)", "CREATE TABLE audit(id INTEGER PRIMARY KEY,kind TEXT)", "CREATE TRIGGER parent_log AFTER INSERT ON parents BEGIN INSERT INTO audit(kind) VALUES('parent'); END", "CREATE TRIGGER range_log AFTER DELETE ON ranges BEGIN INSERT INTO audit(kind) VALUES('delete'); END", "CREATE TRIGGER child_log AFTER INSERT ON children BEGIN INSERT INTO audit(kind) VALUES('child'); END"); err != nil {
					t.Fatal(err)
				}
				var tx *sql.Tx
				var options []Option
				if mode == "external" {
					var err error
					tx, err = h.DB.BeginTx(ctx, nil)
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback()
					options = append(options, WithTx(tx))
				}
				owner := NewData(h.DB, options...)
				if err := owner.BeginInvocation(); err != nil {
					t.Fatal(err)
				}
				defer owner.Complete(ctx, errors.New("cleanup"))
				if err := owner.Start(ctx); err != nil {
					t.Fatal(err)
				}
				row := &bufferedParentRow{}
				if err := owner.Allocate(ctx, "parents", []*bufferedParentRow{row}, "ID"); err != nil {
					t.Fatal(err)
				}
				if row.ID <= 0 {
					t.Fatal("parent identity not allocated")
				}
				producer := owner.ComponentData(ComponentImperative, "").(*Data)
				if err := producer.InsertWithQueueContract("parents", row, rhandler.SourceRow); err != nil {
					t.Fatal(err)
				}
				producer.SealComponent()
				remove := func(d *Data) {
					t.Helper()
					if err := d.Delete("ranges", &bufferedParentRow{999}); err != nil {
						t.Fatal(err)
					}
				}
				if placement == "prefix" {
					middle := owner.ComponentData(ComponentImperative, "").(*Data)
					remove(middle)
					middle.SealComponent()
				}
				consumer := owner.ComponentData(ComponentImperative, "").(*Data)
				child := consumer.ComponentData(ComponentImperative, "").(*Data)
				marker := &bufferedChildRow{ID: 11, ParentID: row.ID}
				result, err := child.FrameworkValidator().Validate(ctx, marker, xhandler.ValidationOptions{Action: xhandler.WriteInsert, Shallow: true})
				if err != nil || result.Failed {
					t.Fatalf("postallocation ID=%d reference=%d validation=%+v err=%v", row.ID, marker.ParentID, result, err)
				}
				var count int
				if err = owner.tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM parents").Scan(&count); err != nil || count != 0 {
					t.Fatalf("validation flushed parent: %d %v", count, err)
				}
				if placement == "late consumer" {
					remove(child)
				}
				if err = child.Insert("children", marker); err != nil {
					t.Fatal(err)
				}
				child.SealComponent()
				consumer.SealComponent()
				if err = owner.ValidateExecutionGuards(ctx); err != nil {
					t.Fatal(err)
				}
				if mode == "tail failure" {
					if err = owner.Execute("INSERT INTO missing_tail VALUES(1)"); err != nil {
						t.Fatal(err)
					}
				}
				err = owner.Complete(ctx, nil)
				if mode == "tail failure" {
					if err == nil {
						t.Fatal("tail failure succeeded")
					}
					for _, table := range []string{"parents", "children", "audit"} {
						if err = h.DB.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil || count != 0 {
							t.Fatalf("rollback %s=%d err=%v", table, count, err)
						}
					}
					if err = h.DB.QueryRow("SELECT COUNT(*) FROM ranges").Scan(&count); err != nil || count != 1 {
						t.Fatal("delete escaped rollback")
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				var rows *sql.Rows
				if tx != nil {
					rows, err = tx.QueryContext(ctx, "SELECT kind FROM audit ORDER BY id")
				} else {
					rows, err = h.DB.QueryContext(ctx, "SELECT kind FROM audit ORDER BY id")
				}
				if err != nil {
					t.Fatal(err)
				}
				var order []string
				for rows.Next() {
					var kind string
					if err = rows.Scan(&kind); err != nil {
						t.Fatal(err)
					}
					order = append(order, kind)
				}
				rows.Close()
				if !reflect.DeepEqual(order, []string{"parent", "delete", "child"}) {
					t.Fatalf("original operation order=%v", order)
				}
				if tx != nil {
					if err = tx.Rollback(); err != nil {
						t.Fatal("framework completed caller transaction", err)
					}
					if err = h.DB.QueryRow("SELECT COUNT(*) FROM parents").Scan(&count); err != nil || count != 0 {
						t.Fatal("caller rollback did not own parent")
					}
				}
			})
		}
	}
}

func TestBufferedReferenceDeleteBarrierAdmission(t *testing.T) {
	for _, tc := range []struct {
		name, table string
		opaque      bool
	}{
		{"same table", "parents", false},
		{"quoted equivalent", `"parents"`, false},
		{"case alias", "PARENTS", false},
		{"default schema ambiguity", "main.parents", false},
		{"malformed target", "ranges; DELETE FROM parents", false},
		{"opaque unrelated SQL", "ranges", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			h := sqlite.New(t)
			h.DB.SetMaxOpenConns(1)
			if err := h.ExecStatements(ctx, "CREATE TABLE parents(id INTEGER PRIMARY KEY)", "CREATE TABLE children(id INTEGER PRIMARY KEY,parent_id INTEGER NOT NULL REFERENCES parents(id))", "CREATE TABLE ranges(id INTEGER PRIMARY KEY)"); err != nil {
				t.Fatal(err)
			}
			owner := NewData(h.DB)
			if err := owner.BeginInvocation(); err != nil {
				t.Fatal(err)
			}
			defer owner.Complete(ctx, errors.New("cleanup"))
			if err := owner.Start(ctx); err != nil {
				t.Fatal(err)
			}
			parent := owner.ComponentData(ComponentImperative, "").(*Data)
			if err := parent.Insert("parents", &bufferedParentRow{7}); err != nil {
				t.Fatal(err)
			}
			parent.SealComponent()
			middle := owner.ComponentData(ComponentImperative, "").(*Data)
			var err error
			if tc.opaque {
				err = middle.Execute("DELETE FROM ranges WHERE id=?", 999)
			} else {
				err = middle.Delete(tc.table, &bufferedParentRow{999})
			}
			if err != nil {
				t.Fatal(err)
			}
			middle.SealComponent()
			consumer := owner.ComponentData(ComponentImperative, "").(*Data)
			child := consumer.ComponentData(ComponentImperative, "").(*Data)
			result, err := child.FrameworkValidator().Validate(ctx, &bufferedChildRow{11, 7}, xhandler.ValidationOptions{Action: xhandler.WriteInsert, Shallow: true})
			if err != nil || !result.Failed {
				t.Fatalf("barrier bypassed: result=%+v err=%v", result, err)
			}
			if len(child.bufferedParents()) != 0 {
				t.Fatal("ambiguous/mutating prefix retained parent evidence")
			}
		})
	}
}
