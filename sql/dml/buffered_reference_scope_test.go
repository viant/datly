package dml

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	xhandler "github.com/viant/xdatly/handler"
)

func TestBufferedReferenceCallerTail(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "ordered commit"
		if fail {
			name = "late database failure rolls back"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			h := sqlite.New(t)
			h.DB.SetMaxOpenConns(1)
			if err := h.ExecStatements(ctx,
				"PRAGMA foreign_keys=ON",
				"CREATE TABLE parents(id INTEGER PRIMARY KEY)",
				"CREATE TABLE children(id INTEGER PRIMARY KEY,parent_id INTEGER NOT NULL REFERENCES parents(id))",
				"CREATE TABLE audit(position INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT)",
				"CREATE TRIGGER parent_audit AFTER INSERT ON parents BEGIN INSERT INTO audit(name) VALUES('parent'); END",
				"CREATE TRIGGER child_audit AFTER INSERT ON children BEGIN INSERT INTO audit(name) VALUES('child'); END",
				"CREATE TABLE failures(id INTEGER PRIMARY KEY)",
				"CREATE TRIGGER late_failure BEFORE INSERT ON failures BEGIN SELECT RAISE(ABORT,'late caller failure'); END",
			); err != nil {
				t.Fatal(err)
			}
			owner := NewData(h.DB)
			if err := owner.BeginInvocation(); err != nil {
				t.Fatal(err)
			}
			defer owner.Complete(ctx, errors.New("test cleanup"))
			if err := owner.Start(ctx); err != nil {
				t.Fatal(err)
			}
			parent := owner.ComponentData(ComponentImperative, "").(*Data)
			if err := parent.Insert("parents", &bufferedParentRow{7}); err != nil {
				t.Fatal(err)
			}
			parent.SealComponent()
			child := owner.ComponentData(ComponentImperative, "").(*Data)
			row := &bufferedChildRow{11, 7}
			result, err := child.FrameworkValidator().Validate(ctx, row, xhandler.ValidationOptions{Action: xhandler.WriteInsert, Shallow: true})
			if err != nil || result.Failed {
				t.Fatalf("validation=%+v err=%v", result, err)
			}
			if err := child.Insert("children", row); err != nil {
				t.Fatal(err)
			}
			child.SealComponent()
			if err := owner.Execute("INSERT INTO audit(name) VALUES('caller')"); err != nil {
				t.Fatal(err)
			}
			if fail {
				if err := owner.Execute("INSERT INTO failures(id) VALUES(1)"); err != nil {
					t.Fatal(err)
				}
			}
			if err := owner.ValidateExecutionGuards(ctx); err != nil {
				t.Fatalf("caller tail invalidated child proof: %v", err)
			}
			var count int
			if err := owner.tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM audit").Scan(&count); err != nil || count != 0 {
				t.Fatalf("validation flushed writes: count=%d err=%v", count, err)
			}
			err = owner.Complete(ctx, nil)
			if fail {
				if err == nil || !strings.Contains(err.Error(), "late caller failure") {
					t.Fatalf("want actual late SQL failure, got %v", err)
				}
				for _, table := range []string{"parents", "children", "audit", "failures"} {
					if err := h.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil || count != 0 {
						t.Fatalf("%s rollback count=%d err=%v", table, count, err)
					}
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			rows, err := h.DB.QueryContext(ctx, "SELECT name FROM audit ORDER BY position")
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			var got []string
			for rows.Next() {
				var value string
				if err := rows.Scan(&value); err != nil {
					t.Fatal(err)
				}
				got = append(got, value)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, []string{"parent", "child", "caller"}) {
				t.Fatalf("execution order=%v", got)
			}
		})
	}
}

func TestBufferedReferenceConsumerScopeBarriers(t *testing.T) {
	type parentRow struct {
		ID   int    `sqlx:"id,primaryKey"`
		Code string `sqlx:"code"`
	}
	type childRow struct {
		ID         int    `sqlx:"id,primaryKey"`
		ParentCode string `sqlx:"parent_code,required,refTable=parents,refColumn=code"`
	}
	for _, placement := range []string{"inside before insert", "inside after insert", "nested consumer", "late caller prefix", "late binding prefix"} {
		for _, barrier := range []string{"execute", "delete", "reference update"} {
			t.Run(placement+"/"+barrier, func(t *testing.T) {
				ctx := context.Background()
				h := sqlite.New(t)
				h.DB.SetMaxOpenConns(1)
				if err := h.ExecStatements(ctx, "PRAGMA foreign_keys=ON", "CREATE TABLE parents(id INTEGER PRIMARY KEY,code TEXT UNIQUE)", "CREATE TABLE children(id INTEGER PRIMARY KEY,parent_code TEXT NOT NULL REFERENCES parents(code))"); err != nil {
					t.Fatal(err)
				}
				owner := NewData(h.DB)
				if err := owner.BeginInvocation(); err != nil {
					t.Fatal(err)
				}
				defer owner.Complete(ctx, errors.New("test cleanup"))
				if err := owner.Start(ctx); err != nil {
					t.Fatal(err)
				}
				parent := owner.ComponentData(ComponentImperative, "").(*Data)
				if err := parent.Insert("parents", &parentRow{7, "seed"}); err != nil {
					t.Fatal(err)
				}
				parent.SealComponent()
				relation := ComponentImperative
				if strings.HasPrefix(placement, "late ") {
					relation = ComponentBinding
				}
				child := owner.ComponentData(relation, "01").(*Data)
				row := &childRow{11, "seed"}
				result, err := child.FrameworkValidator().Validate(ctx, row, xhandler.ValidationOptions{Action: xhandler.WriteInsert, Shallow: true})
				if err != nil || result.Failed {
					t.Fatalf("initial validation=%+v err=%v", result, err)
				}
				if placement == "inside after insert" {
					if err := child.Insert("children", row); err != nil {
						t.Fatal(err)
					}
				}
				target := child
				switch placement {
				case "nested consumer":
					target = child.ComponentData(ComponentImperative, "").(*Data)
				case "late caller prefix":
					target = owner
				case "late binding prefix":
					// Appended after validation, yet sorts before the consumer.
					target = owner.ComponentData(ComponentBinding, "00").(*Data)
				}
				switch barrier {
				case "execute":
					err = target.Execute("DELETE FROM parents")
				case "delete":
					err = target.Delete("parents", &bufferedParentRow{7})
				case "reference update":
					err = target.Update("parents", &parentRow{7, "changed"})
				}
				if err != nil {
					t.Fatal(err)
				}
				if target != child && target != owner {
					target.SealComponent()
				}
				if placement != "inside after insert" {
					if err := child.Insert("children", row); err != nil {
						t.Fatal(err)
					}
				}
				child.SealComponent()
				if err := owner.ValidateExecutionGuards(ctx); err == nil || !strings.Contains(err.Error(), "buffered parent proof invalidated") {
					t.Fatalf("unsafe consumer scope passed: %v", err)
				}
				if err := owner.Complete(ctx, nil); err == nil {
					t.Fatal("caught invalidation allowed commit")
				}
				var count int
				if err := h.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM parents").Scan(&count); err != nil || count != 0 {
					t.Fatalf("invalidated parent persisted: count=%d err=%v", count, err)
				}
			})
		}
	}
}
