package writer

import (
	"context"
	"github.com/viant/datly/internal/dialectcontext"
	"github.com/viant/datly/spec"
	"github.com/viant/sqlx/metadata/info"
	h "github.com/viant/xdatly/handler"
	"reflect"
	"strings"
	"testing"
)

type reconciliationBatchDML struct {
	h.DML
	statements []string
	arguments  [][]any
}

func (d *reconciliationBatchDML) Execute(sql string, args ...any) error {
	d.statements = append(d.statements, sql)
	d.arguments = append(d.arguments, args)
	return nil
}

type reconciliationBatchBinder struct{ d *reconciliationBatchDML }

func (*reconciliationBatchBinder) Bind(context.Context, any) error { return nil }
func (b *reconciliationBatchBinder) Lookup(context.Context, h.ValueKey) (any, bool, error) {
	return b.d, true, nil
}

func TestReconciliationDeleteBatchBoundaries(t *testing.T) {
	type useCase struct {
		description           string
		count, size, expected int
		foreign               bool
	}
	for _, tc := range []useCase{{"1001 rows produce two chunks", 1001, 1000, 2, false}, {"one row", 1, 1000, 1, false}, {"default scalar path", 2, 0, 0, false}, {"foreign parent rejected", 2, 1000, 0, true}} {
		t.Run(tc.description, func(t *testing.T) {
			type row struct{ ID, ParentID int }
			key := Field{Name: "ID", Column: "id", Index: []int{0}}
			parentKey := Field{Name: "ParentID", Column: "parent_id", Index: []int{1}}
			root := &Record{}
			child := &Record{Table: "child", Keys: []Field{key}}
			relation := &Relation{Child: child, Links: []Link{{Parent: key, Child: parentKey}}}
			role := reconciliationRoleMetadata{declaration: spec.ReconciliationRole{Holder: "Children", DeleteBatchSize: tc.size}, relation: relation}
			root.reconciliation = &reconciliationMetadata{roles: []reconciliationRoleMetadata{role}}
			parent := &Frame{Record: root, Entity: reflect.ValueOf(&row{ID: 7})}
			p := &Program{metadata: &Metadata{Root: root}, frames: &MutationFrames{}, actions: &MutationActions{}, previousFields: map[*Record]fieldSet{child: {"ID": true, "ParentID": true}}}
			for i := 0; i < tc.count; i++ {
				value := &row{ID: i + 1, ParentID: 7}
				if tc.foreign && i == tc.count-1 {
					value.ParentID = 8
				}
				frame := &Frame{Record: child, Parent: parent, Entity: reflect.ValueOf(value), Previous: reflect.ValueOf(value)}
				p.frames.Rows = append(p.frames.Rows, frame)
				p.actions.Rows = append(p.actions.Rows, &Action{Kind: h.WriteDelete, Entity: frame.Entity, frame: frame})
			}
			d := &reconciliationBatchDML{}
			dialect := &info.Dialect{QuoteCharacter: '"', Placeholder: "?"}
			dialect.Name = "SQLite"
			ctx := dialectcontext.WithDialect(t.Context(), dialect)
			batches, err := p.reconciliationDeleteBatches(ctx, &reconciliationBatchBinder{d})
			if tc.foreign {
				if err == nil {
					t.Fatal("foreign parent accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, action := range p.actions.Rows {
				if fn := batches[action]; fn != nil {
					if err = fn(); err != nil {
						t.Fatal(err)
					}
				}
			}
			if len(d.statements) != tc.expected {
				t.Fatalf("statements=%d expected=%d", len(d.statements), tc.expected)
			}
			total := 0
			for i, sql := range d.statements {
				if !strings.Contains(sql, `"parent_id" = ? AND "id" IN (`) {
					t.Fatal(sql)
				}
				args := d.arguments[i]
				if args[0] != 7 || len(args)-1 > tc.size {
					t.Fatalf("invalid scoped chunk %v", args)
				}
				for _, arg := range args[1:] {
					total++
					if arg != total {
						t.Fatalf("id order: got %v expected %d", arg, total)
					}
				}
			}
			if tc.size > 0 && total != tc.count {
				t.Fatalf("deleted=%d expected=%d", total, tc.count)
			}
		})
	}
}
