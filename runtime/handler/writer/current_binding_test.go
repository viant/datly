package writer

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
)

type currentEvidence struct {
	ID *int `sqlx:"id,primaryKey=true"`
}
type currentEvidenceInput struct {
	Evidence        *currentEvidence `parameter:"Evidence,kind=view" view:"Evidence,table=parents,auxiliary=true"`
	ChildEvidence   *currentEvidence `parameter:"ChildEvidence,kind=view" view:"ChildEvidence,table=children,auxiliary=true"`
	Rows            []*sqPlainParent `parameter:"Rows,kind=body,in=data" view:"Rows,table=parents"`
	CurrentRows     []*sqPlainParent `parameter:"CurrentRows,kind=view" view:"CurrentRows,table=parents"`
	CurrentChildren []*sqPlainChild  `parameter:"CurrentChildren,kind=view" view:"CurrentChildren,table=children"`
}

func TestSQLiteCurrentDoesNotAdoptAuxiliaryTableEvidence(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx, append(strings.Split(sqSchema, "\n"), "INSERT INTO parents(id,name) VALUES(1,'parent'); INSERT INTO children(id,parent_id,label) VALUES(10,1,'original')")...); err != nil {
		t.Fatal(err)
	}
	in := &currentEvidenceInput{
		Evidence: &currentEvidence{ID: ptr(1)}, ChildEvidence: &currentEvidence{ID: ptr(10)},
		Rows:            []*sqPlainParent{{ID: ptr(1), Children: []*sqPlainChild{{ID: ptr(10), Label: ptr("changed"), Has: &sqPlainChildHas{ID: true, Label: true}}}, Has: &sqPlainParentHas{ID: true, Children: true}}},
		CurrentRows:     []*sqPlainParent{{ID: ptr(1), Name: ptr("parent")}},
		CurrentChildren: []*sqPlainChild{{ID: ptr(10), ParentID: ptr(1), Label: ptr("original")}},
	}
	out := &struct {
		Data []*sqPlainParent `parameter:"Data,kind=output,in=body"`
	}{}
	if _, err := runSQLiteWriter(t, ctx, db, sqComponent("current-evidence", "patch", ""), in, out, "patch"); err != nil {
		t.Fatal(err)
	}
	var parent int
	var label string
	if err := db.DB.QueryRowContext(ctx, "SELECT parent_id,label FROM children WHERE id=10").Scan(&parent, &label); err != nil {
		t.Fatal(err)
	}
	if parent != 1 || label != "changed" {
		t.Fatalf("child=%d/%s", parent, label)
	}
}

func TestCurrentRoleBeatsEarlierAmbiguousTableFallback(t *testing.T) {
	typ := reflect.TypeOf(struct {
		First          []*currentEvidence `parameter:"First,kind=view" view:"First,table=records"`
		Second         []*currentEvidence `parameter:"Second,kind=view" view:"Second,table=records"`
		CurrentRecords []*currentEvidence `parameter:"CurrentRecords,kind=view" view:"CurrentRecords,table=records"`
	}{})
	index, _, err := currentInputField(typ, "records", false, "Records")
	if err != nil || index != 2 {
		t.Fatalf("index=%d err=%v", index, err)
	}
	if _, _, err = currentInputField(typ, "records", false, "Another"); err == nil {
		t.Fatal("ambiguous table fallback accepted")
	}
}

func TestAuxiliaryCurrentUsesCanonicalGeneratedRoleName(t *testing.T) {
	typ := reflect.TypeOf(struct {
		Evidence             []*currentEvidence `parameter:"Evidence,kind=view" view:"Evidence,table=progress,auxiliary=true"`
		CurrentStageProgress []*currentEvidence `parameter:"CurrentStageProgress,kind=view" view:"CurrentStageProgress,table=progress,auxiliary=true"`
	}{})
	index, _, err := currentInputField(typ, "progress", true, "stage_progress")
	if err != nil || index != 1 {
		t.Fatalf("index=%d err=%v", index, err)
	}
	index, _, err = currentInputField(typ, "progress", false, "stage_progress")
	if err != nil || index != -1 {
		t.Fatalf("writable role adopted auxiliary Current: index=%d err=%v", index, err)
	}
}
