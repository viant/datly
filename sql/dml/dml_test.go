package dml

import "testing"

func TestBuildExecutionPlanGroupsCompatibleContiguousInserts(t *testing.T) {
	type userRecord struct {
		Name string
	}
	type auditRecord struct {
		Event string
	}
	plan := buildExecutionPlan([]*dataOperation{
		{kind: dataOpInsert, table: "users", data: &userRecord{Name: "john"}},
		{kind: dataOpInsert, table: "users", data: &userRecord{Name: "jane"}},
		{kind: dataOpUpdate, table: "users", data: &userRecord{Name: "jack"}},
		{kind: dataOpInsert, table: "users", data: &userRecord{Name: "sam"}},
		{kind: dataOpInsert, table: "audit", data: &auditRecord{Event: "created"}},
		{kind: dataOpInsert, table: "users", data: &auditRecord{Event: "wrong-type"}},
	})
	if len(plan) != 5 {
		t.Fatalf("expected 5 execution steps, got %d", len(plan))
	}
	if got := len(plan[0].operations); got != 2 {
		t.Fatalf("expected first step to batch 2 inserts, got %d", got)
	}
	if plan[0].kind != dataOpInsert || plan[0].table != "users" {
		t.Fatalf("unexpected first step %+v", plan[0])
	}
	if plan[1].kind != dataOpUpdate || len(plan[1].operations) != 1 {
		t.Fatalf("unexpected second step %+v", plan[1])
	}
	if plan[2].kind != dataOpInsert || plan[2].table != "users" || len(plan[2].operations) != 1 {
		t.Fatalf("unexpected third step %+v", plan[2])
	}
	if plan[3].kind != dataOpInsert || plan[3].table != "audit" || len(plan[3].operations) != 1 {
		t.Fatalf("unexpected fourth step %+v", plan[3])
	}
	if plan[4].kind != dataOpInsert || plan[4].table != "users" || len(plan[4].operations) != 1 {
		t.Fatalf("unexpected fifth step %+v", plan[4])
	}
}

func TestBatchInsertPayloadPreservesTypedOrder(t *testing.T) {
	type userRecord struct {
		Name string
	}
	operations := []*dataOperation{
		{kind: dataOpInsert, table: "users", data: &userRecord{Name: "john"}},
		{kind: dataOpInsert, table: "users", data: &userRecord{Name: "jane"}},
	}
	payload, ok := batchInsertPayload(operations).([]*userRecord)
	if !ok {
		t.Fatalf("expected typed batch payload, got %T", batchInsertPayload(operations))
	}
	if len(payload) != 2 {
		t.Fatalf("expected 2 batched records, got %d", len(payload))
	}
	if payload[0].Name != "john" || payload[1].Name != "jane" {
		t.Fatalf("unexpected batch payload order %+v", payload)
	}
}

func TestCanBatchInsertSkipsRecordsThatNeedGeneratedIDBackfill(t *testing.T) {
	type userRecord struct {
		ID   int    `sqlx:"id,primaryKey"`
		Name string `sqlx:"name"`
	}
	if canBatchInsert([]*dataOperation{
		{kind: dataOpInsert, table: "users", data: &userRecord{Name: "john"}},
		{kind: dataOpInsert, table: "users", data: &userRecord{Name: "jane"}},
	}) {
		t.Fatalf("expected zero-id records to stay on the per-row insert path")
	}
}
