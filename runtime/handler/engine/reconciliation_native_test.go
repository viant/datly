package engine_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly/locator"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/runtime/handler/compiler"
	"github.com/viant/datly/runtime/handler/engine"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/runtime/handler/writer"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/dml"
	h "github.com/viant/xdatly/handler"
)

type reconciliationRootHas struct{ ID, Name, Count, Labels, Grants, Links, Tokens, Blocks bool }
type reconciliationRoot struct {
	ID     *int                   `sqlx:"id,primaryKey,autoincrement"`
	Name   *string                `sqlx:"name" validate:"required"`
	Count  int                    `sqlx:"count"`
	Labels []*reconciliationLeaf  `view:"Labels,table=reconcile_labels" on:"ID=ParentID"`
	Grants []*reconciliationLeaf  `view:"Grants,table=reconcile_grants" on:"ID=ParentID"`
	Links  []*reconciliationLeaf  `view:"Links,table=reconcile_links" on:"ID=ParentID"`
	Tokens []*reconciliationLeaf  `view:"Tokens,table=reconcile_tokens" on:"ID=ParentID"`
	Blocks []*reconciliationLeaf  `view:"Blocks,table=reconcile_blocks" on:"ID=ParentID"`
	Has    *reconciliationRootHas `setMarker:"true" sqlx:"-" json:"-"`
}
type reconciliationLeafHas struct{ ID, ParentID, Name bool }
type reconciliationLeaf struct {
	ID       *int                   `sqlx:"id,primaryKey,autoincrement"`
	ParentID *int                   `sqlx:"parent_id,refTable=reconcile_root,refColumn=id"`
	Name     *string                `sqlx:"name" validate:"required"`
	Has      *reconciliationLeafHas `setMarker:"true" sqlx:"-" json:"-"`
}
type reconciliationInput struct {
	Rows          []*reconciliationRoot `parameter:"Rows,kind=body,in=data" view:"Rows,table=reconcile_root"`
	CurrentRows   []*reconciliationRoot `parameter:"CurrentRows,kind=view,in=CurrentRows" view:"CurrentRows,table=reconcile_root"`
	CurrentLabels []*reconciliationLeaf `parameter:"CurrentLabels,kind=view,in=CurrentLabels" view:"CurrentLabels,table=reconcile_labels"`
	CurrentGrants []*reconciliationLeaf `parameter:"CurrentGrants,kind=view,in=CurrentGrants" view:"CurrentGrants,table=reconcile_grants"`
	CurrentLinks  []*reconciliationLeaf `parameter:"CurrentLinks,kind=view,in=CurrentLinks" view:"CurrentLinks,table=reconcile_links"`
	CurrentTokens []*reconciliationLeaf `parameter:"CurrentTokens,kind=view,in=CurrentTokens" view:"CurrentTokens,table=reconcile_tokens"`
	CurrentBlocks []*reconciliationLeaf `parameter:"CurrentBlocks,kind=view,in=CurrentBlocks" view:"CurrentBlocks,table=reconcile_blocks"`
	Method        string                `parameter:"Method,kind=query,in=method"`
}
type reconciliationOutput struct {
	Data []*reconciliationRoot `parameter:"Data,kind=output,in=body"`
}
type reconciliationProbeKey struct{}
type reconciliationProbe struct {
	filtered    bool
	mode        string
	base        context.Context
	cancel      context.CancelFunc
	allocations map[string][]int
	originals   map[string][]bool
	trace       []string
	physical    []string
	refs        []writer.OccurrenceRef
	calls       int
	premerge    map[*reconciliationRoot]int
	outcomes    []h.Outcome
}
type reconciliationHooks struct{}

var reconciliationLinked = reflect.TypeFor[reconciliationHooks]()

func reconciliationProbeFrom(ctx context.Context) *reconciliationProbe {
	return ctx.Value(reconciliationProbeKey{}).(*reconciliationProbe)
}

type reconciliationFilterHooks struct{ reconciliationHooks }

var reconciliationFilterLinked = reflect.TypeFor[reconciliationFilterHooks]()

func (*reconciliationFilterHooks) AfterValidateInput(ctx context.Context, input *reconciliationInput, _ *reconciliationOutput) error {
	p := reconciliationProbeFrom(ctx)
	if p.mode != "filter before reconcile" {
		return nil
	}
	for _, row := range input.Rows {
		var kept []*reconciliationLeaf
		for _, leaf := range row.Grants {
			if *leaf.ID != 0 {
				return fmt.Errorf("filter ran after allocation: %d", *leaf.ID)
			}
			if *leaf.Name != "filtered" {
				kept = append(kept, leaf)
			}
		}
		row.Grants = kept
	}
	p.filtered = true
	return nil
}

func (*reconciliationHooks) ReconcileInput(ctx context.Context, input *reconciliationInput, _ *reconciliationOutput, native writer.ReconciliationContext) (writer.ReconciliationPlan, error) {
	p := reconciliationProbeFrom(ctx)
	p.calls++
	roots, e := native.Roots()
	if e != nil {
		return writer.ReconciliationPlan{}, e
	}
	plan := writer.ReconciliationPlan{}
	for i, root := range roots {
		entry := writer.ReconciliationRootPlan{Root: root.Ref}
		count := 0
		for _, role := range root.Roles {
			count += len(role.Working)
		}
		entry.Assignments = []writer.ReconciliationAssignment{{Field: "Count", Value: count, MarkPresent: true}}
		p.premerge[input.Rows[i]] = count
		for _, role := range root.Roles {
			rp := writer.ReconciliationRolePlan{Holder: role.Holder}
			seen := map[string]bool{}
			current := map[string]writer.ReconciliationObservation{}
			for _, row := range role.Current {
				current[*row.Row.(*reconciliationLeaf).Name] = row
			}
			if role.Holder == "Labels" && input.Method != "PUT" {
				for _, row := range role.Current {
					rp.Selected = append(rp.Selected, writer.ReconciliationSelection{Occurrence: row.Ref})
					seen[*row.Row.(*reconciliationLeaf).Name] = true
				}
			}
			for _, row := range role.Working {
				leaf := row.Row.(*reconciliationLeaf)
				p.allocations[role.Holder] = append(p.allocations[role.Holder], *leaf.ID)
				p.originals[role.Holder] = append(p.originals[role.Holder], row.Original.Has("ID"))
				selection := writer.ReconciliationSelection{Occurrence: row.Ref}
				if p.mode == "stale" && len(p.refs) > 0 {
					selection.Occurrence = p.refs[0]
				}
				if p.mode == "forge" {
					selection.Occurrence = writer.OccurrenceRef{}
				}
				p.refs = append(p.refs, row.Ref)
				if (role.Holder == "Labels" || role.Holder == "Grants") && seen[*leaf.Name] {
					continue
				}
				seen[*leaf.Name] = true
				if role.Holder == "Grants" {
					if prior, ok := current[*leaf.Name]; ok {
						selection.AdoptCurrent = prior.Ref
					}
				}
				if p.mode == "bad scalar" {
					selection.Assignments = []writer.ReconciliationAssignment{{Field: "Name", Value: (*string)(nil), MarkPresent: true}}
				}
				if p.mode == "wrong role" && len(root.Roles[0].Working) > 0 {
					selection.Occurrence = root.Roles[0].Working[0].Ref
				}
				rp.Selected = append(rp.Selected, selection)
			}
			if role.Holder == "Grants" && len(role.Working) == 0 && len(role.Current) > 0 {
				rp.Selected = append(rp.Selected, writer.ReconciliationSelection{Occurrence: role.Current[0].Ref})
			}
			if input.Method == "PUT" {
				keep := map[int]bool{}
				for _, selected := range rp.Selected {
					for _, row := range append(append([]writer.ReconciliationObservation(nil), role.Working...), role.Current...) {
						if row.Ref == selected.Occurrence {
							id := *row.Row.(*reconciliationLeaf).ID
							if selected.AdoptCurrent != (writer.OccurrenceRef{}) {
								for _, prior := range role.Current {
									if prior.Ref == selected.AdoptCurrent {
										id = *prior.Row.(*reconciliationLeaf).ID
									}
								}
							}
							keep[id] = true
						}
					}
				}
				for _, row := range role.Current {
					if !keep[*row.Row.(*reconciliationLeaf).ID] {
						rp.Deletes = append(rp.Deletes, row.Ref)
					}
				}
			}
			if p.mode == "duplicate" && len(rp.Selected) > 0 {
				rp.Selected = append(rp.Selected, rp.Selected[0])
			}
			entry.Roles = append(entry.Roles, rp)
		}
		plan.Roots = append(plan.Roots, entry)
	}
	if p.mode == "cancel" {
		p.cancel()
	}
	if p.mode == "Current mutation" {
		*input.CurrentGrants[0].Name = "tampered"
	}
	if p.mode == "request mutation" {
		input.Method = "tampered"
	}
	if p.mode == "mutation" {
		input.Rows[0].Name = new(string)
	}
	if p.mode == "panic" {
		panic("reconcile test panic")
	}
	if p.mode == "error" {
		return plan, errors.New("reconcile callback failure")
	}
	if p.mode == "bad root field" {
		plan.Roots[0].Assignments = []writer.ReconciliationAssignment{{Field: "ID", Value: new(int)}}
	}
	if p.mode == "bad value type" {
		plan.Roots[0].Assignments = []writer.ReconciliationAssignment{{Field: "Count", Value: int64(3)}}
	}
	return plan, nil
}
func (*reconciliationHooks) WriteEligible(ctx context.Context, row *reconciliationRoot, _ h.LifecycleContext[reconciliationRoot, h.NoParent, reconciliationOutput], _ h.WriteAction) (bool, error) {
	p := reconciliationProbeFrom(ctx)
	if p.premerge[row] != row.Count {
		return false, fmt.Errorf("premerge association or count lost")
	}
	return p.mode != "suppressed", nil
}
func (*reconciliationHooks) AfterQueue(ctx context.Context, row *reconciliationRoot, state h.LifecycleContext[reconciliationRoot, h.NoParent, reconciliationOutput]) error {
	p := reconciliationProbeFrom(ctx)
	p.trace = append(p.trace, fmt.Sprintf("root:%d", *row.ID))
	if p.mode == "root failure" {
		return errors.New("root observation failure")
	}
	if p.mode == "Previous mutation" {
		*state.Previous.Grants[0].Name = "tampered"
	}
	if p.mode == "queued mutation" {
		row.Labels[0].Name = new(string)
	}
	return nil
}
func (*reconciliationHooks) ObserveQueueAttempt(ctx context.Context, event h.QueueAttemptEvent) {
	if event.Boundary == h.PhaseBegin {
		p := reconciliationProbeFrom(ctx)
		p.trace = append(p.trace, event.Role+":"+string(event.Operation))
	}
}
func (*reconciliationHooks) AfterQueueInput(ctx context.Context, _ *reconciliationInput, _ *reconciliationOutput) error {
	p := reconciliationProbeFrom(ctx)
	p.trace = append(p.trace, "batch")
	return nil
}
func (*reconciliationHooks) Finalize(ctx context.Context, _ *reconciliationInput, _ *reconciliationOutput, outcome h.Outcome) error {
	p := reconciliationProbeFrom(ctx)
	p.outcomes = append(p.outcomes, outcome)
	return nil
}
func reconcileLeaf(name string) *reconciliationLeaf {
	return &reconciliationLeaf{ID: new(int), Name: &name, Has: &reconciliationLeafHas{Name: true}}
}
func reconcileRow() *reconciliationRoot {
	id, name := 1, "changed"
	return &reconciliationRoot{ID: &id, Name: &name, Has: &reconciliationRootHas{ID: true, Name: true}}
}
func newReconciliationDB(t *testing.T) *sqlite.Harness {
	db := sqlite.New(t)
	statements := []string{`CREATE TABLE reconcile_root(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT NOT NULL,count INTEGER NOT NULL DEFAULT 0)`, `INSERT INTO reconcile_root(id,name) VALUES(1,'old')`, `CREATE TABLE reconcile_trace(seq INTEGER PRIMARY KEY AUTOINCREMENT,value TEXT NOT NULL)`}
	for _, role := range []string{"labels", "grants", "links", "tokens", "blocks"} {
		table := "reconcile_" + role
		statements = append(statements, fmt.Sprintf(`CREATE TABLE %s(id INTEGER PRIMARY KEY AUTOINCREMENT,parent_id INTEGER NOT NULL REFERENCES reconcile_root(id),name TEXT NOT NULL)`, table))
		for _, operation := range []string{"INSERT", "UPDATE", "DELETE"} {
			ref := "NEW"
			if operation == "DELETE" {
				ref = "OLD"
			}
			statements = append(statements, fmt.Sprintf(`CREATE TRIGGER trace_%s_%s AFTER %s ON %s BEGIN INSERT INTO reconcile_trace(value) VALUES('%s:%s:' || %s.id); END`, role, operation, operation, table, role, operation, ref))
		}
	}
	statements = append(statements, `INSERT INTO reconcile_labels(id,parent_id,name) VALUES(10,1,'a')`, `INSERT INTO reconcile_grants(id,parent_id,name) VALUES(40,1,'a')`, `DELETE FROM reconcile_trace`)
	if e := db.ExecStatements(t.Context(), statements...); e != nil {
		t.Fatal(e)
	}
	return db
}
func runReconciliation(t *testing.T, db *sqlite.Harness, input *reconciliationInput, p *reconciliationProbe) (any, error) {
	t.Helper()
	if p.allocations == nil {
		p.allocations = map[string][]int{}
		p.originals = map[string][]bool{}
		p.premerge = map[*reconciliationRoot]int{}
	}
	linked := reconciliationLinked
	if p.mode == "filter before reconcile" {
		linked = reconciliationFilterLinked
	}
	config := &spec.Reconciliation{Mode: "same-parent-root-first", RootFields: []string{"Count"}}
	root := &spec.View{Name: "Rows", EntityHooks: linked.Name(), Source: &spec.ViewSource{Table: "reconcile_root"}, Reconciliation: config}
	for _, role := range []string{"Labels", "Grants", "Links", "Tokens", "Blocks"} {
		config.Roles = append(config.Roles, spec.ReconciliationRole{Holder: role, Fields: []string{"Name", "ParentID"}, AdoptIdentity: role == "Grants"})
		root.Relations = append(root.Relations, &spec.Relation{Name: role, Holder: role, View: &spec.View{Name: role, Source: &spec.ViewSource{Table: "reconcile_" + strings.ToLower(role)}}})
	}
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: reconciliationLinked.PkgPath(), Name: "Reconcile"}, Settings: &spec.Settings{Mutation: "patch"}, Routes: []*spec.Route{{Method: "PATCH", Path: "/reconcile"}}, RootView: root}
	compiled, e := compiler.New(compiler.Input{Component: component, InputType: reflect.TypeFor[reconciliationInput]()}).Compile()
	if e != nil {
		t.Fatal(e)
	}
	route, _ := compiled.Input.ForRoute(spec.RouteRef{Method: "PATCH", Path: "/reconcile"})
	native, e := writer.New(component, reflect.TypeFor[reconciliationInput](), reflect.TypeFor[reconciliationOutput](), "patch")
	if e != nil {
		t.Fatal(e)
	}
	provider := handlerprovider.Named("view", func(ctx context.Context, _ reflect.Type, key string) (any, bool, error) {
		if key == "CurrentRows" {
			rows, e := db.DB.QueryContext(ctx, `SELECT id,name,count FROM reconcile_root ORDER BY id`)
			if e != nil {
				return nil, true, e
			}
			defer rows.Close()
			var result []*reconciliationRoot
			for rows.Next() {
				row := &reconciliationRoot{ID: new(int), Name: new(string)}
				if e = rows.Scan(row.ID, row.Name, &row.Count); e != nil {
					return nil, true, e
				}
				result = append(result, row)
			}
			return result, true, rows.Err()
		}
		table := "reconcile_" + strings.ToLower(strings.TrimPrefix(key, "Current"))
		rows, e := db.DB.QueryContext(ctx, "SELECT id,parent_id,name FROM "+table+" ORDER BY id")
		if e != nil {
			return nil, true, e
		}
		defer rows.Close()
		var result []*reconciliationLeaf
		for rows.Next() {
			row := &reconciliationLeaf{ID: new(int), ParentID: new(int), Name: new(string)}
			if e = rows.Scan(row.ID, row.ParentID, row.Name); e != nil {
				return nil, true, e
			}
			result = append(result, row)
		}
		return result, true, rows.Err()
	})
	base := p.base
	if base == nil {
		base = t.Context()
	}
	ctx := context.WithValue(base, reconciliationProbeKey{}, p)
	return engine.New().Execute(ctx, engine.Request{Input: route, BoundInput: input, Handler: native, Providers: []locator.Provider{provider}, DataSource: dml.Source{DB: db.DB}})
}
func reconcileIDs(rows []*reconciliationLeaf) []int {
	result := []int{}
	for _, row := range rows {
		result = append(result, *row.ID)
	}
	return result
}
func TestFiniteReconciliationNativeAllocationSQLite(t *testing.T) {
	db := newReconciliationDB(t)
	row := reconcileRow()
	row.Labels = []*reconciliationLeaf{reconcileLeaf("a"), reconcileLeaf("b")}
	row.Grants = []*reconciliationLeaf{reconcileLeaf("a"), reconcileLeaf("a"), reconcileLeaf("c")}
	p := &reconciliationProbe{}
	result, e := runReconciliation(t, db, &reconciliationInput{Rows: []*reconciliationRoot{row}}, p)
	if e != nil {
		t.Fatal(e)
	}
	effective := result.(*reconciliationOutput).Data[0]
	if !reflect.DeepEqual(p.allocations["Labels"], []int{11, 12}) || !reflect.DeepEqual(p.allocations["Grants"], []int{41, 42, 43}) || !reflect.DeepEqual(reconcileIDs(effective.Labels), []int{10, 12}) || !reflect.DeepEqual(reconcileIDs(effective.Grants), []int{40, 43}) {
		t.Fatalf("allocated=%v final labels=%v grants=%v", p.allocations, reconcileIDs(effective.Labels), reconcileIDs(effective.Grants))
	}
	if effective.Count != 5 || !reflect.DeepEqual(p.originals["Grants"], []bool{false, false, false}) {
		t.Fatalf("premerge count/originals=%d/%v", effective.Count, p.originals)
	}
	next := reconcileRow()
	next.Labels = []*reconciliationLeaf{reconcileLeaf("next")}
	next.Grants = []*reconciliationLeaf{reconcileLeaf("next")}
	q := &reconciliationProbe{}
	if _, e = runReconciliation(t, db, &reconciliationInput{Rows: []*reconciliationRoot{next}}, q); e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(q.allocations["Labels"], []int{13}) || !reflect.DeepEqual(q.allocations["Grants"], []int{44}) {
		t.Fatal("discarded allocations were reclaimed", q.allocations)
	}
}
func TestFiniteReconciliationReusedOnlySQLite(t *testing.T) {
	db := newReconciliationDB(t)
	row := reconcileRow()
	row.Grants = []*reconciliationLeaf{reconcileLeaf("a")}
	p := &reconciliationProbe{}
	result, e := runReconciliation(t, db, &reconciliationInput{Rows: []*reconciliationRoot{row}}, p)
	if e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(p.allocations["Grants"], []int{41}) || !reflect.DeepEqual(reconcileIDs(result.(*reconciliationOutput).Data[0].Grants), []int{40}) {
		t.Fatal(p.allocations)
	}
	next := reconcileRow()
	next.Grants = []*reconciliationLeaf{reconcileLeaf("next")}
	q := &reconciliationProbe{}
	if _, e = runReconciliation(t, db, &reconciliationInput{Rows: []*reconciliationRoot{next}}, q); e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(q.allocations["Grants"], []int{42}) {
		t.Fatal(q.allocations)
	}
}
func TestFiniteReconciliationFailureControlsSQLite(t *testing.T) {
	for _, mode := range []string{"forge", "duplicate", "wrong role", "bad root field", "bad value type", "bad scalar", "mutation", "panic", "error", "queued mutation", "root failure", "Current mutation", "Previous mutation", "request mutation"} {
		t.Run(mode, func(t *testing.T) {
			db := newReconciliationDB(t)
			row := reconcileRow()
			row.Labels = []*reconciliationLeaf{reconcileLeaf("b")}
			row.Grants = []*reconciliationLeaf{reconcileLeaf("c")}
			p := &reconciliationProbe{mode: mode}
			_, e := runReconciliation(t, db, &reconciliationInput{Rows: []*reconciliationRoot{row}}, p)
			if e == nil {
				t.Fatal("accepted", mode)
			}
			var name string
			if e = db.DB.QueryRow(`SELECT name FROM reconcile_root WHERE id=1`).Scan(&name); e != nil || name != "old" {
				t.Fatal("root failed rollback", name, e)
			}
			var count int
			if e = db.DB.QueryRow(`SELECT COUNT(*) FROM reconcile_labels`).Scan(&count); e != nil || count != 1 {
				t.Fatal("children failed rollback", count, e)
			}
		})
	}
}
func TestFiniteReconciliationFixedTraversalAndDeletesSQLite(t *testing.T) {
	db := newReconciliationDB(t)
	for _, role := range []string{"labels", "grants", "links", "tokens", "blocks"} {
		if e := db.ExecStatements(t.Context(), fmt.Sprintf(`INSERT INTO reconcile_%s(id,parent_id,name) VALUES(90,1,'discard')`, role)); e != nil {
			t.Fatal(e)
		}
	}
	if e := db.ExecStatements(t.Context(), `DELETE FROM reconcile_trace`); e != nil {
		t.Fatal(e)
	}
	row := reconcileRow()
	row.Labels = []*reconciliationLeaf{reconcileLeaf("new label")}
	row.Grants = []*reconciliationLeaf{reconcileLeaf("a")}
	row.Links = []*reconciliationLeaf{reconcileLeaf("new link")}
	row.Tokens = []*reconciliationLeaf{reconcileLeaf("new token")}
	row.Blocks = []*reconciliationLeaf{reconcileLeaf("new block")}
	p := &reconciliationProbe{}
	result, e := runReconciliation(t, db, &reconciliationInput{Rows: []*reconciliationRoot{row}, Method: "PUT"}, p)
	if e != nil {
		t.Fatal(e)
	}
	want := []string{"Rows:update", "root:1", "Rows/Labels:delete", "Rows/Labels:delete", "Rows/Labels:insert", "Rows/Grants:delete", "Rows/Grants:update", "Rows/Links:delete", "Rows/Links:insert", "Rows/Tokens:delete", "Rows/Tokens:insert", "Rows/Blocks:delete", "Rows/Blocks:insert", "batch"}
	if !reflect.DeepEqual(p.trace, want) {
		t.Fatalf("queue order=%v want=%v", p.trace, want)
	}
	final := result.(*reconciliationOutput).Data[0]
	if len(final.Labels) != 1 || len(final.Grants) != 1 || len(final.Links) != 1 || len(final.Tokens) != 1 || len(final.Blocks) != 1 {
		t.Fatal("internal tombstones leaked into output")
	}
	rows, e := db.DB.Query(`SELECT value FROM reconcile_trace ORDER BY seq`)
	if e != nil {
		t.Fatal(e)
	}
	defer rows.Close()
	var physical []string
	for rows.Next() {
		var value string
		if e = rows.Scan(&value); e != nil {
			t.Fatal(e)
		}
		physical = append(physical, value)
	}
	expected := []string{"labels:DELETE:10", "labels:DELETE:90", "labels:INSERT:91", "grants:DELETE:90", "grants:UPDATE:40", "links:DELETE:90", "links:INSERT:91", "tokens:DELETE:90", "tokens:INSERT:91", "blocks:DELETE:90", "blocks:INSERT:91"}
	if !reflect.DeepEqual(physical, expected) {
		t.Fatalf("physical SQLite order=%v want=%v", physical, expected)
	}
}

func TestFiniteReconciliationSuppliedIdentityAndOriginalSQLite(t *testing.T) {
	for _, kind := range []string{"omitted", "zero", "null", "matched different", "unknown nonzero"} {
		t.Run(kind, func(t *testing.T) {
			db := newReconciliationDB(t)
			row := reconcileRow()
			leaf := reconcileLeaf("a")
			switch kind {
			case "zero":
				leaf.Has.ID = true
			case "null":
				leaf.Has.ID = true
				leaf.ID = nil
			case "matched different":
				if e := db.ExecStatements(t.Context(), `INSERT INTO reconcile_grants(id,parent_id,name) VALUES(55,1,'other')`); e != nil {
					t.Fatal(e)
				}
				id := 55
				leaf.ID = &id
				leaf.Has.ID = true
			case "unknown nonzero":
				id := 777
				leaf.ID = &id
				leaf.Has.ID = true
				leaf.Name = new(string)
				*leaf.Name = "new supplied"
			}
			row.Grants = []*reconciliationLeaf{leaf}
			p := &reconciliationProbe{}
			result, e := runReconciliation(t, db, &reconciliationInput{Rows: []*reconciliationRoot{row}, Method: "PUT"}, p)
			if e != nil {
				t.Fatal(e)
			}
			final := result.(*reconciliationOutput).Data[0].Grants
			effective := 40
			if kind == "unknown nonzero" {
				effective = 777
			}
			if len(final) != 1 || *final[0].ID != effective {
				t.Fatal("effective identity", reconcileIDs(final))
			}
			wantOriginal := kind != "omitted"
			if !reflect.DeepEqual(p.originals["Grants"], []bool{wantOriginal}) {
				t.Fatal("Original was recaptured", p.originals)
			}
			if kind == "matched different" {
				var n int
				if e = db.DB.QueryRow(`SELECT COUNT(*) FROM reconcile_grants WHERE id=55`).Scan(&n); e != nil || n != 0 {
					t.Fatal("PUT did not delete superseded Current55", n, e)
				}
				if !reflect.DeepEqual(p.allocations["Grants"], []int{55}) {
					t.Fatal("supplied identity rewritten in allocation evidence", p.allocations)
				}
			}
		})
	}
}
func TestFiniteReconciliationRepeatedRootsAndSameParentDefaultsSQLite(t *testing.T) {
	for _, alias := range []bool{false, true} {
		t.Run(fmt.Sprint(alias), func(t *testing.T) {
			db := newReconciliationDB(t)
			if e := db.ExecStatements(t.Context(), `INSERT INTO reconcile_root(id,name) VALUES(2,'foreign')`, `INSERT INTO reconcile_grants(id,parent_id,name) VALUES(45,2,'foreign default')`); e != nil {
				t.Fatal(e)
			}
			first, second := reconcileRow(), reconcileRow()
			if alias {
				second = first
			}
			p := &reconciliationProbe{}
			result, e := runReconciliation(t, db, &reconciliationInput{Rows: []*reconciliationRoot{first, second}}, p)
			if e != nil {
				t.Fatal(e)
			}
			roots := result.(*reconciliationOutput).Data
			if len(roots) != 2 || roots[0] == roots[1] {
				t.Fatal("root occurrence association collapsed")
			}
			for _, root := range roots {
				if !reflect.DeepEqual(reconcileIDs(root.Grants), []int{40}) || *root.Grants[0].ParentID != 1 {
					t.Fatal("foreign default selected", reconcileIDs(root.Grants))
				}
			}
			if roots[0].Grants[0] == roots[1].Grants[0] {
				t.Fatal("Current projection copies share a pointer")
			}
			var name string
			if e = db.DB.QueryRow(`SELECT name FROM reconcile_grants WHERE id=45`).Scan(&name); e != nil || name != "foreign default" {
				t.Fatal("foreign row changed")
			}
		})
	}
}
func TestFiniteReconciliationForeignCurrentIdentityRejectsBeforeAllocationSQLite(t *testing.T) {
	db := newReconciliationDB(t)
	if e := db.ExecStatements(t.Context(), `INSERT INTO reconcile_root(id,name) VALUES(2,'foreign')`, `INSERT INTO reconcile_grants(id,parent_id,name) VALUES(45,2,'foreign')`); e != nil {
		t.Fatal(e)
	}
	row := reconcileRow()
	leaf := reconcileLeaf("foreign")
	id := 45
	leaf.ID = &id
	leaf.Has.ID = true
	row.Grants = []*reconciliationLeaf{leaf}
	p := &reconciliationProbe{}
	_, e := runReconciliation(t, db, &reconciliationInput{Rows: []*reconciliationRoot{row}}, p)
	if e == nil || !strings.Contains(e.Error(), "outside parent scope") || p.calls != 0 {
		t.Fatal("foreign parent admitted", e, p.calls)
	}
	var infra int
	if e = db.DB.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE name LIKE 'sqlx_sequence%'`).Scan(&infra); e != nil || infra != 0 {
		t.Fatal("foreign rejection allocated", infra, e)
	}
}
func TestFiniteReconciliationLatePhysicalFailuresSQLite(t *testing.T) {
	for _, role := range []string{"labels", "grants", "links", "tokens", "blocks"} {
		t.Run(role, func(t *testing.T) {
			db := newReconciliationDB(t)
			if e := db.ExecStatements(t.Context(), fmt.Sprintf(`CREATE TRIGGER late_failure BEFORE INSERT ON reconcile_%s WHEN NEW.name='blocked' BEGIN SELECT RAISE(ABORT,'late physical failure'); END`, role)); e != nil {
				t.Fatal(e)
			}
			row := reconcileRow()
			row.Labels = []*reconciliationLeaf{reconcileLeaf("l")}
			row.Grants = []*reconciliationLeaf{reconcileLeaf("g")}
			row.Links = []*reconciliationLeaf{reconcileLeaf("i")}
			row.Tokens = []*reconciliationLeaf{reconcileLeaf("t")}
			row.Blocks = []*reconciliationLeaf{reconcileLeaf("b")}
			switch role {
			case "labels":
				*row.Labels[0].Name = "blocked"
			case "grants":
				*row.Grants[0].Name = "blocked"
			case "links":
				*row.Links[0].Name = "blocked"
			case "tokens":
				*row.Tokens[0].Name = "blocked"
			case "blocks":
				*row.Blocks[0].Name = "blocked"
			}
			p := &reconciliationProbe{}
			_, e := runReconciliation(t, db, &reconciliationInput{Rows: []*reconciliationRoot{row}, Method: "PUT"}, p)
			if e == nil || !strings.Contains(e.Error(), "late physical failure") {
				t.Fatal("physical failure missing", e)
			}
			for _, table := range []string{"labels", "grants", "links", "tokens", "blocks"} {
				want := 0
				if table == "labels" || table == "grants" {
					want = 1
				}
				var actual int
				if e = db.DB.QueryRow("SELECT COUNT(*) FROM reconcile_" + table).Scan(&actual); e != nil || actual != want {
					t.Fatalf("%s rollback count=%d want=%d err=%v", table, actual, want, e)
				}
			}
		})
	}
}
func TestFiniteReconciliationSuppressedRootKeepsChildrenSQLite(t *testing.T) {
	db := newReconciliationDB(t)
	row := reconcileRow()
	row.Links = []*reconciliationLeaf{reconcileLeaf("child")}
	p := &reconciliationProbe{mode: "suppressed"}
	if _, e := runReconciliation(t, db, &reconciliationInput{Rows: []*reconciliationRoot{row}}, p); e != nil {
		t.Fatal(e)
	}
	var name string
	if e := db.DB.QueryRow(`SELECT name FROM reconcile_root WHERE id=1`).Scan(&name); e != nil || name != "old" {
		t.Fatal("root persisted", name, e)
	}
	for _, step := range p.trace {
		if strings.HasPrefix(step, "root:") {
			t.Fatal("suppressed root observed", p.trace)
		}
	}
	var n int
	if e := db.DB.QueryRow(`SELECT COUNT(*) FROM reconcile_links`).Scan(&n); e != nil || n != 1 {
		t.Fatal("child suppressed", n, e)
	}
}
func TestFiniteReconciliationCancellationAndInitialValidationSQLite(t *testing.T) {
	for _, kind := range []string{"cancel", "invalid discarded"} {
		t.Run(kind, func(t *testing.T) {
			db := newReconciliationDB(t)
			row := reconcileRow()
			row.Labels = []*reconciliationLeaf{reconcileLeaf("a")}
			p := &reconciliationProbe{mode: kind}
			if kind == "cancel" {
				p.base, p.cancel = context.WithCancel(t.Context())
				defer p.cancel()
			} else {
				row.Labels[0].Name = nil
			}
			_, e := runReconciliation(t, db, &reconciliationInput{Rows: []*reconciliationRoot{row}}, p)
			if e == nil {
				t.Fatal("accepted", kind)
			}
			if kind == "cancel" && !errors.Is(e, context.Canceled) {
				t.Fatal(e)
			}
			if kind == "invalid discarded" {
				if p.calls != 0 {
					t.Fatal("invalid discarded row reached reconciliation")
				}
				var infra int
				if e = db.DB.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE name LIKE 'sqlx_sequence%'`).Scan(&infra); e != nil || infra != 0 {
					t.Fatal("initial validation allocated", infra, e)
				}
			}
			var name string
			if e = db.DB.QueryRow(`SELECT name FROM reconcile_root WHERE id=1`).Scan(&name); e != nil || name != "old" {
				t.Fatal("rollback", name, e)
			}
		})
	}
}
func TestFiniteReconciliationStaleAttemptReferenceSQLite(t *testing.T) {
	db := newReconciliationDB(t)
	row := reconcileRow()
	row.Grants = []*reconciliationLeaf{reconcileLeaf("a")}
	p := &reconciliationProbe{}
	if _, e := runReconciliation(t, db, &reconciliationInput{Rows: []*reconciliationRoot{row}}, p); e != nil {
		t.Fatal(e)
	}
	p.mode = "stale"
	next := reconcileRow()
	next.Grants = []*reconciliationLeaf{reconcileLeaf("b")}
	_, e := runReconciliation(t, db, &reconciliationInput{Rows: []*reconciliationRoot{next}}, p)
	if e == nil || !strings.Contains(e.Error(), "stale, forged") {
		t.Fatal("stale ticket admitted", e)
	}
}
func TestFiniteReconciliationNewRootSQLite(t *testing.T) {
	for _, mode := range []string{"new root", "suppressed"} {
		t.Run(mode, func(t *testing.T) {
			db := newReconciliationDB(t)
			name := "new root"
			row := &reconciliationRoot{ID: new(int), Name: &name, Has: &reconciliationRootHas{Name: true}, Links: []*reconciliationLeaf{reconcileLeaf("child")}}
			p := &reconciliationProbe{mode: mode}
			output, e := runReconciliation(t, db, &reconciliationInput{Rows: []*reconciliationRoot{row}}, p)
			if mode == "suppressed" {
				if e == nil || !strings.Contains(e.Error(), "cannot suppress an INSERT root") {
					t.Fatal("unpersisted parent admitted", e)
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			result := output.(*reconciliationOutput).Data[0]
			if *result.ID != 2 || *result.Links[0].ParentID != 2 {
				t.Fatal("native root/FK allocation lost")
			}
		})
	}
}

func TestFiniteReconciliationAfterValidateInputIntegrationSQLite(t *testing.T) {
	db := newReconciliationDB(t)
	row := reconcileRow()
	discarded := reconcileLeaf("filtered")
	row.Grants = []*reconciliationLeaf{discarded, reconcileLeaf("a"), reconcileLeaf("new")}
	p := &reconciliationProbe{mode: "filter before reconcile"}
	result, err := runReconciliation(t, db, &reconciliationInput{Rows: []*reconciliationRoot{row}}, p)
	if err != nil {
		t.Fatal(err)
	}
	effective := result.(*reconciliationOutput).Data[0]
	if !p.filtered || *discarded.ID != 0 || !reflect.DeepEqual(p.allocations["Grants"], []int{41, 42}) || !reflect.DeepEqual(reconcileIDs(effective.Grants), []int{40, 42}) || effective.Count != 2 {
		t.Fatalf("filter=%v discarded=%d allocated=%v final=%v count=%d", p.filtered, *discarded.ID, p.allocations, reconcileIDs(effective.Grants), effective.Count)
	}
	var count int
	if err := db.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM reconcile_grants WHERE name='filtered'").Scan(&count); err != nil || count != 0 {
		t.Fatalf("filtered row persisted: count=%d err=%v", count, err)
	}
	next := reconcileRow()
	next.Grants = []*reconciliationLeaf{reconcileLeaf("next")}
	q := &reconciliationProbe{}
	if _, err := runReconciliation(t, db, &reconciliationInput{Rows: []*reconciliationRoot{next}}, q); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(q.allocations["Grants"], []int{43}) {
		t.Fatalf("unexpected next allocation %v", q.allocations)
	}
}
