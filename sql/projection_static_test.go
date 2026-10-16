package sql

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/viant/datly/data"
)

func staticTestView() *data.View {
	return &data.View{Columns: []*data.Column{{Name: "ID", Column: "ID"}, {Name: "NAME", Column: "NAME"}}, SelectorFields: []data.SelectorField{{GoName: "Id", PublicName: "id", Column: "ID"}, {GoName: "Name", PublicName: "name", Column: "NAME"}}}
}

func TestStaticProjectionRetainsNativeSelection(t *testing.T) {
	source := `SELECT records.* FROM (SELECT r.ID, r.NAME FROM records r ${predicate.Builder().CombineOr($predicate.FilterGroup(0,"AND")).Build("WHERE")}) records`
	view := staticTestView()
	plan, err := NewStaticProjection(source, view, nil)
	if err != nil {
		t.Fatal(err)
	}
	selected, err := plan.Select([]string{"nAmE"})
	if err != nil {
		t.Fatal(err)
	}
	evaluated := `SELECT records.* FROM (SELECT r.ID, r.NAME FROM records r WHERE r.ID=?) records`
	prepared, err := selected.Prepare(source, view, evaluated)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Source != evaluated || prepared.Render(evaluated) != `SELECT NAME FROM (`+evaluated+`) AS datly_view` {
		t.Fatalf("unexpected rendering %s", prepared.Render(evaluated))
	}
	if !strings.Contains(source, "${predicate") {
		t.Fatal("source template changed")
	}
	_, err = plan.Select([]string{"MiSsInG"})
	var unknown *UnknownProjectionColumnError
	if !errors.As(err, &unknown) || unknown.RequestedColumn != "MiSsInG" {
		t.Fatalf("requested identity lost: %v", err)
	}
	if _, err = selected.Prepare(source, staticTestView(), evaluated); err == nil {
		t.Fatal("foreign view selection admitted")
	}
	if _, err = selected.Prepare(source+" ", view, evaluated); err == nil {
		t.Fatal("foreign source selection admitted")
	}
	if _, err = (&StaticProjectionSelection{}).Prepare(source, view, evaluated); err == nil {
		t.Fatal("unchecked zero selection admitted")
	}
}

func TestStaticProjectionPlainAndConcurrentInvocations(t *testing.T) {
	source := `SELECT records.* FROM (SELECT r.ID, r.NAME FROM records r) records`
	view := staticTestView()
	plan, err := NewStaticProjection(source, view, nil)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			field := []string{"id", "name"}[i%2]
			selected, err := plan.Select([]string{field})
			if err != nil {
				t.Error(err)
				return
			}
			evaluated := fmt.Sprintf("%s WHERE records.ID=%d", source, i)
			first, err := selected.Prepare(source, view, evaluated)
			if err != nil {
				t.Error(err)
				return
			}
			first.Source = "changed invocation copy"
			second, err := selected.Prepare(source, view, evaluated)
			if err != nil {
				t.Error(err)
				return
			}
			if second.Source != evaluated || !strings.HasPrefix(second.Render(evaluated), "SELECT "+strings.ToUpper(field)+" FROM (") {
				t.Error("selection leaked between invocations")
			}
		}(i)
	}
	wg.Wait()
}

func TestStaticProjectionRejectsUnprovedSources(t *testing.T) {
	for _, source := range []string{
		`not a query`,
		`SELECT records.* FROM records`,
		`SELECT records.* FROM (SELECT r.* FROM records r) records`,
		`SELECT records.* EXCEPT(NAME) FROM (SELECT r.ID,r.NAME FROM records r) records`,
		`SELECT records.ID FROM (SELECT r.ID,r.NAME FROM records r) records`,
		`SELECT records.* FROM (SELECT r.ID,COUNT(*) AS NAME FROM records r GROUP BY r.ID) records`,
		`SELECT records.* FROM (SELECT r.ID,r.NAME FROM records r UNION SELECT r.ID,r.NAME FROM records r) records`,
		`SELECT records.* FROM (SELECT r.ID,r.NAME FROM $Unsafe.Table r) records`,
		`SELECT records.* FROM (SELECT r.ID,r.NAME FROM records r) records JOIN extra e ON e.ID=records.ID`,
	} {
		t.Run(source, func(t *testing.T) {
			if _, err := NewStaticProjection(source, staticTestView(), nil); err == nil {
				t.Fatal("unproved projection admitted")
			}
		})
	}
}

func TestStaticProjectionMatchesNativeNullAndSelectionRendering(t *testing.T) {
	source := `SELECT records.* FROM (SELECT r.ID,r.NAME FROM records r) records`
	for _, allowNulls := range []bool{false, true} {
		for _, fields := range [][]string{nil, {"NAME"}, {"NAME", "ID"}} {
			t.Run(fmt.Sprintf("nulls=%v/fields=%v", allowNulls, fields), func(t *testing.T) {
				view := staticTestView()
				view.Spec.AllowNulls = &allowNulls
				view.Columns[0].Nullable = true
				view.Columns[0].NullFallback = "0"
				plan, err := NewStaticProjection(source, view, nil)
				if err != nil {
					t.Fatal(err)
				}
				chosen, err := plan.Select(fields)
				if err != nil {
					t.Fatal(err)
				}
				prepared, err := chosen.Prepare(source, view, source)
				if err != nil {
					t.Fatal(err)
				}
				native, err := (SelectorProjection{SQL: source, View: view}).Prepare(fields)
				if err != nil {
					t.Fatal(err)
				}
				if prepared.Render(source) != native.Render(native.Source) {
					t.Fatalf("native renderer changed: static=%s native=%s", prepared.Render(source), native.Render(native.Source))
				}
			})
		}
	}
}

func TestStaticProjectionRejectsTemplateControlStructure(t *testing.T) {
	for _, source := range []string{
		`SELECT records.* FROM (SELECT r.ID #if(true),r.NAME #end FROM records r) records`,
		`SELECT records.* FROM (SELECT #foreach($name in $Names) $name, #end r.ID FROM records r) records`,
		`SELECT records.* FROM (SELECT r.ID,r.NAME FROM #if(true) records #else archive #end r) records`,
		`#if(true) SELECT records.* FROM (SELECT r.ID,r.NAME FROM records r) records #end`,
		`SELECT records.* FROM (SELECT r.ID,r.NAME FROM (SELECT r.ID,r.NAME FROM #if(true) records #else archive #end r) r) records`,
	} {
		t.Run(source, func(t *testing.T) {
			if _, err := NewStaticProjection(source, staticTestView(), nil); err == nil {
				t.Fatal("template-dependent SQL authority admitted")
			}
		})
	}
}
