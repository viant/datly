package differ_test

import (
	"context"
	"errors"
	"github.com/viant/datly/runtime/differ"
	"github.com/viant/datly/runtime/handler/provider"
	xdiffer "github.com/viant/xdatly/differ"
	"github.com/viant/xdatly/handler"
	"reflect"
	"sync"
	"testing"
)

func TestTypedDifferenceCapability(t *testing.T) {
	type detail struct{ Count int }
	type record struct {
		Name    string
		Detail  *detail
		Ignored string `diff:"-"`
	}
	from := &record{Name: "before", Detail: &detail{Count: 1}, Ignored: "old"}
	to := &record{Name: "after", Detail: &detail{Count: 2}, Ignored: "new"}
	service := differ.New()
	providers := provider.Capabilities(handler.Capabilities{Differ: service})
	if len(providers) != 1 || providers[0].Kind() != string(handler.DifferKey) {
		t.Fatal("missing native DI capability")
	}
	value, found, err := providers[0].Locate(nil).Value(context.Background(), nil, "")
	if err != nil || !found || value != service {
		t.Fatalf("capability=%v found=%v err=%v", value, found, err)
	}
	for _, tc := range []struct {
		name    string
		shallow bool
		count   int
	}{{"deep", false, 2}, {"shallow", true, 1}} {
		t.Run(tc.name, func(t *testing.T) {
			changes, err := service.Diff(context.Background(), from, to, xdiffer.WithShallow(tc.shallow))
			if err != nil {
				t.Fatal(err)
			}
			records := changes.ToChangeRecords(xdiffer.WithSource("records"), xdiffer.WithSourceID(7), xdiffer.WithUserID("u1"))
			if len(records) != tc.count {
				t.Fatalf("changes=%+v", records)
			}
			for _, change := range records {
				if change.Source != "records" || change.SourceID != 7 || change.UserID != "u1" || change.Change != "update" {
					t.Fatalf("record=%+v", change)
				}
			}
		})
	}
	if from.Name != "before" || from.Detail.Count != 1 || to.Detail.Count != 2 {
		t.Fatal("comparison mutated input")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := service.Diff(ctx, from, to); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation=%v", err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				if _, err := service.Diff(context.Background(), from, to); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
}

func TestDifferenceCollectionsAndPresence(t *testing.T) {
	service := differ.New()
	for _, tc := range []struct {
		name     string
		from, to any
		want     map[string]string
	}{
		{"map", map[string]any{"keep": 1, "gone": 2}, map[string]any{"keep": 3, "new": 4}, map[string]string{"[keep]": "update", "[gone]": "delete", "[new]": "create"}},
		{"slice", []int{1, 2}, []int{1, 3, 4}, map[string]string{"[1]": "update", "[2]": "create"}},
		{"nil", nil, nil, map[string]string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := service.Diff(context.Background(), tc.from, tc.to)
			if err != nil {
				t.Fatal(err)
			}
			got := map[string]string{}
			for _, change := range result.Changes {
				if change.Error != "" {
					t.Fatal(change.Error)
				}
				got[change.Path.String()] = string(change.Type)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got=%v want=%v", got, tc.want)
			}
		})
	}
	type marker struct {
		Count bool
		Other bool
	}
	type record struct {
		Count int
		Other int
		Has   *marker `setMarker:"true"`
	}
	from := &record{Count: 5, Other: 9, Has: &marker{Count: true}}
	to := &record{Count: 0, Other: 0, Has: &marker{Count: true}}
	result, err := service.Diff(context.Background(), from, to, xdiffer.WithSetMarker(true))
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Changes) != 1 || result.Changes[0].Path.String() != "Count" || result.Changes[0].To != 0 {
		t.Fatalf("presence=%+v from=%T to=%T", result.Changes[0], result.Changes[0].From, result.Changes[0].To)
	}
}

func TestDifferenceNativeErrorsRemainVisible(t *testing.T) {
	changes, err := differ.New().Diff(context.Background(), map[string]int{"a": 1}, map[string]int{"a": 2})
	if err == nil || changes == nil || len(changes.Changes) != 1 || changes.Changes[0].Error == "" {
		t.Fatalf("changes=%+v err=%v", changes, err)
	}
	records := changes.ToChangeRecords()
	if len(records) != 1 || records[0].Error != changes.Changes[0].Error {
		t.Fatalf("lost comparison error: %+v", records)
	}
}
