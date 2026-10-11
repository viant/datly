package sql

import (
	"context"
	"github.com/viant/bindly/resource"
	"strings"
	"testing"
	"testing/fstest"
)

func TestParameterizedEmbedScopedRecursiveResources(t *testing.T) {
	store := resource.New()
	if err := store.Register("allocator", fstest.MapFS{
		"pacing.sql":     {Data: []byte(`${embed:pace/slot.sql}`)},
		"pace/slot.sql":  {Data: []byte(`$View.ParentJoinOn("AND","${holderKey}") ${embed({"holderKey":"child.ID"}):child.sql}`)},
		"pace/child.sql": {Data: []byte(`$View.ParentJoinOn("AND","${holderKey}")`)},
	}); err != nil {
		t.Fatal(err)
	}
	source := `${embed({"holderKey":"ao.ID"}):allocator:pacing.sql} JOIN ${embed({"holderKey":"ao.CAMPAIGN_ID"}):allocator:pacing.sql}`
	got, err := ExpandEmbeddedResources(context.Background(), source, store)
	if err != nil {
		t.Fatal(err)
	}
	want := `$View.ParentJoinOn("AND","ao.ID") $View.ParentJoinOn("AND","child.ID") JOIN $View.ParentJoinOn("AND","ao.CAMPAIGN_ID") $View.ParentJoinOn("AND","child.ID")`
	if got != want {
		t.Fatalf("got %s want %s", got, want)
	}
	if strings.Contains(got, "${embed") || strings.Contains(got, "${holderKey}") {
		t.Fatal("unresolved template")
	}
	again, err := ExpandEmbeddedResources(context.Background(), source, store)
	if err != nil || again != want {
		t.Fatal("resource authority mutated")
	}
}
func TestParameterizedEmbedJSONAndFailureBoundaries(t *testing.T) {
	assets := fstest.MapFS{"value.sql": {Data: []byte(`${name}/${count}/${enabled}`)}, "cycle.sql": {Data: []byte(`${embed:cycle.sql}`)}}
	got, err := ExpandEmbeddedResources(context.Background(), `${embed({"name":"a)b","count":2,"enabled":true}):value.sql}`, assets)
	if err != nil || got != "a)b/2/true" {
		t.Fatalf("got %s err %v", got, err)
	}
	for _, source := range []string{`${embed(null):value.sql}`, `${embed({):value.sql}`, `${embed({"name":"x"}:value.sql}`, `${embed:cycle.sql}`, `${embed:missing.sql}`, `${embed:../value.sql}`, `${embed:value.sql`} {
		if _, err = ExpandEmbeddedResources(context.Background(), source, assets); err == nil {
			t.Fatalf("invalid reference accepted: %s", source)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = ExpandEmbeddedResources(ctx, `${embed:value.sql}`, assets); err == nil {
		t.Fatal("canceled expansion proceeded")
	}
}
