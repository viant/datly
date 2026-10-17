package transcribe

import (
	"context"
	"testing"
	"testing/fstest"

	"github.com/viant/bindly/resource"
)

func TestSourcePreparationRequiresSameSourceAuthority(t *testing.T) {
	store := func() *resource.Store {
		result, err := resource.New().WithDefault(fstest.MapFS{})
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	source := &Source{Text: "#setting($_ = $route('/records','GET'))\nSELECT 1", Path: "source/query.dql", Resources: store()}
	first, err := prepareSourceCompilation(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	copy := *source
	again, err := prepareSourceCompilation(context.Background(), &copy)
	if err != nil || again != first {
		t.Fatalf("source lineage did not share preparation: %v", err)
	}
	for _, change := range []func(*Source){
		func(s *Source) { s.Text += "\n" },
		func(s *Source) { s.Resources = store() },
		func(s *Source) { s.Path = "other/query.dql" },
	} {
		copy := *source
		change(&copy)
		changed, err := prepareSourceCompilation(context.Background(), &copy)
		if err != nil || changed == first || changed.prepared == first.prepared {
			t.Fatalf("changed source authority reused preparation: %v", err)
		}
	}
}
