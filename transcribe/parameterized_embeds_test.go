package transcribe

import (
	"context"
	"github.com/viant/bindly/resource"
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"
)

func TestParameterizedEmbedsDetachedAuthorityAndSourceMap(t *testing.T) {
	originalFS := fstest.MapFS{"value.sql": {Data: []byte(`SELECT '${key}' AS NAME`)}, "ordinary.sql": {Data: []byte(`SELECT 1`)}}
	store, err := resource.New().WithDefault(originalFS)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Register("named", fstest.MapFS{"child.sql": {Data: []byte(`SELECT 2`)}}); err != nil {
		t.Fatal(err)
	}
	text := `SELECT * FROM (${embed({"key":"first"}):value.sql}) a JOIN (${embed({"key":"second"}):value.sql}) b ON 1=1`
	source := &Source{Text: text, Resources: store}
	staged, patches, err := prepareParameterizedEmbeds(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	if source.Text != text || source.Resources != store || staged == source {
		t.Fatal("caller source mutated")
	}
	if len(patches) != 2 {
		t.Fatalf("patches=%d", len(patches))
	}
	for n, patch := range patches {
		replacement := string(patch.Replacement)
		name := replacement[len("${embed:") : len(replacement)-1]
		body, err := fs.ReadFile(staged.Resources, name)
		if err != nil {
			t.Fatal(err)
		}
		want := []string{"first", "second"}[n]
		if !strings.Contains(string(body), want) {
			t.Fatalf("per-use scope missing: %s", body)
		}
		if _, err = fs.ReadFile(store, name); err == nil {
			t.Fatal("virtual resource escaped to caller")
		}
	}
	for _, path := range []string{"ordinary.sql", "named:child.sql"} {
		if _, err = fs.ReadFile(staged.Resources, path); err != nil {
			t.Fatal(err)
		}
	}
	mapper := newSourceMap(len(text), patches, 0, text)
	last := strings.Index(staged.Text, " ON 1=1")
	if got, want := mapper.MapOffset(last), strings.Index(text, " ON 1=1"); got != want {
		t.Fatalf("trailing authored offset=%d want=%d", got, want)
	}
	inner := strings.Index(staged.Text, "__datly_parameterized__")
	if got, want := mapper.MapOffset(inner), patches[0].Span.Start; got != want {
		t.Fatalf("embed authored offset=%d want=%d", got, want)
	}
	again, _, err := prepareParameterizedEmbeds(context.Background(), source)
	if err != nil || again.Text != staged.Text {
		t.Fatal("repeat compile changed resource identity")
	}
}
func TestParameterizedEmbedsFailureDoesNotPublishResources(t *testing.T) {
	store, err := resource.New().WithDefault(fstest.MapFS{"value.sql": {Data: []byte(`SELECT '${key}'`)}})
	if err != nil {
		t.Fatal(err)
	}
	source := &Source{Text: `${embed({"key":"first"}):value.sql} ${embed({"key":"second"}):missing.sql}`, Resources: store}
	if _, _, err = prepareParameterizedEmbeds(context.Background(), source); err == nil {
		t.Fatal("missing resource accepted")
	}
	body, err := fs.ReadFile(store, "value.sql")
	if err != nil || string(body) != `SELECT '${key}'` {
		t.Fatal("failed staging changed original resource")
	}
}
