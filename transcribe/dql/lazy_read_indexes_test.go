package dql

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/tag"
)

func TestLazyReadIndexesRoundTrip(t *testing.T) {
	const route = "#setting($_ = $route('/records','PATCH'))\n"
	const directive = "#setting($_ = $lazy_read_indexes())\n"
	parse := func(source string) (*spec.Component, error) {
		return parseComponentSource("example.com/records", "Records", source)
	}
	component, err := parse(route + directive + "SELECT id FROM records")
	if err != nil {
		t.Fatal(err)
	}
	if component.Settings == nil || !component.Settings.LazyReadIndexes {
		t.Fatal("missing DQL opt-in")
	}
	raw, err := tag.SettingsFromSpec(component.Settings).StructTag()
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := tag.ParseSettings(reflect.StructTag(raw))
	if err != nil {
		t.Fatal(err)
	}
	settings := new(spec.Settings)
	parsed.Apply(settings)
	if !settings.LazyReadIndexes {
		t.Fatal("package bootstrap lost lazyReadIndexes")
	}
	exported := (Serializer{}).Export(component, "")
	if !exported.Supported || !strings.Contains(exported.Source, directive) {
		t.Fatalf("export lost directive: %s", exported.Source)
	}
	for _, unsupported := range []string{directive + directive, "#setting($_ = $lazy_read_indexes(true))\n", "#setting($_ = $lazy_read_indexes().Optional())\n"} {
		if _, err = parse(route + unsupported + "SELECT id FROM records"); err == nil {
			t.Fatalf("accepted %q", unsupported)
		}
	}
}
