package dql

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
)

func TestSequenceStrategySettings(t *testing.T) {
	for _, strategy := range []string{"", "transient", "reservation"} {
		directive := ""
		if strategy != "" {
			directive = "#setting($_ = $sequence_strategy('" + strategy + "'))\n"
		}
		component, err := parseComponentSource("example.com/records", "Records", directive+"#setting($_ = $route('/records','POST'))\nSELECT 1")
		if err != nil {
			t.Fatal(err)
		}
		actual := ""
		if component.Settings != nil {
			actual = component.Settings.SequenceStrategy
		}
		if actual != strategy {
			t.Fatalf("strategy=%q want=%q", actual, strategy)
		}
		if strategy != "" && component.Settings.IsZero() {
			t.Fatal("setting dropped as zero")
		}
		tag, err := dtag.SettingsFromSpec(component.Settings).StructTag()
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := dtag.ParseSettings(reflect.StructTag(tag))
		if err != nil {
			t.Fatal(err)
		}
		restored := &spec.Settings{}
		decoded.Apply(restored)
		if restored.SequenceStrategy != strategy {
			t.Fatal("generated settings lost strategy")
		}
		exported := (Serializer{}).Export(component, "")
		if !exported.Supported {
			t.Fatalf("strategy reconstruction lost: %+v", exported)
		}
		rebuilt, err := parseComponentSource("example.com/records", "Records", exported.Source)
		if err != nil {
			t.Fatal(err)
		}
		got := ""
		if rebuilt.Settings != nil {
			got = rebuilt.Settings.SequenceStrategy
		}
		if got != strategy {
			t.Fatal("DQL reconstruction lost strategy")
		}
	}
}
func TestSequenceStrategyRejectsInvalidAuthorship(t *testing.T) {
	for _, body := range []string{"$sequence_strategy('maxid')", "$sequence_strategy('udf')", "$sequence_strategy('unknown')", "$sequence_strategy('')", "$sequence_strategy($Input.Mode)", "$sequence_strategy('transient','reservation')", "$sequence_strategy('reservation').WithMode('x')"} {
		_, err := parseComponentSource("example.com/records", "Records", "#setting($_ = "+body+")\nSELECT 1")
		if err == nil || !strings.Contains(err.Error(), "sequence_strategy") {
			t.Fatalf("accepted or imprecisely rejected %s: %v", body, err)
		}
	}
	_, err := parseComponentSource("example.com/records", "Records", "#setting($_ = $sequence_strategy('transient'))\n#setting($_ = $sequence_strategy('reservation'))\nSELECT 1")
	if err == nil {
		t.Fatal("duplicate sequence setting accepted")
	}
}
