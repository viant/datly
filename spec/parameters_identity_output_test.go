package spec

import (
	"reflect"
	"testing"
)

func TestParameterIdentityCanonicalOutput(t *testing.T) {
	if (*Parameter)(nil).Identity() != "" {
		t.Fatal("nil identity")
	}
	cases := []struct {
		name, kind, source string
		emit               bool
		want               string
	}{
		{"Data", "body", "data", false, "data|body|data"},
		{"Data", "body", "data", true, "data|output|data"},
		{"Data", "output", "data", false, "data|output|data"},
		{"BodyEcho", "body", "", true, "bodyecho|output|body"},
		{"BodyEcho", "output", "body", false, "bodyecho|output|body"},
		{"Data", "output", "view", true, "data|output|view"},
		{"Data", "output", "view", false, "data|output|view"},
		{"Status", "output", "status", true, "status|output|status"},
		{"Log", "logger", "", false, "log|logger|"},
		{"Log", "logger", "", true, "log|logger||output"},
		{"Flag", "const", "Flag", false, "flag|const|flag"},
		{"Flag", "const", "Flag", true, "flag|const|flag|output"},
		{"Summary", "output", "summary", false, "summary|output|derived"},
		{"Summary", "output", "derived", true, "summary|output|derived"},
		{" Data ", " body ", " data ", true, "data|output|data"},
	}
	for _, c := range cases {
		t.Run(c.want, func(t *testing.T) {
			p := &Parameter{Name: c.name, Source: BindSource{Kind: c.kind, Name: c.source}, EmitOutput: c.emit}
			before := *p
			if got := p.Identity(); got != c.want {
				t.Fatalf("got %q want %q", got, c.want)
			}
			if !reflect.DeepEqual(before, *p) {
				t.Fatal("identity mutated authored contract")
			}
			if c.emit && p.IsMutationInput() {
				t.Fatal("outgoing parameter became mutation input")
			}
		})
	}
}

func TestEffectiveParametersCanonicalOutputAuthority(t *testing.T) {
	input := &Parameter{Name: "Data", Source: BindSource{Kind: "body", Name: "data"}, Declaration: DeclarationKindSet}
	set := &Parameter{Name: "Data", Source: BindSource{Kind: "body", Name: "data"}, EmitOutput: true, Declaration: DeclarationKindSet}
	define := &Parameter{Name: "Data", Source: BindSource{Kind: "output", Name: "data"}, Declaration: DeclarationKindDefine}
	got := EffectiveParameters([]*Parameter{input, set, define})
	if len(got) != 2 || got[0] != input || got[1] != define {
		t.Fatalf("authority: %#v", got)
	}
	if set.Source.Kind != "body" || !set.EmitOutput {
		t.Fatal("authored storage changed")
	}
}

func TestParameterIdentityReservedBodySummaryBaselineLimitation(t *testing.T) {
	authored := &Parameter{Name: "Echo", Source: BindSource{Kind: "body", Name: "summary"}, EmitOutput: true}
	reloaded := &Parameter{Name: "Echo", Source: BindSource{Kind: "output", Name: "summary"}}
	if authored.Identity() != "echo|output|summary" || reloaded.Identity() != "echo|output|derived" || authored.Identity() == reloaded.Identity() {
		t.Fatal("reserved baseline limitation changed")
	}
}
