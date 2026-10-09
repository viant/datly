package transcribe

import (
	"context"
	"github.com/viant/bindly/resource"
	"strings"
	"testing"
	"testing/fstest"
)

func TestCompilerParameterizedNestedEmbed(t *testing.T) {
	assets := resource.New()
	if err := assets.Register("allocator", fstest.MapFS{
		"pacing.sql":           {Data: []byte(`SELECT AD_ORDER_ID FROM (${embed:pace_budget/slot.sql}) t`)},
		"pace_budget/slot.sql": {Data: []byte(`SELECT ao.ID AS AD_ORDER_ID FROM orders ao $View.ParentJoinOn("WHERE","${holderKey}")`)},
	}); err != nil {
		t.Fatal(err)
	}
	source := &Source{Scope: "example.com/mdp", Name: "AllocatorEmbed", Resources: assets, Text: `#setting($_ = $route('/allocator-embed','GET'))
SELECT pacing.*, type(pacing,'Pacing'), cast(pacing.AD_ORDER_ID AS int)
FROM (${embed({"holderKey":"ao.ID"}):allocator:pacing.sql}) pacing`}
	compiled, err := NewCompiler().Compile(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	if err = resolveComponentSources(compiled.Component, compiled.Source.Resources); err != nil {
		t.Fatal(err)
	}
	sql := compiled.Component.RootSource().SQL
	if strings.Contains(sql, "${embed") || strings.Contains(sql, "${holderKey}") || !strings.Contains(sql, `$View.ParentJoinOn("WHERE","ao.ID")`) {
		t.Fatalf("original scoped expansion missing: %s", sql)
	}
}

func TestCompilerPlainEmbedControl(t *testing.T) {
	assets, err := resource.New().WithDefault(fstest.MapFS{
		"pacing.sql":           {Data: []byte(`SELECT 1 AS AD_ORDER_ID`)},
		"pace_budget/slot.sql": {Data: []byte(`SELECT ao.ID AS AD_ORDER_ID FROM orders ao`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := NewCompiler().Compile(context.Background(), &Source{Scope: "example.com/mdp", Name: "PlainEmbed", Resources: assets, Text: `#setting($_ = $route('/plain-embed','GET'))
SELECT pacing.*, type(pacing,'Pacing'), cast(pacing.AD_ORDER_ID AS int)
FROM (${embed:pacing.sql}) pacing`})
	if err != nil {
		t.Fatal(err)
	}
	if err = resolveComponentSources(compiled.Component, compiled.Source.Resources); err != nil {
		t.Fatal(err)
	}
}
