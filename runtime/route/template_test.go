package route

import "testing"

func TestPathTemplateMatchEscapedPath(t *testing.T) {
	template, err := CompilePathTemplate("/orders/{orderID}/items/{itemID}")
	if err != nil {
		t.Fatal(err)
	}
	params, ok, err := template.MatchEscapedPath("/orders/a%2Fb/items/7")
	if err != nil || !ok {
		t.Fatalf("MatchEscapedPath() ok=%v err=%v", ok, err)
	}
	if params["orderID"] != "a/b" || params["itemID"] != "7" {
		t.Fatalf("MatchEscapedPath() params=%v", params)
	}
	if _, ok, err := template.MatchEscapedPath("/orders/a/b/items/7"); err != nil || ok {
		t.Fatalf("split encoded value matched: ok=%v err=%v", ok, err)
	}
	if _, ok, err := template.MatchEscapedPath("/orders/%zz/items/7"); err == nil || ok {
		t.Fatalf("malformed escape accepted: ok=%v err=%v", ok, err)
	}
}

func TestCompilePathTemplateValidationAndIsolation(t *testing.T) {
	for _, path := range []string{"orders/{id}", "/orders/{id}/{id}", "/orders/{bad-name}", "/orders/{id", "/orders?id=1"} {
		if _, err := CompilePathTemplate(path); err == nil {
			t.Fatalf("CompilePathTemplate(%q) expected error", path)
		}
	}
	template, err := CompilePathTemplate("/orders/{id}")
	if err != nil {
		t.Fatal(err)
	}
	parameters := template.Parameters()
	parameters[0] = "changed"
	if template.Parameters()[0] != "id" || template.Path() != "/orders/{id}" {
		t.Fatalf("PathTemplate exposed mutable state")
	}
	escaped, err := CompilePathTemplate("/order history/{id}")
	if err != nil || escaped.EscapedTemplatePath() != "/order%20history/{id}" {
		t.Fatalf("EscapedTemplatePath()=%q err=%v", escaped.EscapedTemplatePath(), err)
	}
}

func TestPathTemplateStaticSegmentRejectsEncodedSlash(t *testing.T) {
	template, err := CompilePathTemplate("/orders/current")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := template.MatchEscapedPath("/orders/curr%2Fent"); err != nil || ok {
		t.Fatalf("static encoded slash matched: ok=%v err=%v", ok, err)
	}
}
