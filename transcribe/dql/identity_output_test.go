package dql

import (
	"strings"
	"testing"
)

func TestCanonicalOutputDuplicateAtSecondSourceSpan(t *testing.T) {
	for _, second := range []string{
		`#define($_ = $Data<string>(output/body))`,
		`#define($_ = $Data<string>(output/body).Output())`,
	} {
		source := `#define($_ = $Data<string>(body/).Output())` + "\n" + second + "\nSELECT 1"
		prepared := PrepareSource(source)
		if len(prepared.Diagnostics) != 1 {
			t.Fatalf("diagnostics %#v", prepared.Diagnostics)
		}
		d := prepared.Diagnostics[0]
		if d.Offset != strings.Index(source, second) || d.End != strings.Index(source, second)+len(second) || !strings.Contains(d.Message, "defined more than once") {
			t.Fatalf("span %#v", d)
		}
	}
}

func TestCanonicalOutputDefineOverridesSetAndPreservesInput(t *testing.T) {
	for _, decl := range []string{
		`#set($_ = $Data<Legacy>(body/).Output())
#define($_ = $Data<Preferred>(output/body))`,
		`#define($_ = $Data<Preferred>(body/).Output())
#set($_ = $Data<Legacy>(output/body))`,
	} {
		source := `#setting($_ = $route('/records','GET'))
#set($_ = $Data<string>(body/).Optional())` + "\n" + decl + "\nSELECT 1"
		c, err := parseComponentSource("example.com/identity/records", "records", source)
		if err != nil {
			t.Fatal(err)
		}
		if len(c.Parameters) != 2 {
			t.Fatalf("parameters %#v", c.Parameters)
		}
		seenIn, seenOut := false, false
		for _, p := range c.Parameters {
			if p.TypeExpr == "Preferred" {
				seenOut = true
				if p.Identity() != "data|output|body" {
					t.Fatal(p.Identity())
				}
			} else if p.TypeExpr == "string" {
				seenIn = true
				if p.Identity() != "data|body|" {
					t.Fatal(p.Identity())
				}
			} else {
				t.Fatal(p.TypeExpr)
			}
		}
		if !seenIn || !seenOut {
			t.Fatal("separate source authorities lost")
		}
	}
}
