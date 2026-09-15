package constant_test

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/viant/bindly/xform/conv"
	"github.com/viant/datly/constant"
	"github.com/viant/datly/spec"
)

func TestFilesPreserveScalarsAndOverridePresence(t *testing.T) {
	for _, format := range []string{"json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			text := `{"Number":9007199254740993,"Unsigned":18446744073709551615,"Zero":0,"Flag":false,"Empty":""}`
			if format == "yaml" {
				text = "Number: 9007199254740993\nUnsigned: 18446744073709551615\nZero: 0\nFlag: false\nEmpty: ''\n"
			}
			file := filepath.Join(t.TempDir(), "const."+format)
			if err := os.WriteFile(file, []byte(text), 0600); err != nil {
				t.Fatal(err)
			}
			values, err := (constant.Loader{}).Load(context.Background(), file)
			if err != nil {
				t.Fatal(err)
			}
			authored := &spec.Component{Settings: &spec.Settings{Const: map[string]string{"Zero": "17", "Flag": "true", "Empty": "default", "Default": "retained"}}}
			effective, err := values.For(authored)
			if err != nil {
				t.Fatal(err)
			}
			for name, want := range map[string]string{"Zero": "0", "Flag": "false", "Empty": "", "Default": "retained", "Number": "9007199254740993", "Unsigned": "18446744073709551615"} {
				got, found := effective.Lookup(name)
				if !found || got != want {
					t.Fatalf("%s=%q found=%v", name, got, found)
				}
			}
			for _, test := range []struct {
				name string
				typ  reflect.Type
				want any
			}{{"Number", reflect.TypeFor[int64](), int64(9007199254740993)}, {"Unsigned", reflect.TypeFor[uint64](), uint64(18446744073709551615)}, {"Zero", reflect.TypeFor[int](), 0}, {"Flag", reflect.TypeFor[bool](), false}, {"Empty", reflect.TypeFor[string](), ""}} {
				raw, _ := effective.Lookup(test.name)
				got, err := (conv.ValueConverter{}).Convert(raw, test.typ)
				if err != nil || got != test.want {
					t.Fatalf("%s: %v %v", test.name, got, err)
				}
			}
			if authored.Settings.Const["Zero"] != "17" {
				t.Fatal("authored default mutated")
			}
			data, _ := os.ReadFile(file)
			if string(data) != text {
				t.Fatal("constant file changed")
			}
		})
	}
}

func TestMalformedFiles(t *testing.T) {
	for _, test := range []struct{ ext, body string }{{"json", `{"X":null}`}, {"json", `{"X":[]}`}, {"json", `{"X":{}}`}, {"json", `{"X":1,"X":2}`}, {"json", `{"X":1,"x":2}`}, {"json", `{"Unsafe":"x"}`}, {"json", `{"bad-name":1}`}, {"json", `{X: 1}`}, {"yaml", "X: !unknown a"}, {"yaml", "X: &value text\nY: *value\n"}, {"yaml", "X: 1\n---\nY: 2"}, {"yaml", "- a"}, {"txt", "X: a"}} {
		t.Run(test.body, func(t *testing.T) {
			file := filepath.Join(t.TempDir(), "const."+test.ext)
			if err := os.WriteFile(file, []byte(test.body), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := (constant.Loader{}).Load(context.Background(), file); err == nil {
				t.Fatal("accepted malformed file")
			}
		})
	}
}

func TestValuesFreezeAndPaths(t *testing.T) {
	source := map[string]string{"Stage": "e2e", "Dollar": "$Stage"}
	values, err := constant.New(source)
	if err != nil {
		t.Fatal(err)
	}
	source["Stage"] = "prod"
	for input, want := range map[string]string{"app:${Stage}/query.sql": "app:e2e/query.sql", "https://example.test/$Stage/assets": "https://example.test/e2e/assets", "$Dollar": "$Stage"} {
		got, err := values.Path(input)
		if err != nil || got != want {
			t.Fatalf("%q: %q %v", input, got, err)
		}
	}
	for _, input := range []string{"${Missing}/x", "${Stage", "$StageSuffix"} {
		if _, err := values.Path(input); err == nil {
			t.Fatalf("accepted %q", input)
		}
	}
}

func TestInstanceCannotShadowRequestParameter(t *testing.T) {
	values, _ := constant.New(map[string]string{"project": "trusted"})
	component := &spec.Component{Parameters: []*spec.Parameter{{Name: "project", Source: spec.BindSource{Kind: "query", Name: "project"}, TypeExpr: "string"}}}
	if _, err := values.For(component); err == nil {
		t.Fatal("instance constant shadowed request parameter")
	}
}
