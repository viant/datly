package tool

import (
	"encoding/json"
	"fmt"
	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/spec"
	"reflect"
	"strings"
	"testing"
)

type visibilityRootPublic struct{ Root string }
type visibilityRootPrivate struct {
	Root string `internal:"true"`
}
type visibilityChildPublic struct{ Query string }
type visibilityChildPrivate struct {
	Query string `internal:"true" mcp:"name=hidden,aliases=secret_alias"`
}
type visibilityChildInvalid struct {
	Query string `internal:"true" mcp:"name=bad name,aliases=bad alias"`
}

type visibilityChildWide struct {
	First  string
	Second string
	Query  string
}

func TestTransitiveVisibilityUsesChildContract(t *testing.T) {
	for _, tc := range []struct {
		name        string
		root, child reflect.Type
		published   bool
	}{
		{"foreign index must not expose private child", reflect.TypeFor[visibilityRootPublic](), reflect.TypeFor[visibilityChildPrivate](), false},
		{"foreign index must not hide public child", reflect.TypeFor[visibilityRootPrivate](), reflect.TypeFor[visibilityChildPublic](), true},
		{"hidden malformed metadata skipped", reflect.TypeFor[visibilityRootPublic](), reflect.TypeFor[visibilityChildInvalid](), false},
		{"child index beyond root fields", reflect.TypeFor[visibilityRootPublic](), reflect.TypeFor[visibilityChildWide](), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := testRouteContract(t, tc.root, []bindly.BindingSpec{{Path: "Root", Name: "Root", Location: bindstate.Location{Kind: "query", In: "root"}}})
			child := testRouteContract(t, tc.child, []bindly.BindingSpec{{Path: "Query", Name: "Query", Location: bindstate.Location{Kind: "query", In: "q"}}})
			plan, err := NewCompiler().Compile(Input{Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "query"}, Contract: root, Fields: child.Fields()})
			if err != nil {
				t.Fatal(err)
			}
			want := 0
			if tc.published {
				want = 1
			}
			if len(plan.Arguments()) != want {
				t.Fatalf("arguments=%v want count %d", plan.Arguments(), want)
			}
		})
	}
}

type AncestorLeaf struct {
	Query string `internal:"false" mcp:"name=secret_name,aliases=secret_alias"`
}
type AncestorInvalidLeaf struct {
	Query string `internal:"false" mcp:"unsupported=invalid"`
}

func TestTransitiveAncestorMetadataIsHidden(t *testing.T) {
	for _, pointer := range []bool{false, true} {
		for _, invalid := range []bool{false, true} {
			name := fmt.Sprintf("pointer=%v invalid=%v", pointer, invalid)
			t.Run(name, func(t *testing.T) {
				leaf := reflect.TypeFor[AncestorLeaf]()
				if invalid {
					leaf = reflect.TypeFor[AncestorInvalidLeaf]()
				}
				if pointer {
					leaf = reflect.PointerTo(leaf)
				}
				rootType := reflect.StructOf([]reflect.StructField{{Name: "Node", Type: leaf}})
				childType := reflect.StructOf([]reflect.StructField{{Name: "Node", Type: leaf, Tag: `internal:"true"`}})
				binding := bindly.BindingSpec{Path: "Node.Query", Name: "Query", Location: bindstate.Location{Kind: "query", In: "secret_query"}, Extension: &spec.Parameter{Description: "secret_description", Example: "secret_example"}}
				root := testRouteContract(t, rootType, []bindly.BindingSpec{binding})
				child := testRouteContract(t, childType, []bindly.BindingSpec{binding})
				plan, err := NewCompiler().Compile(Input{Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "ancestor"}, Contract: root, Fields: child.Fields()})
				if err != nil {
					t.Fatal(err)
				}
				data, err := json.Marshal(plan.Metadata())
				if err != nil {
					t.Fatal(err)
				}
				for _, secret := range []string{"secret_name", "secret_alias", "secret_query", "secret_description", "secret_example"} {
					if strings.Contains(string(data), secret) {
						t.Fatalf("ancestor-hidden metadata leaked %s: %s", secret, data)
					}
				}
				if len(plan.Arguments()) != 0 {
					t.Fatalf("ancestor-hidden argument exposed: %v", plan.Arguments())
				}
			})
		}
	}
}

type ShadowBase struct {
	Value string `json:"value"`
}
type PrivateWinner struct {
	ShadowBase
	Value  string `json:"value" internal:"true"`
	Public string `json:"public"`
}
type PublicWinner struct {
	ShadowBase `internal:"true"`
	Value      string `json:"value"`
	Public     string `json:"public"`
}
type AmbiguousLeft struct {
	Value string `json:"value"`
}
type AmbiguousRight struct {
	Value string `json:"value"`
}
type AmbiguousWinner struct {
	AmbiguousLeft
	AmbiguousRight
	Public string `json:"public"`
}

func TestTransitiveAnonymousNativeJSONSelection(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload reflect.Type
		value   bool
	}{
		{"private winner does not revive shadow", reflect.TypeFor[PrivateWinner](), false},
		{"public winner remains", reflect.TypeFor[PublicWinner](), true},
		{"equal-depth ambiguity remains absent", reflect.TypeFor[AmbiguousWinner](), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owner := reflect.StructOf([]reflect.StructField{{Name: "Payload", Type: tc.payload, Tag: `anonymous:"true"`}})
			child := testRouteContract(t, owner, []bindly.BindingSpec{{Path: "Payload", Location: bindstate.Location{Kind: "body"}}})
			root := testRouteContract(t, reflect.TypeFor[visibilityRootPublic](), []bindly.BindingSpec{{Path: "Root", Location: bindstate.Location{Kind: "query", In: "root"}}})
			plan, err := NewCompiler().Compile(Input{Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "selection"}, Contract: root, Fields: child.Fields()})
			if err != nil {
				t.Fatal(err)
			}
			properties := plan.Metadata().InputSchema.Properties
			_, has := properties["value"]
			if has != tc.value {
				t.Fatalf("value present=%v want %v: %v", has, tc.value, properties)
			}
			if _, ok := properties["public"]; !ok {
				t.Fatal("public sibling lost")
			}
		})
	}
}

func TestTransitivePrivateAnonymousBodySkipsExpansion(t *testing.T) {
	payload := reflect.TypeFor[visibilityChildPublic]()
	node := reflect.StructOf([]reflect.StructField{{Name: "Payload", Type: payload, Tag: `anonymous:"true" internal:"false"`}})
	for _, pointer := range []bool{false, true} {
		t.Run(fmt.Sprint(pointer), func(t *testing.T) {
			typ := node
			if pointer {
				typ = reflect.PointerTo(typ)
			}
			public := reflect.StructOf([]reflect.StructField{{Name: "Node", Type: typ}})
			private := reflect.StructOf([]reflect.StructField{{Name: "Node", Type: typ, Tag: `internal:"true"`}})
			binding := bindly.BindingSpec{Path: "Node.Payload", Location: bindstate.Location{Kind: "body"}}
			root := testRouteContract(t, public, []bindly.BindingSpec{binding})
			child := testRouteContract(t, private, []bindly.BindingSpec{binding})
			plan, err := NewCompiler().Compile(Input{Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "body"}, Contract: root, Fields: child.Fields()})
			if err != nil {
				t.Fatal(err)
			}
			if len(plan.Arguments()) != 0 {
				t.Fatalf("private body expanded: %v", plan.Arguments())
			}
		})
	}
}
