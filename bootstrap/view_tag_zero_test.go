package bootstrap

import (
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
)

func TestContractResolverTaggedZeroLimitMeansNoLimit(t *testing.T) {
	for _, tc := range []struct {
		name, limit string
		wantNoLimit bool
		wantLimit   *int
	}{
		{name: "zero", limit: ",limit=0", wantNoLimit: true},
		{name: "absent"},
		{name: "positive", limit: ",limit=3", wantLimit: intPointerForTagLimit(3)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tag := `parameter:"Records,kind=view,in=Records" view:"Records,table=records,orderBy=ID DESC,offset=2,selectorDefaultLimit=7,selectorProjection=true` + tc.limit + `" sql:"SELECT ID FROM records"`
			typ := reflect.StructOf([]reflect.StructField{{Name: "Records", Type: reflect.TypeFor[[]*packageViewRow](), Tag: reflect.StructTag(tag)}})
			c, err := (ContractResolver{Component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/records", Name: "Records"}}, InputType: linkedContractType(typ)}).Resolve()
			if err != nil {
				t.Fatal(err)
			}
			if len(c.Views) != 1 {
				t.Fatalf("views=%v", c.Views)
			}
			v := c.Views[0]
			if v.Selector == nil || v.Selector.NoLimit != tc.wantNoLimit || v.Selector.DefaultLimit != 7 || !v.Selector.AllowFields {
				t.Fatalf("selector=%+v", v.Selector)
			}
			controls := v.Source.Controls
			if controls == nil || controls.OrderBy != "ID DESC" || controls.Offset == nil || *controls.Offset != 2 {
				t.Fatalf("controls=%+v", controls)
			}
			if tc.wantLimit == nil {
				if controls.Limit != nil {
					t.Fatalf("limit=%d", *controls.Limit)
				}
			} else if controls.Limit == nil || *controls.Limit != *tc.wantLimit {
				t.Fatalf("limit=%v", controls.Limit)
			}
		})
	}
}
func intPointerForTagLimit(v int) *int { return &v }
