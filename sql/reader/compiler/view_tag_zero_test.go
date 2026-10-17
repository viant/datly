package compiler

import (
	"testing"

	"github.com/viant/datly/data"
	dtag "github.com/viant/datly/tag"
)

func TestApplyFieldViewTagZeroLimitMeansNoLimit(t *testing.T) {
	for _, tc := range []struct {
		name, limit string
		wantNoLimit bool
		wantLimit   int
	}{
		{name: "zero", limit: ",limit=0", wantNoLimit: true},
		{name: "absent", wantLimit: -1},
		{name: "positive", limit: ",limit=3", wantLimit: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			options, err := dtag.ParseView("Records,table=records,orderBy=ID DESC,offset=2,selectorDefaultLimit=7,selectorProjection=true" + tc.limit)
			if err != nil {
				t.Fatal(err)
			}
			before := options.Selector.Clone()
			v := &data.View{}
			applyFieldViewTag(v, options)
			if v.Spec.Selector == nil || v.Spec.Selector.NoLimit != tc.wantNoLimit || v.Spec.Selector.DefaultLimit != 7 || !v.Spec.Selector.AllowFields {
				t.Fatalf("selector=%+v", v.Spec.Selector)
			}
			if options.Selector.NoLimit != before.NoLimit || options.Selector.DefaultLimit != 7 || v.Spec.Selector == options.Selector {
				t.Fatal("mutated or reused authored selector")
			}
			controls := v.Spec.Source.Controls
			if controls == nil || controls.OrderBy != "ID DESC" || controls.Offset == nil || *controls.Offset != 2 {
				t.Fatalf("controls=%+v", controls)
			}
			if tc.wantLimit <= 0 {
				if controls.Limit != nil {
					t.Fatalf("limit=%d", *controls.Limit)
				}
			} else if controls.Limit == nil || *controls.Limit != tc.wantLimit {
				t.Fatalf("limit=%v", controls.Limit)
			}
		})
	}
}
