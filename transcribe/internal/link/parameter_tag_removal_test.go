package link

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/dql"
	"testing"
)

func TestAuthoredEmptyTransportTagsRetireInheritedMetadata(t *testing.T) {
	base := &spec.Parameter{Name: "AdOrders", Source: spec.BindSource{Kind: "body", Name: "data"}, TypeExpr: "[]*Adorder", Codec: &spec.Codec{Body: "AdorderJSONCodec"}, WireTypeExpr: "AdorderSlice", Tag: `codec:"AdorderJSONCodec" wireType:"AdorderSlice"`}
	for _, tc := range []struct {
		name, tag   string
		codec, wire bool
	}{
		{"omitted", "", true, true},
		{"unrelated", `json:"data"`, true, true},
		{"codec", `codec:""`, false, true},
		{"wire", `wireType:""`, true, false},
		{"both", `codec:"" wireType:""`, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := `#setting($_ = $route('/orders', 'PATCH'))
#define($_ = $AdOrders<[]*Adorder>(body/data).Required()`
			if tc.tag != "" {
				source += `.WithTag('` + tc.tag + `')`
			}
			source += ")\nSELECT 1"
			component, err := dql.ParsePreparedComponentSource("example.com/orders", "Orders", dql.PrepareSource(source))
			require.NoError(t, err)
			require.Len(t, component.Parameters, 1)
			authored := component.Parameters[0]
			got, err := (&Loader{}).mergeParams([]*spec.Parameter{base}, []*spec.Parameter{authored})
			require.NoError(t, err)
			require.Len(t, got, 1)
			if tc.codec {
				require.NotNil(t, got[0].Codec)
			} else {
				require.Nil(t, got[0].Codec)
			}
			if tc.wire {
				require.Equal(t, "AdorderSlice", got[0].WireTypeExpr)
			} else {
				require.Empty(t, got[0].WireTypeExpr)
			}
			require.Equal(t, "AdorderJSONCodec", base.Codec.Body)
			require.Equal(t, "AdorderSlice", base.WireTypeExpr)
		})
	}
}
