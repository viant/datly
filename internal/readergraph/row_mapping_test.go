package readergraph

import (
	"context"
	"reflect"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"github.com/viant/x"
	loaderast "github.com/viant/x/loader/ast"
	xshape "github.com/viant/x/shape"
)

func TestReaderMetadataRowsUseNativeFieldMapping(t *testing.T) {
	for _, tc := range []struct {
		name, fields, requested, source, claimedTag string
		wrongType, reject                           bool
	}{
		{name: "SQL alias", fields: "EventDate int `sqlx:\"event_date\"`", requested: "event_date", source: "event_date"},
		{name: "exact Go", fields: "EventDate int `sqlx:\"event_date\"`", requested: "EventDate", source: "event_date"},
		{name: "ordinary Go", fields: "EventDate int", requested: "EventDate"},
		{name: "SQL case", fields: "EventDate int `sqlx:\"event_date\"`", requested: "EVENT_DATE", source: "event_date"},
		{name: "pipe result", fields: "EventDate int `sqlx:\"event_date|event_result\"`", requested: "event_result", source: "event_date"},
		{name: "selector alias", fields: "EventDate int `sqlx:\"event_date\" selectorAlias:\"event_result\"`", requested: "event_result", source: "event_date"},
		{name: "hidden mapped", fields: "EventDate int `sqlx:\"event_date\" json:\"-\" internal:\"true\"`", requested: "event_date", source: "event_date"},
		{name: "shared physical distinct result", fields: "First int `sqlx:\"raw|first_result\"`; Second int `sqlx:\"raw|second_result\"`", requested: "second_result", source: "raw"},
		{name: "alias ambiguity", fields: "First int `sqlx:\"event_date\"`; Second int `sqlx:\"event_date\"`", requested: "event_date", source: "event_date", reject: true},
		{name: "Go alias conflict", fields: "EventDate int `sqlx:\"first\"`; Other int `sqlx:\"EventDate\"`", requested: "EventDate", source: "first", reject: true},
		{name: "unmapped", fields: "EventDate int", requested: "event_date", reject: true},
		{name: "wrong source", fields: "EventDate int `sqlx:\"event_date\"`", requested: "event_date", source: "other", reject: true},
		{name: "forged tag and source fallback", fields: "EventDate int `sqlx:\"event_date\"`", requested: "unowned", source: "event_date", claimedTag: `sqlx:"unowned"`, reject: true},
		{name: "transient", fields: "EventDate int `sqlx:\"-\"`", requested: "EventDate", reject: true},
		{name: "presence", fields: "EventDate int `sqlx:\"event_date,presence=true\"`", requested: "EventDate", reject: true},
		{name: "set marker", fields: "EventDate int `setMarker:\"true\"`", requested: "EventDate", reject: true},
		{name: "wrong type", fields: "EventDate int `sqlx:\"event_date\"`", requested: "event_date", source: "event_date", wrongType: true, reject: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fsys := fstest.MapFS{"root/go.mod": {Data: []byte("module example.com/reader\n")}, "root/row.go": {Data: []byte("package reader\ntype Row struct {" + tc.fields + "}\n")}}
			loaded, err := loaderast.LoadPackageFS(context.Background(), fsys, "root")
			require.NoError(t, err)
			var row *x.Type
			for _, typ := range loaded.Types {
				if typ.Name == "Row" {
					row = &x.Type{Name: typ.Name, PkgPath: loaded.PkgPath, SynteticType: typ}
				}
			}
			require.NotNil(t, row)
			require.Nil(t, row.Type, "source proof must not use reflection")
			sourceShape := xshape.New(row, nil)
			fields, err := sourceShape.Fields()
			require.NoError(t, err)
			var reflected []reflect.StructField
			for _, f := range fields {
				reflected = append(reflected, reflect.StructField{Name: f.Name, Type: reflect.TypeFor[int](), Tag: f.Tag})
			}
			linked := x.NewType(reflect.StructOf(reflected), x.WithName("Row"), x.WithPkgPath(row.PkgPath))
			for _, variant := range []*x.Type{row, linked} {
				label := "source"
				if variant.Type != nil {
					label = "reflected"
				}
				t.Run(label, func(t *testing.T) {
					document := documentFixture()
					document.Package = variant.PkgPath
					document.Component.RootView.TypeName = variant.Key()
					identity, err := document.Component.RootView.Identity()
					require.NoError(t, err)
					document.Rows = map[string]string{identity: variant.Key()}
					typ := "int"
					if tc.wrongType {
						typ = "string"
					}
					document.Component.RootView.Columns = []*spec.Column{{Name: tc.requested, Source: tc.source, Tag: tc.claimedTag, Type: spec.TypeRef{Name: typ}}}
					originalName, originalSource := tc.requested, tc.source
					err = CheckRows(&document, func(name string) (*x.Type, error) {
						if name == variant.Key() {
							return variant, nil
						}
						return nil, nil
					})
					if tc.reject {
						require.Error(t, err)
					} else {
						require.NoError(t, err)
					}
					require.Equal(t, originalName, document.Component.RootView.Columns[0].Name)
					require.Equal(t, originalSource, document.Component.RootView.Columns[0].Source)
					require.Equal(t, tc.claimedTag, document.Component.RootView.Columns[0].Tag)
				})
			}
		})
	}
}
