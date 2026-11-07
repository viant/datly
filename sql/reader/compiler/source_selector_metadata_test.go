package compiler

import (
	"github.com/stretchr/testify/require"
	"reflect"
	"strconv"
	"testing"
)

func TestDeclaredSourceSelectorMetadata(t *testing.T) {
	type row struct {
		TaxonomyID int    `sqlx:"ID|TAXONOMY_ID" source:"ID" json:"taxonomyId"`
		Name       string `sqlx:"NAME"`
	}
	alias, cols := selectorSourceAlias(reflect.TypeFor[row](), []int{0})
	require.Equal(t, "ID", alias)
	require.Equal(t, []string{"ID", "TAXONOMY_ID"}, cols)
	type noSource struct {
		TaxonomyID int `sqlx:"ID|TAXONOMY_ID" json:"taxonomyId"`
	}
	alias, cols = selectorSourceAlias(reflect.TypeFor[noSource](), []int{0})
	require.Empty(t, alias)
	require.Empty(t, cols)
	type hiddenCollision struct {
		TaxonomyID int `sqlx:"physical|TAXONOMY_ID" source:"ID" json:"taxonomyId"`
		ID         int `sqlx:"private_id" json:"-"`
	}
	alias, cols = selectorSourceAlias(reflect.TypeFor[hiddenCollision](), []int{0})
	require.Empty(t, alias)
	require.Empty(t, cols)
	type duplicate struct {
		First  int `sqlx:"a|first_result" source:"ID"`
		Second int `sqlx:"b|second_result" source:"id"`
	}
	for _, index := range []int{0, 1} {
		alias, cols = selectorSourceAlias(reflect.TypeFor[duplicate](), []int{index})
		require.Empty(t, alias)
		require.Empty(t, cols)
	}
	type hiddenDestination struct {
		Hidden     int `sqlx:"secret|TAXONOMY_ID" json:"-"`
		TaxonomyID int `sqlx:"ID|TAXONOMY_ID" source:"ID" json:"taxonomyId"`
	}
	alias, cols = selectorSourceAlias(reflect.TypeFor[hiddenDestination](), []int{1})
	require.Equal(t, "ID", alias)
	require.Equal(t, []string{"ID"}, cols, "hidden destination may not verify public result")
}

func TestDeclaredSourceSelectorQuotedCollision(t *testing.T) {
	for _, alias := range []string{"ID", "`ID`", `"ID"`} {
		for _, other := range []reflect.StructField{
			{Name: "ID", Type: reflect.TypeFor[int](), Tag: `sqlx:"private_id" json:"-"`},
			{Name: "Hidden", Type: reflect.TypeFor[int](), Tag: `sqlx:"ID" json:"-"`},
			{Name: "Hidden", Type: reflect.TypeFor[int](), Tag: "sqlx:\"`ID`\" json:\"-\""},
			{Name: "Other", Type: reflect.TypeFor[int](), Tag: `sqlx:"other_result" source:"id"`},
			{Name: "Other", Type: reflect.TypeFor[int](), Tag: "sqlx:\"other_result\" source:\"`id`\""},
		} {
			t.Run(alias+"/"+string(other.Tag), func(t *testing.T) {
				row := reflect.StructOf([]reflect.StructField{
					{Name: "TaxonomyID", Type: reflect.TypeFor[int](), Tag: reflect.StructTag("sqlx:\"physical|TAXONOMY_ID\" source:" + strconv.Quote(alias) + " json:\"taxonomyId\"")},
					other,
				})
				got, cols := selectorSourceAlias(row, []int{0})
				require.Empty(t, got)
				require.Empty(t, cols)
				if other.Tag.Get("source") != "" {
					got, cols = selectorSourceAlias(row, []int{1})
					require.Empty(t, got, "duplicate source identity is symmetric")
					require.Empty(t, cols)
				}
			})
		}
	}
}
