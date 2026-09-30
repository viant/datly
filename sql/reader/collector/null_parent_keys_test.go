package collector

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	sqlxio "github.com/viant/sqlx/io"
	"github.com/viant/xunsafe"
)

type nullableKeyParent struct {
	Key   any
	Other any
}

func TestParentPlaceholdersExcludeNullKeys(t *testing.T) {
	zero, id := 0, 7
	var absent *int
	for _, captured := range []bool{false, true} {
		for _, composite := range []bool{false, true} {
			t.Run(fmtKeyCase(captured, composite), func(t *testing.T) {
				parent := newTestView(&data.View{}, reflect.TypeFor[nullableKeyParent]())
				child := newTestView(&data.View{}, reflect.TypeFor[childPlaceholderRow]())
				parentLinks := Links{newTestLink("p", "key", "Key", nil)}
				childLinks := Links{newTestLink("c", "key", "Key", nil)}
				if composite {
					parentLinks = append(parentLinks, newTestLink("p", "other", "Other", nil))
					childLinks = append(childLinks, newTestLink("c", "other", "Other", nil))
				}
				if !captured {
					for _, link := range parentLinks {
						link.XField = xunsafe.FieldByName(reflect.TypeFor[nullableKeyParent](), link.Field)
					}
				}
				parent.Relations = []*Relation{newTestRelation(parent, child, "Children", "", spec.CardinalityMany, parentLinks, childLinks)}
				var rows []nullableKeyParent
				col := NewCollector(parent, &rows, false)
				for _, value := range []any{nil, absent, &absent, &zero, &id, false, ""} {
					row := col.NewItem()().(*nullableKeyParent)
					row.Key, row.Other = value, ""
					if captured {
						*col.ReserveSQLKey("key") = value
						*col.ReserveSQLKey("other") = ""
					}
				}
				if composite {
					row := col.NewItem()().(*nullableKeyParent)
					row.Key, row.Other = 99, absent
					if captured {
						*col.ReserveSQLKey("key") = 99
						*col.ReserveSQLKey("other") = absent
					}
				}
				col.Fetched()
				scalar, tuples, _ := col.Relations(nil)[0].ParentPlaceholders()
				if composite {
					require.Len(t, tuples, 4)
					for _, tuple := range tuples {
						require.Len(t, tuple, 2)
						require.False(t, nullRelationKey(tuple[0]))
						require.Equal(t, "", tuple[1])
					}
				} else {
					require.Len(t, scalar, 4)
					for i, key := range scalar {
						scalar[i] = sqlxio.NormalizeKey(key)
					}
					require.Equal(t, []any{0, 7, false, ""}, scalar)
				}
			})
		}
	}
}

func fmtKeyCase(captured, composite bool) string {
	name := "typed"
	if captured {
		name = "captured"
	}
	if composite {
		name += "/composite"
	} else {
		name += "/scalar"
	}
	return name
}

func TestRelationKeyNullIsNotZero(t *testing.T) {
	zero, flag, empty := 0, false, ""
	for _, value := range []any{zero, &zero, flag, &flag, empty, &empty, []byte{}} {
		require.False(t, nullRelationKey(value), "%T", value)
	}
	for _, value := range []any{nil, (*int)(nil), (*string)(nil), (*bool)(nil), []byte(nil)} {
		require.True(t, nullRelationKey(value), "%T", value)
	}
	var missing *int64
	number := int64(0)
	require.Equal(t, []any{0}, relationKeyValues([]*int64{missing, &number}))
}

func TestParentPlaceholdersNullableKeySlice(t *testing.T) {
	type row struct{ Keys []*int64 }
	parent := newTestView(&data.View{}, reflect.TypeFor[row]())
	child := newTestView(&data.View{}, reflect.TypeFor[childPlaceholderRow]())
	parent.Relations = []*Relation{newTestRelation(parent, child, "Children", "", spec.CardinalityMany,
		Links{newTestLink("p", "key", "Keys", xunsafe.FieldByName(reflect.TypeFor[row](), "Keys"))},
		Links{newTestLink("c", "key", "Key", nil)})}
	zero, seven := int64(0), int64(7)
	var rows []row
	col := NewCollector(parent, &rows, false)
	col.NewItem()().(*row).Keys = []*int64{nil, &zero, &seven, nil}
	col.Fetched()
	keys, _, _ := col.Relations(nil)[0].ParentPlaceholders()
	require.Equal(t, []any{0, 7}, keys)
}
