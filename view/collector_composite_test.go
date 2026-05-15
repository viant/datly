package view

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/view/state"
	"github.com/viant/xunsafe"
)

type compositeParentRow struct {
	AdvertiserID   int
	DmpAdobeValues string
	Adobe          []*compositeChildRow
}

type compositeChildRow struct {
	AdvertiserID  int
	DmpAdobeValue string
}

func TestCollector_ParentPlaceholders_Composite(t *testing.T) {
	parentView := &View{Schema: state.NewSchema(reflect.TypeOf([]*compositeParentRow{}))}
	parentDest := []*compositeParentRow{
		{AdvertiserID: 101, DmpAdobeValues: "A"},
		{AdvertiserID: 202, DmpAdobeValues: "B"},
	}
	parentCollector := NewCollector(parentView.Schema.Slice(), parentView, &parentDest, nil, false)

	relation := &Relation{
		Composite: true,
		On: Links{
			&Link{Field: "AdvertiserID", Column: "ADVERTISER_ID", xField: xunsafe.FieldByName(reflect.TypeOf(compositeParentRow{}), "AdvertiserID")},
			&Link{Field: "DmpAdobeValues", Column: "DMP_ADOBE_VALUES", xField: xunsafe.FieldByName(reflect.TypeOf(compositeParentRow{}), "DmpAdobeValues")},
		},
		Of: &ReferenceView{
			On: Links{
				&Link{Field: "AdvertiserID", Column: "ADVERTISER_ID"},
				&Link{Field: "DmpAdobeValue", Column: "DMP_ADOBE_VALUE"},
			},
		},
	}
	childCollector := &Collector{parent: parentCollector, relation: relation}

	values, composite, columns := childCollector.ParentPlaceholders()
	assert.Nil(t, values)
	assert.Equal(t, []string{"ADVERTISER_ID", "DMP_ADOBE_VALUE"}, columns)
	assert.Equal(t, [][]interface{}{{101, "A"}, {202, "B"}}, composite)
}

func TestCollector_MergeToParent_Composite(t *testing.T) {
	parentView := &View{Schema: state.NewSchema(reflect.TypeOf([]*compositeParentRow{}))}
	parentDest := []*compositeParentRow{
		{AdvertiserID: 101, DmpAdobeValues: "A"},
		{AdvertiserID: 202, DmpAdobeValues: "B"},
	}
	parentCollector := NewCollector(parentView.Schema.Slice(), parentView, &parentDest, nil, false)

	childView := &View{
		Schema: state.NewSchema(reflect.TypeOf([]*compositeChildRow{})),
	}
	relation := &Relation{
		Composite:   true,
		Cardinality: state.Many,
		Holder:      "Adobe",
		holderField: xunsafe.FieldByName(reflect.TypeOf(compositeParentRow{}), "Adobe"),
		On: Links{
			&Link{Field: "AdvertiserID", Column: "ADVERTISER_ID", xField: xunsafe.FieldByName(reflect.TypeOf(compositeParentRow{}), "AdvertiserID")},
			&Link{Field: "DmpAdobeValues", Column: "DMP_ADOBE_VALUES", xField: xunsafe.FieldByName(reflect.TypeOf(compositeParentRow{}), "DmpAdobeValues")},
		},
		Of: &ReferenceView{
			View: View{Schema: state.NewSchema(reflect.TypeOf([]*compositeChildRow{}))},
			On: Links{
				&Link{Field: "AdvertiserID", Column: "ADVERTISER_ID", xField: xunsafe.FieldByName(reflect.TypeOf(compositeChildRow{}), "AdvertiserID")},
				&Link{Field: "DmpAdobeValue", Column: "DMP_ADOBE_VALUE", xField: xunsafe.FieldByName(reflect.TypeOf(compositeChildRow{}), "DmpAdobeValue")},
			},
		},
	}

	childDest := []*compositeChildRow{
		{AdvertiserID: 101, DmpAdobeValue: "A"},
		{AdvertiserID: 202, DmpAdobeValue: "B"},
		{AdvertiserID: 101, DmpAdobeValue: "Z"},
	}
	childCollector := NewCollector(childView.Schema.Slice(), childView, &childDest, nil, true)
	childCollector.parent = parentCollector
	childCollector.relation = relation
	childCollector.view = childView
	childCollector.slice = childView.Schema.Slice()

	childCollector.mergeToParent()

	require.Len(t, parentDest[0].Adobe, 1)
	assert.Equal(t, "A", parentDest[0].Adobe[0].DmpAdobeValue)
	require.Len(t, parentDest[1].Adobe, 1)
	assert.Equal(t, "B", parentDest[1].Adobe[0].DmpAdobeValue)
}
