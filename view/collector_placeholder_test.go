package view

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/viant/datly/view/state"
	"github.com/viant/xunsafe"
)

type placeholderParentRow struct {
	LinkedConversationID string
}

func TestCollector_ParentPlaceholders_SkipsBlankStringKeys(t *testing.T) {
	parentView := &View{Schema: state.NewSchema(reflect.TypeOf([]*placeholderParentRow{}))}
	parentDest := []*placeholderParentRow{
		{LinkedConversationID: ""},
		{LinkedConversationID: "child-1"},
		{LinkedConversationID: "   "},
	}
	parentCollector := NewCollector(parentView.Schema.Slice(), parentView, &parentDest, nil, false)

	relation := &Relation{
		On: Links{
			&Link{
				Field:  "LinkedConversationID",
				Column: "LINKED_CONVERSATION_ID",
				xField: xunsafe.FieldByName(reflect.TypeOf(placeholderParentRow{}), "LinkedConversationID"),
			},
		},
		Of: &ReferenceView{
			On: Links{
				&Link{Field: "ID", Column: "ID"},
			},
		},
	}

	childCollector := &Collector{parent: parentCollector, relation: relation}
	values, composite, columns := childCollector.ParentPlaceholders()

	assert.Equal(t, []interface{}{"child-1"}, values)
	assert.Nil(t, composite)
	assert.Equal(t, []string{"ID"}, columns)
}
