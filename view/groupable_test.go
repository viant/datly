package view

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/tagly/format/text"
)

func TestColumn_Init_GroupableTag(t *testing.T) {
	column := &Column{
		Name:     "region",
		DataType: "string",
		Tag:      `groupable:"true"`,
	}

	err := column.Init(NewResources(EmptyResource(), &View{}), text.CaseFormatLowerUnderscore, true)
	require.NoError(t, err)
	require.True(t, column.Groupable)
}

func TestView_IsGroupable(t *testing.T) {
	groupable := &Column{Name: "region", Groupable: true}
	metric := &Column{Name: "total"}
	index := Columns{groupable, metric}.Index(text.CaseFormatLowerUnderscore)
	index.RegisterWithName("Region", groupable)

	aView := &View{
		Columns:  []*Column{groupable, metric},
		_columns: index,
	}

	require.True(t, aView.IsGroupable("region"))
	require.True(t, aView.IsGroupable("Region"))
	require.False(t, aView.IsGroupable("total"))
	require.False(t, aView.IsGroupable("missing"))
}

func TestView_inherit_Groupable(t *testing.T) {
	child := &View{}
	parent := &View{Groupable: true}

	err := child.inherit(parent)
	require.NoError(t, err)
	require.True(t, child.Groupable)
}
