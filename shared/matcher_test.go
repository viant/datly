package shared

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/tagly/format/text"
)

func TestMatchField_NestedRelationNativeSQLXNames(t *testing.T) {
	type siteView struct {
		SiteName *string `sqlx:"NAME"`
	}
	type publisherView struct {
		PublisherID   int    `sqlx:"ID"`
		PublisherName string `sqlx:"NAME"`
	}

	testCases := []struct {
		rType      reflect.Type
		columnName string
		fieldName  string
	}{
		{reflect.TypeOf(siteView{}), "NAME", "SiteName"},
		{reflect.TypeOf(publisherView{}), "ID", "PublisherID"},
		{reflect.TypeOf(publisherView{}), "NAME", "PublisherName"},
	}
	for _, testCase := range testCases {
		field := MatchField(testCase.rType, testCase.columnName, text.CaseFormatUpperUnderscore)

		require.NotNil(t, field)
		assert.Equal(t, testCase.fieldName, field.Name)
	}
}
