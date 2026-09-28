package shared

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/tagly/format/text"
)

func TestMatchField_SQLXAlternativeNames(t *testing.T) {
	type publisherView struct {
		PublisherID int `sqlx:"ID|PUBLISHER_ID"`
	}

	for _, columnName := range []string{"ID", "PUBLISHER_ID"} {
		t.Run(columnName, func(t *testing.T) {
			field := MatchField(reflect.TypeOf(publisherView{}), columnName, text.CaseFormatUpperUnderscore)

			require.NotNil(t, field)
			assert.Equal(t, "PublisherID", field.Name)
		})
	}
}
