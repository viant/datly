package reader

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/viant/datly/view"
)

func TestActualLimit_PrefersSelectorNoLimit(t *testing.T) {
	aView := &view.View{Selector: &view.Config{Limit: 1}}
	selector := &view.Statelet{}
	selector.WarmupNoLimit = true
	selector.Limit = 0

	assert.Equal(t, 0, actualLimit(aView, selector))
}
