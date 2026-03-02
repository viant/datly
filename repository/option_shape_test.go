package repository

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithShapePipeline(t *testing.T) {
	opts := NewOptions(nil)
	assert.False(t, opts.shapePipeline)

	WithShapePipeline(true)(opts)
	assert.True(t, opts.shapePipeline)

	WithShapePipeline(false)(opts)
	assert.False(t, opts.shapePipeline)
}

func TestWithLegacyTypeContext(t *testing.T) {
	opts := NewOptions(nil)
	assert.False(t, opts.legacyTypeContext)

	WithLegacyTypeContext(true)(opts)
	assert.True(t, opts.legacyTypeContext)

	WithLegacyTypeContext(false)(opts)
	assert.False(t, opts.legacyTypeContext)
}
