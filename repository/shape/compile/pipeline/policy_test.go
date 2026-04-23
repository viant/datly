package pipeline

import (
	"testing"

	"github.com/stretchr/testify/assert"
	dqlstmt "github.com/viant/datly/repository/shape/dql/statement"
)

func TestClassify_ReadOnly(t *testing.T) {
	decision := Classify(dqlstmt.New("SELECT id FROM orders"))
	assert.True(t, decision.HasRead)
	assert.False(t, decision.HasExec)
	assert.False(t, decision.HasUnknown)
}

func TestClassify_ExecOnly(t *testing.T) {
	decision := Classify(dqlstmt.New("UPDATE orders SET id = 1"))
	assert.False(t, decision.HasRead)
	assert.True(t, decision.HasExec)
	assert.False(t, decision.HasUnknown)
}

func TestClassify_Mixed(t *testing.T) {
	decision := Classify(dqlstmt.New("SELECT id FROM orders\nUPDATE orders SET id = 1"))
	assert.True(t, decision.HasRead)
	assert.True(t, decision.HasExec)
	assert.False(t, decision.HasUnknown)
}

func TestClassify_UnknownTemplateOnly(t *testing.T) {
	decision := Classify(dqlstmt.New("$Foo.Bar($x)"))
	assert.False(t, decision.HasRead)
	assert.False(t, decision.HasExec)
	assert.True(t, decision.HasUnknown)
}
