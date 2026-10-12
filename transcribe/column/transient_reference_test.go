package column

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	sqlio "github.com/viant/sqlx/io"
	"reflect"
	"testing"
)

func TestReferenceConstraintPreservesTransientExclusion(t *testing.T) {
	c := &spec.Column{Name: "Ignored", Source: "GROUP_ID", Tag: `sqlx:"-" json:"-" diff:"-" internal:"true"`}
	before := c.Tag
	applyReferenceConstraint(c, &tableReference{table: "GROUPS", column: "ID"}, "GROUP_ID")
	require.Equal(t, before, c.Tag)
	require.True(t, sqlio.ParseTag(reflect.StructTag(c.Tag)).Transient)
	mapped := &spec.Column{Name: "GroupId", Source: "GROUP_ID"}
	applyReferenceConstraint(mapped, &tableReference{table: "GROUPS", column: "ID"}, "GROUP_ID")
	require.False(t, sqlio.ParseTag(reflect.StructTag(mapped.Tag)).Transient)
	require.Equal(t, "GROUPS", sqlio.ParseTag(reflect.StructTag(mapped.Tag)).RefTable)
}
