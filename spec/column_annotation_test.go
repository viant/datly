package spec

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
	"testing"
)

func TestAnnotationProvenanceIsTransientAndCloned(t *testing.T) {
	original := &Column{Name: "value", AnnotationOnly: true}
	cloned := original.Clone()
	require.True(t, cloned.AnnotationOnly)
	require.NotSame(t, original, cloned)
	encoded, err := json.Marshal(cloned)
	require.NoError(t, err)
	var fromJSON Column
	require.NoError(t, json.Unmarshal(encoded, &fromJSON))
	require.False(t, fromJSON.AnnotationOnly)
	encoded, err = yaml.Marshal(cloned)
	require.NoError(t, err)
	var fromYAML Column
	require.NoError(t, yaml.Unmarshal(encoded, &fromYAML))
	require.False(t, fromYAML.AnnotationOnly)
}
