package scan

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestScanner_Result_ValidatesRelations(t *testing.T) {
	s := New()
	invalidYAML := []byte(`
Resource:
  Views:
    - Name: Parent
      Template:
        Source: SELECT p.ID FROM T p
      With:
        - Name: rel
          Holder: Rel
          Cardinality: One
          On:
            - Column: MISSING_COL
              Namespace: p
          Of:
            Ref: Child
            On:
              - Column: ID
                Namespace: c
    - Name: Child
      Template:
        Source: SELECT c.ID FROM T2 c
`)
	_, err := s.result("x", invalidYAML, "", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "dql scan relation validation failed")
	require.Contains(t, err.Error(), "column=\"MISSING_COL\"")
}

func TestScanner_Result_BuildsShapeAndIR(t *testing.T) {
	s := New()
	validYAML := []byte(`
Routes:
  - Name: Sample
    URI: /sample
    Method: GET
    View:
      Ref: root
Resource:
  Views:
    - Name: root
      Connector:
        Ref: main
      Template:
        Source: SELECT r.ID FROM ROOT r
`)
	result, err := s.result("sample", validYAML, "", nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, result.Shape)
	require.NotNil(t, result.IR)
	require.Equal(t, "root", result.Shape.Routes[0].ViewRef)
}

func TestScanner_Result_PropagatesTypeContextFromDQL(t *testing.T) {
	s := New()
	validYAML := []byte(`
Routes:
  - Name: Sample
    URI: /sample
    Method: GET
    View:
      Ref: root
Resource:
  Views:
    - Name: root
      Connector:
        Ref: main
      Template:
        Source: SELECT r.ID FROM ROOT r
`)
	dql := `
#package('mdp/performance')
#import('perf', 'github.com/acme/mdp/performance')
SELECT r.ID FROM ROOT r`
	result, err := s.result("sample", validYAML, dql, nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, result.Shape)
	require.NotNil(t, result.Shape.TypeContext)
	require.Equal(t, "mdp/performance", result.Shape.TypeContext.DefaultPackage)
	require.Len(t, result.Shape.TypeContext.Imports, 1)
	require.Equal(t, "perf", result.Shape.TypeContext.Imports[0].Alias)
}

func TestScanner_Result_ResolvesTypeProvenance(t *testing.T) {
	s := New()
	validYAML := []byte(`
Routes:
  - Name: Sample
    URI: /sample
    Method: GET
    View:
      Ref: root
Resource:
  Types:
    - Name: Order
      Package: github.com/acme/mdp/performance
      SourceURL: /repo/mdp/performance/order.go
  Views:
    - Name: root
      Connector:
        Ref: main
      Template:
        Source: SELECT r.ID FROM ROOT r
`)
	dql := `
#package('github.com/acme/mdp/performance')
SELECT cast(r.ID as 'Order') FROM ROOT r`
	result, err := s.result("sample", validYAML, dql, nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, result.Shape)
	require.Len(t, result.Shape.TypeResolutions, 1)
	resolution := result.Shape.TypeResolutions[0]
	require.Equal(t, "Order", resolution.Expression)
	require.Equal(t, "github.com/acme/mdp/performance.Order", resolution.ResolvedKey)
	require.Contains(t, []string{"default_package", "global_unique"}, resolution.MatchKind)
	require.Equal(t, "resource_type", resolution.Provenance.Kind)
	require.Equal(t, "/repo/mdp/performance/order.go", resolution.Provenance.File)
}

func TestScanner_Result_StrictProvenanceBlocksOutsideRoot(t *testing.T) {
	s := New()
	validYAML := []byte(`
Routes:
  - Name: Sample
    URI: /sample
    Method: GET
    View:
      Ref: root
Resource:
  Types:
    - Name: Order
      Package: github.com/acme/mdp/performance
      SourceURL: /outside/order.go
  Views:
    - Name: root
      Connector:
        Ref: main
      Template:
        Source: SELECT r.ID FROM ROOT r
`)
	dql := `
#package('github.com/acme/mdp/performance')
SELECT cast(r.ID as 'Order') FROM ROOT r`
	strict := true
	_, err := s.result("sample", validYAML, dql, &Request{
		Repository:       filepath.Clean(t.TempDir()),
		StrictProvenance: &strict,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "provenance policy failed")
}
