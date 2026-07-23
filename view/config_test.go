package view

import "testing"

func TestQueryStateParameters_CriteriaParameterUsesCriteriaQuery(t *testing.T) {
	if QueryStateParameters.CriteriaParameter == nil || QueryStateParameters.CriteriaParameter.In.Name != CriteriaQuery {
		t.Fatalf("expected CriteriaParameter query name %q, got %#v", CriteriaQuery, QueryStateParameters.CriteriaParameter)
	}
}
