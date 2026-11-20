package engine

import (
	"slices"
	"testing"

	"github.com/viant/datly/internal/associationtag"
)

type identityPrefixFixture struct{}

func (identityPrefixFixture) AssociationIdentityPrefix(_ associationtag.IdentityInsert, table string) bool {
	return table == "links"
}

func TestAssociationInsertPrefixAuthority(t *testing.T) {
	for _, tc := range []struct {
		name                           string
		handler                        any
		registered, ordinary, identity []string
	}{
		{"ordinary component", struct{}{}, []string{"links", "audit"}, []string{"links", "audit"}, nil},
		{"role only", identityPrefixFixture{}, nil, nil, nil},
		{"exact component grant", identityPrefixFixture{}, []string{"audit", "links"}, []string{"audit"}, []string{"links"}},
		{"other table only", identityPrefixFixture{}, []string{"audit"}, []string{"audit"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ordinary, identity := splitAssociationTables(tc.handler, tc.registered)
			if !slices.Equal(tc.ordinary, ordinary) || !slices.Equal(tc.identity, identity) {
				t.Fatalf("ordinary %v identity %v", ordinary, identity)
			}
		})
	}
}

func TestAssociationInsertRejectsZeroAuthority(t *testing.T) {
	var capability dmlCapability
	err := capability.InsertAssociationIdentity(t.Context(), associationtag.IdentityInsert{}, "links", nil, nil)
	if err == nil || err.Error() != "native association insert authority required" {
		t.Fatalf("zero grant: %v", err)
	}
}
