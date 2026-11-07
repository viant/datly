package column

import (
	"github.com/viant/datly/internal/sqlanalysis"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	"reflect"
	"strings"
	"testing"
)

func TestPredicateForEachInputMembers(t *testing.T) {
	type member struct{ Name *string }
	catalog := typecatalog.NewCatalog()
	if err := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeFor[member](), x.WithName("Member"), x.WithPkgPath("example.com/membership"))); err != nil {
		t.Fatal(err)
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, &typecatalog.ResolutionContext{PackagePath: "example.com/membership"})
	if err != nil {
		t.Fatal(err)
	}
	inputs := newAnalysisInputs(&spec.Component{Parameters: []*spec.Parameter{{Name: "Members", TypeExpr: "[]string"}}}, nil)
	source := `SELECT f.name AS label FROM facts f WHERE (#foreach($member in $Members) f.name=$member.Name #if($foreach.HasNext) OR #end#end)`
	if _, err := sqlanalysis.ConditionalSQL(source, inputs.validate); err == nil {
		t.Fatal("accepted member on scalar collection element")
	}
	inputs.parameters["Members"].TypeExpr = "string"
	if _, err := sqlanalysis.ConditionalSQL(source, inputs.validate); err == nil {
		t.Fatal("accepted scalar foreach collection")
	}
	inputs.types = resolver
	inputs.parameters["Members"].TypeExpr = "[]*Member"
	if _, err := sqlanalysis.ConditionalSQL(source, inputs.validate); err != nil {
		t.Fatal(err)
	}
	badMember := strings.ReplaceAll(source, "$member.Name", "$member.Missing")
	if _, err := sqlanalysis.ConditionalSQL(badMember, inputs.validate); err == nil {
		t.Fatal("accepted missing member on typed collection element")
	}
	inputs.parameters["Members"].TypeExpr = "[]string"
	source = `SELECT f.name AS label FROM facts f WHERE (#foreach($member in $Members) f.name=$member #if($foreach.HasNext) OR #end#end)`
	if _, err := sqlanalysis.ConditionalSQL(source, inputs.validate); err != nil {
		t.Fatal(err)
	}
}
