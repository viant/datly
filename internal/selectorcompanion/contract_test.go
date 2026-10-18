package selectorcompanion

import (
	"context"
	"github.com/viant/datly/spec"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/viant/x"
	loaderast "github.com/viant/x/loader/ast"
	xshape "github.com/viant/x/shape"
	"github.com/viant/xdatly"
)

type fixtureInput struct{ ID int }
type fixtureOutput struct{ Data []int }
type fixtureSelectors struct {
	Fields []string `parameter:"Fields,kind=query,in=fields,required=false" querySelector:"view=rows,property=fields"`
	Page   int      `parameter:"Page,kind=query,in=page,required=false" querySelector:"view=rows,property=page"`
}
type FixtureHolder struct {
	Contract xdatly.Component[fixtureInput, fixtureOutput] `component:"Rows,path=/rows,method=GET"`
}

func (FixtureHolder) DatlyQuerySelectors() fixtureSelectors {
	panic("declaration method must never run")
}

const sourceFixture = `package selectorcompanion
import "github.com/viant/xdatly"
type fixtureInput struct{ ID int }
type fixtureOutput struct{ Data []int }
type fixtureSelectors struct {
 Fields []string ` + "`parameter:\"Fields,kind=query,in=fields,required=false\" querySelector:\"view=rows,property=fields\"`" + `
 Page int ` + "`parameter:\"Page,kind=query,in=page,required=false\" querySelector:\"view=rows,property=page\"`" + `
}
type FixtureHolder struct {
 Contract xdatly.Component[fixtureInput,fixtureOutput] ` + "`component:\"Rows,path=/rows,method=GET\"`" + `
}
func (FixtureHolder) DatlyQuerySelectors() fixtureSelectors { panic("must never run") }
`

// Use the native x source loader, not fabricated catalog entries or parsed
// method bodies. The same loader's declarations feed existing type catalogs.
func loadedHolder(t *testing.T, source string) (*x.Type, xshape.Lookup) {
	t.Helper()
	fsys := fstest.MapFS{
		"root/go.mod": {Data: []byte("module github.com/viant/datly\n")},
		"root/internal/selectorcompanion/holder.go": {Data: []byte(source)},
	}
	pkg, err := loaderast.LoadPackageFS(context.Background(), fsys, "root/internal/selectorcompanion")
	if err != nil {
		t.Fatal(err)
	}
	lookup := func(identity string) (*x.Type, error) {
		for _, typ := range pkg.Types {
			if pkg.PkgPath+"."+typ.Name == identity {
				return &x.Type{Name: typ.Name, PkgPath: pkg.PkgPath, SynteticType: typ}, nil
			}
		}
		return nil, nil
	}
	holder, err := lookup(pkg.PkgPath + ".FixtureHolder")
	if err != nil || holder == nil {
		t.Fatal("missing genuine source holder")
	}
	return holder, lookup
}

func TestCompanionSourceAndReflectionAgreeWithoutInvocation(t *testing.T) {
	holder, lookup := loadedHolder(t, sourceFixture)
	source, err := Resolve(holder, "Contract", lookup)
	if err != nil {
		t.Fatal(err)
	}
	linked, err := Resolve(x.NewType(reflect.TypeFor[FixtureHolder]()), "Contract", nil)
	if err != nil {
		t.Fatal(err)
	}
	if source.Holder != linked.Holder || source.Field != linked.Field || source.Descriptor.Key() != linked.Descriptor.Key() || len(source.Fields) != 2 || len(linked.Fields) != 2 {
		t.Fatal("source/reflected companion identity mismatch")
	}
	for i := range source.Fields {
		s, l := source.Fields[i], linked.Fields[i]
		sType, _ := s.BuiltinType()
		lType, _ := l.BuiltinType()
		if s.Name != l.Name || sType != lType || s.Tag != l.Tag {
			t.Fatal("selector declaration mismatch")
		}
	}
}

func TestCompanionInvalidSourceDeclarationsFailClosed(t *testing.T) {
	for _, tc := range []struct{ name, old, new string }{
		{"mutation owner", `component:"Rows,path=/rows,method=GET"`, `component:"Rows,path=/rows,method=POST" mutation:"insert"`},
		{"custom handler", "method=GET", "method=GET,handler=Custom"},
		{"argument", "DatlyQuerySelectors()", "DatlyQuerySelectors(_ int)"},
		{"variadic", "DatlyQuerySelectors()", "DatlyQuerySelectors(_ ...int)"},
		{"pointer result", "DatlyQuerySelectors() fixtureSelectors", "DatlyQuerySelectors() *fixtureSelectors"},
		{"multiple result", "DatlyQuerySelectors() fixtureSelectors", "DatlyQuerySelectors() (fixtureSelectors,error)"},
		{"exported result", "fixtureSelectors", "FixtureSelectors"},
		{"alias result", "type fixtureSelectors struct", "type fixtureSelectors = struct"},
		{"named underlying result", "type fixtureSelectors struct", "type namedSelectors struct{}; type fixtureSelectors namedSelectors; type unused struct"},
		{"multiple holder fields", "Contract xdatly.Component", "Other int; Contract xdatly.Component"},
		{"wrong component generic", "xdatly.Component[fixtureInput,fixtureOutput]", "fixtureInput"},
		{"promoted selector field", "Fields []string", "fixtureInput; Fields []string"},
		{"unsupported selector", "property=page", "property=criteria"},
		{"wrong field type", "Page int", "Page string"},
		{"nonquery source", "kind=query,in=fields", "kind=header,in=fields"},
		{"missing source name", "kind=query,in=fields", "kind=query"},
		{"codec responsibility", "querySelector:\"view=rows,property=fields\"", "querySelector:\"view=rows,property=fields\" codec:\"Other\""},
		{"scope responsibility", "kind=query,in=fields", "kind=query,in=fields,scope=late"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := strings.ReplaceAll(sourceFixture, tc.old, tc.new)
			holder, lookup := loadedHolder(t, source)
			if _, err := Resolve(holder, "Contract", lookup); err == nil {
				t.Fatal("invalid companion admitted")
			}
		})
	}
}

func TestMissingCompanionMethodPreservesOrdinaryHolder(t *testing.T) {
	holder, lookup := loadedHolder(t, strings.Replace(sourceFixture, "DatlyQuerySelectors()", "UnrelatedMethod()", 1))
	if result, err := Resolve(holder, "Contract", lookup); err != nil || result != nil {
		t.Fatal("ordinary holder changed")
	}
}

func TestCompanionFivePropertiesAndSelectedRoute(t *testing.T) {
	source := strings.Replace(sourceFixture, "Contract xdatly.Component", `Other xdatly.Component[fixtureInput,fixtureOutput] `+"`component:\"Rows,path=/other,method=GET\"`"+`; Contract xdatly.Component`, 1)
	source = strings.Replace(source, "Page int", `Limit int `+"`parameter:\"Limit,kind=query,in=limit,required=false\" querySelector:\"view=rows,property=limit\"`"+`; Offset int `+"`parameter:\"Offset,kind=query,in=offset,required=false\" querySelector:\"view=rows,property=offset\"`"+`; Sort []string `+"`parameter:\"OrderBy,kind=query,in=sort,required=false\" querySelector:\"view=rows,property=orderBy\"`"+`; Page int`, 1)
	holder, lookup := loadedHolder(t, source)
	for _, field := range []string{"Contract", "Other"} {
		contract, err := Resolve(holder, field, lookup)
		if err != nil {
			t.Fatal(err)
		}
		if len(contract.Fields) != 5 {
			t.Fatal("lost selector fields")
		}
	}
}

type repeatedSelectorNames struct {
	RootFields  []string `parameter:"Fields,kind=query,in=root_fields" querySelector:"view=root,property=fields"`
	ChildFields []string `parameter:"Fields,kind=query,in=child_fields" querySelector:"view=children,property=fields"`
}

func TestCompanionCanonicalIdentityIncludesQuerySource(t *testing.T) {
	descriptor := x.NewType(reflect.TypeFor[repeatedSelectorNames]())
	fields, err := xshape.New(descriptor, nil).Fields()
	if err != nil {
		t.Fatal(err)
	}
	root := &spec.Parameter{Name: "Fields", TypeExpr: "[]string", Source: spec.BindSource{Kind: "query", Name: "root_fields"}, QuerySelector: &spec.QuerySelectorBinding{View: "root", Property: spec.SelectorPropertyFields}}
	child := &spec.Parameter{Name: "Fields", TypeExpr: "[]string", Source: spec.BindSource{Kind: "query", Name: "child_fields"}, QuerySelector: &spec.QuerySelectorBinding{View: "children", Property: spec.SelectorPropertyFields}}
	selected, err := selectCanonical(&spec.Component{Parameters: []*spec.Parameter{root, child}}, &Contract{Descriptor: descriptor, Fields: fields})
	if err != nil {
		t.Fatal(err)
	}
	if len(selected.Parameters) != 2 || selected.Parameters[0] != root || selected.Parameters[1] != child {
		t.Fatal("query source identity was collapsed")
	}
}

func TestCompanionPostAndMixedReaderRoutes(t *testing.T) {
	for _, source := range []string{
		strings.ReplaceAll(sourceFixture, "method=GET", "method=POST"),
		strings.Replace(sourceFixture, "Contract xdatly.Component", `Other xdatly.Component[fixtureInput,fixtureOutput] `+"`component:\"Rows,path=/other,method=POST\"`"+`; Contract xdatly.Component`, 1),
	} {
		holder, lookup := loadedHolder(t, source)
		if _, err := Resolve(holder, "Contract", lookup); err != nil {
			t.Fatal(err)
		}
	}
}
