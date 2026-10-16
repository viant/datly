package selectorcompanion

import (
	"context"
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
		{"nonreader", "method=GET", "method=POST"},
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
		{"unsupported selector", "property=page", "property=limit"},
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
