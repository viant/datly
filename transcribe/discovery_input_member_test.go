package transcribe

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/viant/datly/spec"
	sqltemplate "github.com/viant/datly/sql/template"
	"github.com/viant/sqlx"
)

type nullableDiscoveryBounds struct {
	RecordedAt *time.Time
}

type nullableMemberDiscoveryInput struct {
	From   *time.Time               `parameter:"From,kind=form,in=from"`
	Bounds *nullableDiscoveryBounds `parameter:"Bounds,kind=view,in=Bounds"`
}

// The same compiler, template expansion, and SQL binder used by column
// discovery produce these arguments. Their native types must follow the
// declared fields without guessing from SQL or English error messages.
func TestDiscoveryInputNullableMemberNativeArguments(t *testing.T) {
	ctx := context.Background()
	value := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, tc := range []struct {
		name   string
		bounds *nullableDiscoveryBounds
	}{
		{"absent parent", nil},
		{"absent member", &nullableDiscoveryBounds{}},
		{"typed time", &nullableDiscoveryBounds{RecordedAt: &value}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			compiled, err := (&discoveryInputCompiler{
				component: &spec.Component{Parameters: []*spec.Parameter{
					{Name: "From", TypeExpr: "*time.Time", Source: spec.BindSource{Kind: "form", Name: "from"}},
					{Name: "Bounds", Source: spec.BindSource{Kind: "view", Name: "Bounds"}},
				}},
				source: &Source{PackageComponent: &spec.Component{}, LinkedInputType: reflect.TypeFor[nullableMemberDiscoveryInput]()},
			}).compile()
			if err != nil {
				t.Fatal(err)
			}
			if tc.bounds != nil {
				compiled.Value.FieldByName("Bounds").Set(reflect.ValueOf(tc.bounds))
			}
			program, err := (sqltemplate.Compiler{Source: "SELECT $From, $Bounds.RecordedAt", InputType: compiled.Value.Type(), Variables: compiled.Variables}).Compile()
			if err != nil {
				t.Fatal(err)
			}
			expanded, err := program.Evaluate(ctx, sqltemplate.Invocation{Input: compiled.Value})
			if err != nil {
				t.Fatal(err)
			}
			binder := sqlx.NewParameterBinder(compiled.ParameterResolver, expanded.Args...)
			_, args, err := binder.Bind(expanded.SQL)
			if err != nil {
				t.Fatal(err)
			}
			if err := binder.Complete(); err != nil {
				t.Fatal(err)
			}
			if len(args) != 2 {
				t.Fatalf("bound argument count=%d", len(args))
			}
			// Log types only; never SQL, data, a DSN, or native error text.
			t.Logf("bound argument types: top-level=%T nested=%T", args[0], args[1])
			for i, arg := range args {
				switch typed := arg.(type) {
				case time.Time:
					if i == 1 && tc.bounds != nil && tc.bounds.RecordedAt != nil && !typed.Equal(value) {
						t.Error("nested date value changed")
					}
				case *time.Time:
					if i == 1 && tc.bounds != nil && tc.bounds.RecordedAt != nil && (typed == nil || !typed.Equal(value)) {
						t.Error("nested date value changed")
					}
				default:
					t.Errorf("argument %d (%T) lost its declared nullable date type", i, arg)
				}
			}
			if !compiled.Value.FieldByName("From").IsNil() {
				t.Fatal("discovery materialized a runtime date default")
			}
			if tc.bounds == nil && !compiled.Value.FieldByName("Bounds").IsNil() {
				t.Fatal("discovery materialized an absent parent")
			}
			if tc.bounds != nil && tc.bounds.RecordedAt == nil && !compiled.Value.FieldByName("Bounds").Elem().FieldByName("RecordedAt").IsNil() {
				t.Fatal("discovery materialized an absent date member")
			}
		})
	}
}

func TestDiscoveryInputMemberMissingOrDeniedRemainsUnavailable(t *testing.T) {
	denied := errors.New("denied fixture selector")
	for _, tc := range []struct {
		name  string
		found bool
		err   error
	}{
		{"nonexistent member", false, nil}, {"denied member", false, denied},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resolver := discoveryParameterResolver(func(string) (any, bool, error) { return nil, tc.found, tc.err },
				map[string]reflect.Type{"bounds": reflect.TypeFor[*nullableDiscoveryBounds]()})
			value, found, err := resolver("Bounds.Unavailable")
			if value != nil || found || !errors.Is(err, tc.err) {
				t.Fatal("missing or denied member became available")
			}
		})
	}
}
