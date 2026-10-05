package transcribe

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	dsql "github.com/viant/datly/sql"
	sqltemplate "github.com/viant/datly/sql/template"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	smodel "github.com/viant/x/syntetic/model"
)

type analysisClaims struct {
	UserID  int
	Account *analysisAccount
	Dynamic any
}

type analysisAccount struct {
	ID int
}

func valueAnalysisSource(t *testing.T, SQL, authority string) *Source {
	t.Helper()
	source := embeddedAnalysisSource(t, SQL, "total")
	source.Text = `#import('jwt', 'example.com/auth')
#set($_ = $Jwt<string,*jwt.Claims>(header/Authorization).WithCodec('JwtClaim').Required().WithStatusCode(401))
#set($_ = $Category<string>(form/category).Optional().WithPredicate(0, 'equal', 'e', 'category'))
` + source.Text
	if authority == "unknown" {
		return source
	}
	source.Types = typecatalog.NewCatalog()
	if authority == "linked" {
		require.NoError(t, source.Types.Register(typecatalog.TypeOriginPackage,
			x.NewType(reflect.TypeFor[analysisClaims](), x.WithPkgPath("example.com/auth"), x.WithName("Claims"))))
		return source
	}
	file, err := parser.ParseFile(token.NewFileSet(), "claims.go", `package auth
type Claims struct { UserID int; Account *Account; Dynamic any }
type Account struct { ID int }`, 0)
	require.NoError(t, err)
	pack := &smodel.Package{Name: "auth", PkgPath: "example.com/auth"}
	for _, decl := range file.Decls {
		for _, entry := range decl.(*ast.GenDecl).Specs {
			typ := entry.(*ast.TypeSpec)
			pack.Types = append(pack.Types, &smodel.Type{Name: typ.Name.Name, PkgPath: pack.PkgPath, TypeSpec: typ})
		}
	}
	require.NoError(t, source.Types.RegisterPackage(typecatalog.TypeOriginPackage, pack))
	return source
}

func TestCompileEmbeddedInputValueAnalysis(t *testing.T) {
	base := `SELECT e.category, e.total FROM events e WHERE e.recommended_by_user_id = ${Jwt.UserID}`
	predicate := `${predicate.Builder().CombineOr($predicate.FilterGroup(0, "AND")).Build("AND")}`
	for _, authority := range []string{"linked", "source", "unknown"} {
		for _, tc := range []struct{ name, SQL string }{
			{"comparison", base},
			{"predicate", base + "\n" + predicate},
			{"member path", strings.ReplaceAll(base, "Jwt.UserID", "Jwt.Account.ID")},
			{"dynamic value", strings.ReplaceAll(base, "Jwt.UserID", "Jwt.Dynamic.UserID")},
			{"CTE", "WITH scoped AS (" + base + "\n" + predicate + ") SELECT e.category, e.total FROM scoped e"},
			{"derived", "SELECT e.category, e.total FROM (" + base + "\n" + predicate + ") e"},
			{"join", `SELECT e.category, e.total FROM events e JOIN users u ON u.id=${Jwt.UserID}`},
			{"having", `SELECT e.category, COUNT(*) AS total FROM events e GROUP BY e.category HAVING COUNT(*) > ${Jwt.UserID}`},
			{"qualify", `SELECT e.category, e.total FROM events e QUALIFY ROW_NUMBER() OVER (PARTITION BY e.category ORDER BY e.total) = ${Jwt.UserID}`},
			{"scalar input", `SELECT e.category, e.total FROM events e WHERE e.category=${Category}`},
			{"tuple", `SELECT e.category, e.total FROM events e WHERE e.id IN (${Jwt.UserID}, ${Jwt.Account.ID})`},
			{"fixed output expression", `SELECT e.category, e.total + ${Jwt.UserID} AS total FROM events e`},
		} {
			t.Run(authority+"/"+tc.name, func(t *testing.T) {
				source := valueAnalysisSource(t, tc.SQL, authority)
				result, err := NewCompiler().Compile(context.Background(), source)
				require.NoError(t, err)
				before := result.Component.Clone()
				require.NoError(t, column.New(nil).ValidateSourceProjections(result.Component, source.Resources, result.TypeResolver))
				require.Equal(t, before, result.Component)
				resolved := result.Component.RootView.Source.Clone()
				require.NoError(t, dsql.ResolveSource("events", resolved, source.Resources))
				require.Contains(t, resolved.SQL, tc.SQL)
				require.NotContains(t, resolved.SQL, "__datly_analysis_")
				stored, err := source.Resources.ReadFile("sql/events.sql")
				require.NoError(t, err)
				require.Equal(t, tc.SQL, string(stored))
				require.Equal(t, before.RootView.Source.Embeds, result.Component.RootView.Source.Embeds)
			})
		}
	}
}

func TestCompileInputValueAnalysisPreservesRuntimeBinding(t *testing.T) {
	SQL := `SELECT e.category, e.total FROM events e WHERE e.recommended_by_user_id = ${Jwt.UserID}`
	source := valueAnalysisSource(t, SQL, "linked")
	result, err := NewCompiler().Compile(context.Background(), source)
	require.NoError(t, err)
	resolved := result.Component.RootView.Source.Clone()
	require.NoError(t, dsql.ResolveSource("events", resolved, source.Resources))
	type input struct{ Jwt *analysisClaims }
	program, err := (sqltemplate.Compiler{
		Source: resolved.SQL, InputType: reflect.TypeFor[input](),
		Variables: []sqltemplate.Variable{{Name: "Jwt", FieldIndex: []int{0}}},
	}).Compile()
	require.NoError(t, err)
	for _, userID := range []int{0, 42} {
		actual, err := program.Evaluate(context.Background(), sqltemplate.Invocation{
			Input: reflect.ValueOf(input{Jwt: &analysisClaims{UserID: userID}}),
		})
		require.NoError(t, err)
		require.Contains(t, actual.SQL, "e.recommended_by_user_id = ?")
		require.Equal(t, []any{userID}, actual.Args)
		require.NotContains(t, actual.SQL, "__datly_analysis_")
	}
}

func TestCompileEmbeddedInputValueAnalysisRejectsInvalidReferences(t *testing.T) {
	for _, authority := range []string{"linked", "source"} {
		for _, tc := range []struct{ name, SQL, error string }{
			{"undeclared", `SELECT e.total FROM events e WHERE e.id=${Other.UserID}`, "undeclared input"},
			{"missing member", `SELECT e.total FROM events e WHERE e.id=${Jwt.Missing}`, "member Missing"},
			{"missing nested member", `SELECT e.total FROM events e WHERE e.id=${Jwt.Account.Missing}`, "member Missing"},
			{"scalar member", `SELECT e.total FROM events e WHERE e.id=${Jwt.UserID.Missing}`, "struct type int"},
			{"codec input is not output", `SELECT e.total FROM events e WHERE e.id=${Category.UserID}`, "struct type string"},
			{"method", `SELECT e.total FROM events e WHERE e.id=${Jwt.Lookup()}`, "only member access"},
			{"table", `SELECT e.total FROM ${Jwt.UserID} e`, ""},
			{"joined table", `SELECT e.total FROM events e JOIN ${Jwt.UserID} u ON e.id=u.id`, ""},
			{"projection", `SELECT ${Jwt.UserID} FROM events e`, ""},
			{"column identity", `SELECT e.${Jwt.UserID} AS total FROM events e`, ""},
			{"output identity", `SELECT e.id AS ${Jwt.UserID} FROM events e`, ""},
			{"order identity", `SELECT e.total FROM events e ORDER BY ${Jwt.UserID}`, ""},
			{"clause", `SELECT e.total FROM events e ${Jwt.UserID}`, ""},
			{"unsafe", `SELECT e.total FROM events e WHERE e.id=${Unsafe.Jwt.UserID}`, "unsupported dynamic SQL structure"},
			{"malformed", `SELECT e.total FROM events e WHERE e.id=${Jwt.UserID} GROUP BY )`, ""},
		} {
			t.Run(authority+"/"+tc.name, func(t *testing.T) {
				source := valueAnalysisSource(t, tc.SQL, authority)
				_, err := NewCompiler().Compile(context.Background(), source)
				require.Error(t, err)
				if tc.error != "" {
					require.ErrorContains(t, err, tc.error)
				}
				stored, err := source.Resources.ReadFile("sql/events.sql")
				require.NoError(t, err)
				require.Equal(t, tc.SQL, string(stored))
			})
		}
	}
}
