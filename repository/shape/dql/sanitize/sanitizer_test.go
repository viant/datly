package sanitize

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/inference"
	legacy "github.com/viant/datly/internal/translator/parser"
	vstate "github.com/viant/datly/view/state"
	"github.com/viant/velty"
	"github.com/viant/velty/ast/expr"
)

func TestSQL_ParityWithLegacySanitizer(t *testing.T) {
	testCases := []struct {
		name  string
		sql   string
		state inference.State
	}{
		{
			name: "unsafe binding from plain selector",
			sql:  "SELECT * FROM t WHERE id = $Id",
		},
		{
			name: "bracket selector placeholder",
			sql:  "SELECT * FROM t WHERE id = ${Id}",
		},
		{
			name: "declared variable in append context",
			sql:  "#set($x = 1)\nSELECT * FROM t WHERE id = $x",
		},
		{
			name: "const selector keeps raw unsafe prefix",
			sql:  "SELECT * FROM t WHERE id = $ConstId",
			state: inference.State{
				&inference.Parameter{Parameter: vstate.Parameter{Name: "ConstId", In: vstate.NewConstLocation("ConstId")}},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			state := testCase.state
			tpl, err := legacy.NewTemplate(testCase.sql, &state)
			require.NoError(t, err)
			expected := tpl.Sanitize()

			actual := SQL(testCase.sql, Options{
				Declared: tpl.Declared,
				Consts:   constNames(state),
			})
			assert.Equal(t, expected, actual)
		})
	}
}

func TestSQL_ParityWithLegacySanitizer_RuntimeExpansion(t *testing.T) {
	testCases := []struct {
		name  string
		sql   string
		state inference.State
	}{
		{
			name: "plain selector binding",
			sql:  "SELECT * FROM t WHERE id = $Id",
		},
		{
			name: "bracket selector binding",
			sql:  "SELECT * FROM t WHERE id = ${Id}",
		},
		{
			name: "declared variable binding",
			sql:  "#set($x = 7)\nSELECT * FROM t WHERE id = $x",
		},
		{
			name: "const raw unsafe",
			sql:  "SELECT * FROM t WHERE id = $ConstId",
			state: inference.State{
				&inference.Parameter{Parameter: vstate.Parameter{Name: "ConstId", In: vstate.NewConstLocation("ConstId")}},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			state := testCase.state
			tpl, err := legacy.NewTemplate(testCase.sql, &state)
			require.NoError(t, err)
			legacySQL := tpl.Sanitize()

			shapeSQL := SQL(testCase.sql, Options{
				Declared: tpl.Declared,
				Consts:   constNames(state),
			})
			require.Equal(t, legacySQL, shapeSQL)

			assert.Equal(t, renderVeltySQL(t, legacySQL), renderVeltySQL(t, shapeSQL))
		})
	}
}

func TestHolderName(t *testing.T) {
	assert.Equal(t, "Foo", holderName("$Foo"))
	assert.Equal(t, "Foo", holderName("${Foo}"))
	assert.Equal(t, "Foo", holderName("$Foo.Bar"))
	assert.Equal(t, "Foo", holderName("$Unsafe.Foo"))
	assert.Equal(t, "Foo", holderName("$Has.Foo"))
	assert.Equal(t, "Foo", holderName("$Unsafe.Foo.Bar"))
	assert.Equal(t, "", holderName("$Foo.Bar()"))
	assert.Equal(t, "", holderName("Foo"))
	assert.Equal(t, "", holderName(""))
}

func TestAddUnsafePrefixAndPlaceholder(t *testing.T) {
	assert.Equal(t, "$Unsafe.Foo", addUnsafePrefix("$Foo"))
	assert.Equal(t, "${Unsafe.Foo}", addUnsafePrefix("${Foo}"))
	assert.Equal(t, "$criteria.AppendBinding($Foo)", asPlaceholder("$Foo"))
	assert.Equal(t, "${criteria.AppendBinding($Foo)}", asPlaceholder("${Foo}"))
}

func TestSQL_EdgeBranches(t *testing.T) {
	assert.Equal(t, "", SQL("  ", Options{}))
	assert.Equal(t, "#if(", SQL("#if(", Options{}))
	assert.Equal(t, "#if(true)", SQL("#if(true)", Options{}))
	assert.Equal(t, "SELECT $Unsafe.Id", SQL("SELECT $Unsafe.Id", Options{}))
	assert.Equal(t, "SELECT $Has.Id", SQL("SELECT $Has.Id", Options{}))
	assert.Equal(t, "SELECT $Foo.Bar()", SQL("SELECT $Foo.Bar()", Options{}))
	assert.Equal(t, "#set($x = $y)\nSELECT $criteria.AppendBinding($Unsafe.y)", SQL("#set($x = $y)\nSELECT $y", Options{}))
}

func TestSQL_RewritePreservesLineCount(t *testing.T) {
	input := "#set($x = 1)\nSELECT *\nFROM t\nWHERE id = $Id\nAND name = ${Name}\n"
	out := SQL(input, Options{})
	assert.Equal(t, strings.Count(strings.TrimSpace(input), "\n"), strings.Count(out, "\n"))
}

func TestInSetDirective(t *testing.T) {
	adj := &bindingAdjuster{source: []byte("#set($x = $y)\nSELECT $z")}
	assert.True(t, adj.inSetDirective(7))
	assert.False(t, adj.inSetDirective(len(adj.source)))
	assert.False(t, adj.inSetDirective(-1))
}

func TestDeclared(t *testing.T) {
	declared := Declared("#set($x = 1)\n#set($y = $x)\nSELECT $x, $z")
	assert.True(t, declared["x"])
	assert.True(t, declared["y"])
	assert.False(t, declared["z"])
}

func TestDeclared_ParameterDeclarationStyle(t *testing.T) {
	declared := Declared("#set($_ = $Jwt<string>(header/Authorization))\nSELECT $Jwt.UserID")
	assert.True(t, declared["Jwt"])
}

func TestDeclaredListener_OnEventBranches(t *testing.T) {
	declared := map[string]bool{}
	l := &declaredListener{declared: declared}

	l.OnEvent(velty.Event{Type: velty.EventExitNode})
	l.OnEvent(velty.Event{Type: velty.EventEnterNode, ExprContext: velty.ExprContext{Kind: velty.CtxIfCond}})
	l.OnEvent(velty.Event{Type: velty.EventEnterNode, ExprContext: velty.ExprContext{Kind: velty.CtxSetLHS}, Node: &expr.Literal{Value: "x"}})
	l.OnEvent(velty.Event{Type: velty.EventEnterNode, ExprContext: velty.ExprContext{Kind: velty.CtxSetLHS}, Node: &expr.Select{ID: "x"}})

	assert.True(t, declared["x"])
}

func TestAdjust_Branches(t *testing.T) {
	adj := &bindingAdjuster{source: []byte("SELECT $Unsafe.Id")}

	// non selector node
	action, err := adj.Adjust(&expr.Literal{Value: "x"}, &velty.ParserContext{})
	require.NoError(t, err)
	assert.Equal(t, velty.ActionKeep, action.Kind)

	// selector without span
	sel := &expr.Select{FullName: "$Unsafe.Id", ID: "Unsafe"}
	action, err = adj.Adjust(sel, &velty.ParserContext{})
	require.NoError(t, err)
	assert.Equal(t, velty.ActionKeep, action.Kind)

	// selector in set lhs context
	ctx := &velty.ParserContext{}
	ctx.InitSource("", adj.source)
	ctx.SetSpan(sel, velty.Span{Start: 7, End: 16})
	ctx.PushExprContext(velty.ExprContext{Kind: velty.CtxSetLHS, ArgIdx: -1})
	action, err = adj.Adjust(sel, ctx)
	require.NoError(t, err)
	assert.Equal(t, velty.ActionKeep, action.Kind)

	// selector replacement equals raw
	ctx.PopExprContext()
	action, err = adj.Adjust(sel, ctx)
	require.NoError(t, err)
	assert.Equal(t, velty.ActionKeep, action.Kind)
}

func constNames(state inference.State) map[string]bool {
	ret := map[string]bool{}
	for _, param := range state {
		if param == nil || param.In == nil {
			continue
		}
		if param.In.Kind == vstate.KindConst {
			ret[param.Name] = true
		}
	}
	return ret
}

type criteriaMock struct{}

func (c criteriaMock) AppendBinding(value interface{}) string {
	return fmt.Sprintf("{%v}", value)
}

type unsafeMock struct {
	Id      int
	Name    string
	ConstId int
}

func renderVeltySQL(t *testing.T, template string) string {
	t.Helper()
	planner := velty.New()
	require.NoError(t, planner.DefineVariable("criteria", criteriaMock{}))
	require.NoError(t, planner.DefineVariable("Unsafe", unsafeMock{}))
	require.NoError(t, planner.DefineVariable("Id", 0))
	require.NoError(t, planner.DefineVariable("Name", ""))
	require.NoError(t, planner.DefineVariable("ConstId", 0))

	exec, newState, err := planner.Compile([]byte(template))
	require.NoError(t, err)
	state := newState()
	require.NoError(t, state.SetValue("criteria", criteriaMock{}))
	require.NoError(t, state.SetValue("Unsafe", unsafeMock{Id: 10, Name: "ann", ConstId: 77}))
	require.NoError(t, state.SetValue("Id", 10))
	require.NoError(t, state.SetValue("Name", "ann"))
	require.NoError(t, state.SetValue("ConstId", 77))
	require.NoError(t, exec.Exec(state))
	return state.Buffer.String()
}
