package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/view/keywords"
	"github.com/viant/velty/functions"
)

func TestTemplate_Sanitize(t *testing.T) {
	state := inference.State{}
	tmpl, err := NewTemplate("#set($x = 1) SELECT * FROM t WHERE id = $x AND name = $Name", &state)
	require.NoError(t, err)
	actual := tmpl.Sanitize()
	assert.Contains(t, actual, "#set($x = 1)")
	assert.Contains(t, actual, "$criteria.AppendBinding($x)")
	assert.Contains(t, actual, "$criteria.AppendBinding($Unsafe.Name)")
}

func TestSanitize_SkipsFirstSetVariableOccurrence(t *testing.T) {
	iter := newIterable(map[string]bool{"x": true})
	expr := &Expression{
		IsVariable:      true,
		OccurrenceIndex: 0,
		Context:         SetContext,
		FullName:        "$x",
		Start:           0,
		End:             2,
	}
	dst := []byte("$x")
	actual, _ := sanitize(iter, expr, dst, 0, 0)
	assert.Equal(t, "$x", string(actual))
}

func TestUnwrapBrackets(t *testing.T) {
	raw, had := unwrapBrackets("${Foo}")
	assert.Equal(t, "$Foo", raw)
	assert.True(t, had)

	raw, had = unwrapBrackets("$Foo")
	assert.Equal(t, "$Foo", raw)
	assert.False(t, had)
}

func TestSanitizeContent(t *testing.T) {
	iter := newIterable(nil)
	expr := &Expression{Start: 0, End: 10}
	assert.Equal(t, "$A", sanitizeContent(iter, expr, "$A"))

	iter = newIterable(nil)
	parent := &Expression{Start: 0, End: 13, FullName: "$Fn($A, $B)"}
	argA := &Expression{Start: 4, End: 6, FullName: "$A", Holder: "A"}
	argB := &Expression{Start: 8, End: 10, FullName: "$B", Holder: "B"}
	next := &Expression{Start: 20, End: 22, FullName: "$C", Holder: "C"}
	iter.expressions = Expressions{argA, argB, next}
	actual := sanitizeContent(iter, parent, parent.FullName)
	assert.Equal(t, "$Fn($criteria.AppendBinding($Unsafe.A), $criteria.AppendBinding($Unsafe.B))", actual)
}

func TestSanitizeParameter(t *testing.T) {
	t.Run("standalone fn entry is preserved", func(t *testing.T) {
		name := "TestStandaloneSanitize"
		keywords.Add(name, functions.NewEntry(nil, &keywords.StandaloneFn{}))
		iter := newIterable(nil)
		expr := &Expression{Holder: name, FullName: "$" + name + "(1)"}
		assert.Equal(t, "$"+name+"(1)", sanitizeParameter(expr, "$"+name+"(1)", iter, nil, 0))
	})

	t.Run("set marker prefix preserved", func(t *testing.T) {
		iter := newIterable(nil)
		expr := &Expression{Holder: "Value", Prefix: keywords.SetMarkerKey}
		assert.Equal(t, "$Value", sanitizeParameter(expr, "$Value", iter, nil, 0))
	})

	t.Run("namespace metadata preserved", func(t *testing.T) {
		iter := newIterable(nil)
		expr := &Expression{
			Holder: "Any",
			Entry:  functions.NewEntry(nil, keywords.NewNamespace()),
		}
		assert.Equal(t, "$Any", sanitizeParameter(expr, "$Any", iter, nil, 0))
	})

	t.Run("const parameter gets Unsafe prefix", func(t *testing.T) {
		iter := newIterable(nil, inference.NewConstParameter("ConstX", 1))
		expr := &Expression{Holder: "ConstX"}
		assert.Equal(t, "$Unsafe.ConstX", sanitizeParameter(expr, "$ConstX", iter, nil, 0))
	})

	t.Run("func context with variable and Params prefix strips prefix", func(t *testing.T) {
		iter := newIterable(map[string]bool{"X": true})
		expr := &Expression{Holder: "X", Prefix: keywords.ParamsKey, Context: FuncContext}
		assert.Equal(t, "$X", sanitizeParameter(expr, "$Unsafe.X", iter, nil, 0))
	})

	t.Run("func context with non variable and empty prefix adds Unsafe", func(t *testing.T) {
		iter := newIterable(nil)
		expr := &Expression{Holder: "X", Prefix: "", Context: FuncContext}
		assert.Equal(t, "$Unsafe.X", sanitizeParameter(expr, "$X", iter, nil, 0))
	})

	t.Run("func context with variable and custom prefix keeps raw", func(t *testing.T) {
		iter := newIterable(map[string]bool{"X": true})
		expr := &Expression{Holder: "X", Prefix: keywords.AndPrefix, Context: ForEachContext}
		assert.Equal(t, "$X", sanitizeParameter(expr, "$X", iter, nil, 0))
	})

	t.Run("func context with non variable and non empty prefix keeps raw", func(t *testing.T) {
		iter := newIterable(nil)
		expr := &Expression{Holder: "X", Prefix: keywords.OrPrefix, Context: SetContext}
		assert.Equal(t, "$X", sanitizeParameter(expr, "$X", iter, nil, 0))
	})

	t.Run("func context with expression entry preserves raw", func(t *testing.T) {
		iter := newIterable(nil)
		expr := &Expression{Holder: "X", Context: IfContext, Entry: functions.NewEntry(nil, nil)}
		assert.Equal(t, "$X", sanitizeParameter(expr, "$X", iter, nil, 0))
	})

	t.Run("append context variable with Params prefix strips prefix", func(t *testing.T) {
		iter := newIterable(map[string]bool{"X": true})
		expr := &Expression{Holder: "X", Prefix: keywords.ParamsKey}
		assert.Equal(t, "$X", sanitizeParameter(expr, "$Unsafe.X", iter, nil, 0))
	})

	t.Run("append context variable placeholder", func(t *testing.T) {
		iter := newIterable(map[string]bool{"X": true})
		expr := &Expression{Holder: "X"}
		assert.Equal(t, "$criteria.AppendBinding($X)", sanitizeParameter(expr, "$X", iter, nil, 0))
	})

	t.Run("append context params prefix preserved", func(t *testing.T) {
		iter := newIterable(nil)
		expr := &Expression{Holder: "X", Prefix: keywords.ParamsKey}
		assert.Equal(t, "$Unsafe.X", sanitizeParameter(expr, "$Unsafe.X", iter, nil, 0))
	})

	t.Run("context metadata unexpand raw preserved", func(t *testing.T) {
		iter := newIterable(nil)
		expr := &Expression{
			Holder: "Ctx",
			Entry:  functions.NewEntry(nil, keywords.NewContextMetadata("ctx", nil, true)),
		}
		assert.Equal(t, "$Ctx", sanitizeParameter(expr, "$Ctx", iter, nil, 0))
	})

	t.Run("context metadata expandable becomes placeholder", func(t *testing.T) {
		iter := newIterable(nil)
		expr := &Expression{
			Holder: "Ctx",
			Entry:  functions.NewEntry(nil, keywords.NewContextMetadata("ctx", nil, false)),
		}
		assert.Equal(t, "$criteria.AppendBinding($Ctx)", sanitizeParameter(expr, "$Ctx", iter, nil, 0))
	})

	t.Run("non context metadata entry becomes placeholder", func(t *testing.T) {
		iter := newIterable(nil)
		expr := &Expression{
			Holder: "Ctx",
			Entry:  functions.NewEntry(nil, struct{}{}),
		}
		assert.Equal(t, "$criteria.AppendBinding($Ctx)", sanitizeParameter(expr, "$Ctx", iter, nil, 0))
	})

	t.Run("default path adds Unsafe and placeholder", func(t *testing.T) {
		iter := newIterable(nil)
		expr := &Expression{Holder: "X"}
		assert.Equal(t, "$criteria.AppendBinding($Unsafe.X)", sanitizeParameter(expr, "$X", iter, nil, 0))
	})
}

func TestSanitizeAsPlaceholder(t *testing.T) {
	assert.Equal(t, "$criteria.AppendBinding($X)", sanitizeAsPlaceholder("$X"))
}

func TestSanitize_WithBracketsWrapping(t *testing.T) {
	iter := newIterable(nil)
	expr := &Expression{
		FullName: "${X}",
		Holder:   "X",
		Start:    0,
		End:      4,
	}
	dst := []byte("${X}")
	actual, _ := sanitize(iter, expr, dst, 0, 0)
	assert.Equal(t, "${criteria.AppendBinding($Unsafe.X)}", string(actual))
}

func TestSanitize_NoChangePathAndCursorOffset(t *testing.T) {
	iter := newIterable(nil)
	expr := &Expression{
		FullName: "$Unsafe.X",
		Holder:   "X",
		Prefix:   keywords.ParamsKey,
		Start:    8,
		End:      17,
	}
	dst := []byte("SELECT " + expr.FullName)
	actual, offset := sanitize(iter, expr, dst, 0, 7)
	assert.Equal(t, "SELECT $Unsafe.X", string(actual))
	assert.Equal(t, 0, offset)
}

func newIterable(declared map[string]bool, params ...*inference.Parameter) *iterables {
	if declared == nil {
		declared = map[string]bool{}
	}
	state := inference.State{}
	for _, param := range params {
		if param != nil {
			state.Append(param)
		}
	}
	tmpl := &Template{
		Declared: declared,
		State:    &state,
	}
	return &iterables{expressionMatcher: &expressionMatcher{Template: tmpl}}
}
