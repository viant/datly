package parser

type iterables struct {
	*expressionMatcher
	stack Expressions
	Index int
}

func (m *iterables) Has() bool {
	return m.Index < len(m.expressionMatcher.expressions) || len(m.stack) > 0
}

func (m *iterables) Next() *Expression {
	expression, has := m.pop()
	if has {
		return expression
	}
	expression = m.expressions[m.Index]
	m.Index++
	return expression
}

func (m *iterables) Push(expression *Expression) {
	m.stack = append(m.stack, expression)
}

func (m *iterables) Pop() (*Expression, bool) {
	expression, has := m.pop()
	if has {
		return expression, has
	}
	has = m.Has()
	if has {
		return m.Next(), has
	}
	return nil, false
}

func (m *iterables) pop() (*Expression, bool) {
	if len(m.stack) > 0 {
		actual := m.stack[0]
		m.stack = m.stack[1:]
		return actual, true
	}
	return nil, false
}

func (t *Template) iterable() *iterables {
	result := &expressionMatcher{Template: t, occurrences: map[string]int{}}
	result.init()
	return &iterables{expressionMatcher: result}
}
