package cmd

type Stack struct {
	items []*stmtBuilder
}

func NewStack(items ...*stmtBuilder) *Stack {
	aStack := &Stack{}
	aStack.Push(items...)
	return aStack
}

func (s *Stack) Pop() *stmtBuilder {
	i := s.items[len(s.items)-1]
	s.items = s.items[:len(s.items)-1]
	return i
}

func (s *Stack) Push(items ...*stmtBuilder) {
	s.items = append(s.items, items...)
}

func (s *Stack) Flush() {
	for len(s.items) > 0 {
		s.Pop().writeString("\n#end")
	}
}
