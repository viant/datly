package ast

import "strings"

type (
	Scope struct {
		Variables map[string]*Variable
		Parent    *Scope
	}

	Variable struct {
		Name string
	}
)

func NewScope() *Scope {
	return &Scope{
		Variables: map[string]*Variable{},
	}
}

func (s *Scope) NextScope() *Scope {
	scope := NewScope()
	scope.Parent = s
	return scope
}

func (s *Scope) DeclareVariable(variable string) {
	split := strings.Split(variable, ".")
	if len(split) > 0 {
		variable = split[0]
	}

	if s.Variables[variable] != nil {
		return
	}

	s.Variables[variable] = &Variable{
		Name: variable,
	}
}

func (s *Scope) IsDeclared(variable string) bool {
	tmp := s
	for tmp != nil {
		if _, ok := tmp.Variables[variable]; ok {
			return true
		}

		tmp = tmp.Parent
	}

	return false
}
