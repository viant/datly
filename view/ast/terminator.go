package ast

import (
	"github.com/viant/parsly"
)

type terminatorAny struct {
	terminatedBy []byte
	inclusive    bool
}

func (t *terminatorAny) Match(cursor *parsly.Cursor) (matched int) {
	hasMatch := false
outer:
	for _, c := range cursor.Input[cursor.Pos:] {
		matched++
		for _, terminator := range t.terminatedBy {
			if hasMatch = c == terminator; hasMatch {
				if !t.inclusive {
					matched--
				}
				break outer
			}
		}

	}
	if !hasMatch {
		return 0
	}
	return matched
}

func newTerminatorAny(inclusive bool, terminatedBy []byte) *terminatorAny {
	return &terminatorAny{
		terminatedBy: terminatedBy,
		inclusive:    inclusive,
	}
}
