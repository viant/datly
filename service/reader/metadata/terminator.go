package metadata

import (
	"github.com/viant/parsly"
)

type wordTerminator struct {
	terminatedBy []byte
	inclusive    bool
}

func (t *wordTerminator) Match(cursor *parsly.Cursor) (matched int) {
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

	return matched
}

func newTerminatorAny(inclusive bool, terminatedBy []byte) *wordTerminator {
	return &wordTerminator{
		terminatedBy: terminatedBy,
		inclusive:    inclusive,
	}
}
