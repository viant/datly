package state

import "fmt"

const (
	ValuesSeparator = ','
)

type (
	SelectorParamIt struct {
		queryParamValue string
		start           int

		next              Param
		updatedViewPrefix bool
		separator         int32
	}

	Param struct {
		Value string
	}
)

func NewParamIt(value string, separators ...int32) *SelectorParamIt {
	separator := ValuesSeparator
	if len(separators) > 0 {
		separator = separators[0]
	}

	return &SelectorParamIt{
		queryParamValue: value,
		separator:       separator,
	}
}

func (s *SelectorParamIt) Has() bool {
	return s.start < len(s.queryParamValue)
}

func (s *SelectorParamIt) Next() (Param, error) {
	s.resetNext()
	if s.queryParamValue[s.start] == '(' {
		end := s.exprBlockEnd(s.start + 1)
		if end == -1 {
			return Param{}, fmt.Errorf(`value "%v" contains unclosed expressions`, s.queryParamValue[s.start:])
		}

		s.next.Value = s.sliceParamValue(s.start, end, true)
		s.start = end + 2
		return s.next, nil
	}

	for i := s.start; i < len(s.queryParamValue); i++ {
		switch s.queryParamValue[i] {
		case byte(s.separator):
			s.next.Value = s.sliceParamValue(s.start, i, s.updatedViewPrefix)
			s.start = i + 1
			return s.next, nil
		}
	}

	s.next.Value = s.sliceParamValue(s.start, len(s.queryParamValue), s.updatedViewPrefix)
	s.start = len(s.queryParamValue)

	return s.next, nil
}

func (s *SelectorParamIt) sliceParamValue(start, end int, skipFirst bool) string {
	if !skipFirst {
		return s.queryParamValue[start:end]
	}

	if start+1 >= end {
		return ""
	}

	return s.queryParamValue[start+1 : end]
}

func (s *SelectorParamIt) resetNext() {
	s.next.Value = ""
	s.updatedViewPrefix = false
}

func (s *SelectorParamIt) exprBlockEnd(start int) int {
	depth := 1
	for i := start; i < len(s.queryParamValue); i++ {
		switch s.queryParamValue[i] {
		case ')':
			depth--
			if depth == 0 {
				return i
			}
		case '(':
			depth++
		}
	}

	return -1
}
