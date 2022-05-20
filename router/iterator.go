package router

import "fmt"

const (
	ValuesSeparator = ','
)

type (
	SelectorParamIt struct {
		paramValue string
		start      int

		next              Param
		updatedViewPrefix bool
	}

	Param struct {
		Prefix string
		Value  string
	}
)

func NewParamIt(value string) *SelectorParamIt {
	return &SelectorParamIt{
		paramValue: value,
	}
}

func (s *SelectorParamIt) Has() bool {
	return s.start < len(s.paramValue)
}

func (s *SelectorParamIt) Next() (Param, error) {
	s.resetNext()
	if s.start != 0 {
		if s.paramValue[s.start] != ValuesSeparator {
			return s.next, fmt.Errorf("expected at %v position value separator (%v) but got %v", s.start, string(ValuesSeparator), string(s.paramValue[s.start]))
		}
		s.start++
	}

	for i := s.start; i < len(s.paramValue); i++ {
		switch s.paramValue[i] {
		case ValuesSeparator:
			s.next.Value = s.sliceParamValue(s.start, i, s.updatedViewPrefix)
			if i == 0 {
				s.start = 1
			} else {
				s.start = i
			}

			return s.next, nil
		case '(':
			end := s.exprBlockEnd(i + 1)
			if end == -1 {
				return Param{}, fmt.Errorf(`value "%v" contains unclosed expressions`, s.paramValue[i:])
			}
			s.next.Value = s.sliceParamValue(i, end, true)
			s.start = end + 1
			return s.next, nil
		}
	}

	s.next.Value = s.sliceParamValue(s.start, len(s.paramValue), s.updatedViewPrefix)
	s.start = len(s.paramValue)

	return s.next, nil
}

func (s *SelectorParamIt) sliceParamValue(start, end int, skipFirst bool) string {
	if !skipFirst {
		return s.paramValue[start:end]
	}

	if start+1 >= end {
		return ""
	}

	return s.paramValue[start+1 : end]
}

func (s *SelectorParamIt) resetNext() {
	s.next.Value = ""
	s.next.Prefix = ""
	s.updatedViewPrefix = false
}

func (s *SelectorParamIt) exprBlockEnd(start int) int {
	depth := 1
	for i := start; i < len(s.paramValue); i++ {
		switch s.paramValue[i] {
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
