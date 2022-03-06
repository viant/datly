package data

import (
	"fmt"
	"github.com/viant/toolbox/format"
)

type CaseFormat string

const (
	UpperUnderscore CaseFormat = "uu"
	LowerUnderscore CaseFormat = "lu"
	UpperCamel      CaseFormat = "uc"
	LowerCamel      CaseFormat = "lc"
	Lower           CaseFormat = "l"
	Upper           CaseFormat = "u"
)

func (f *CaseFormat) Init() error {
	switch *f {
	case UpperUnderscore, LowerUnderscore, UpperCamel, LowerCamel, Lower, Upper:
		return nil
	case "":
		*f = LowerUnderscore
		return nil
	}

	return fmt.Errorf("unsupported Format: %v", *f)
}

func (f CaseFormat) Caser() (format.Case, error) {
	return format.NewCase(string(f))
}