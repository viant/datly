package view

import (
	"fmt"
	"github.com/viant/toolbox/format"
	"strings"
)

type CaseFormat string

const (
	UpperUnderscoreShort CaseFormat = "uu"
	UpperUnderscore      CaseFormat = "upperunderscore"
	LowerUnderscoreShort CaseFormat = "lu"
	LowerUnderscore      CaseFormat = "lowerunderscore"
	UpperCamelShort      CaseFormat = "uc"
	UpperCamel           CaseFormat = "uppercamel"
	LowerCamelShort      CaseFormat = "lc"
	LowerCamel           CaseFormat = "lowercamel"
	LowerShort           CaseFormat = "l"
	Lower                CaseFormat = "lower"
	UpperShort           CaseFormat = "u"
	Upper                CaseFormat = "upper"
)

func (f *CaseFormat) Init() error {
	switch *f {
	case UpperUnderscoreShort, UpperUnderscore,
		LowerUnderscoreShort, LowerUnderscore,
		UpperCamelShort, UpperCamel,
		LowerCamelShort, LowerCamel,
		LowerShort, Lower,
		UpperShort, Upper:
		return nil
	case "":
		*f = LowerUnderscoreShort
		return nil
	}

	return fmt.Errorf("unsupported Format: %v", *f)
}

func (f CaseFormat) Caser() (format.Case, error) {
	return format.NewCase(string(f))
}

//DetectCase detect case format
func DetectCase(data ...string) string {
	if len(data) == 0 {
		return "lc"
	}

	result := ""

	hasUnderscore := false

outer:
	for _, datum := range data {
		for _, aByte := range datum {
			if aByte >= 'A' && aByte <= 'Z' {
				result += "u"
			}

			if aByte >= 'a' && aByte <= 'z' {
				result += "l"
			}

			if len(result) > 0 {
				break outer
			}
		}
	}

	for _, datum := range data {
		hasUnderscore = hasUnderscore || strings.Contains(datum, "_")
		if hasUnderscore {
			break
		}
	}

	if hasUnderscore {
		result += "u"
	} else {
		result += "c"
	}

	return result
}
