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
	upperCases := 0
	loweCases := 0
	hasUnderscore := false

outer:
	for _, datum := range data {
		for _, aByte := range datum {

			if aByte >= 'A' && aByte <= 'Z' {
				if result == "" {
					result += "u"
				}
				upperCases++
			}

			if aByte >= 'a' && aByte <= 'z' {
				if result == "" {
					result += "l"
				}
				loweCases++
			}

			if aByte == '_' {
				hasUnderscore = true
			}

			if result != "" && (loweCases > 0 && upperCases > 0) || hasUnderscore {
				break outer
			}
		}
	}

	if hasUnderscore {
		result += "u"
	} else {
		suffix := "c"
		switch strings.ToLower(result) {
		case "u":
			if loweCases == 0 {
				suffix = "u"
			}
		case "l":
			if upperCases == 0 {
				suffix = "u"
			}
		}
		result += suffix
	}
	if result == "c" {
		result = "uc"
	}
	return result
}
