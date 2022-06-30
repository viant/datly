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
func DetectCase(text string) string {
	if len(text) == 0 {
		return "lc" ///default
	}
	result := ""
	if text[0:1] == strings.ToUpper(text[0:1]) {
		result = "u"
	} else {
		result = "l"
	}
	if strings.Contains(text, "_") {
		result += "u"
	} else if text[1:2] != strings.ToUpper(text[1:2]) {
		result += "c"
	} else {
		result += "u"
	}
	return result
}
