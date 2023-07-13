package setter

import "github.com/viant/datly/utils/formatter"

func SetStringIfEmpty(dest *string, src string) {
	if dest != nil && *dest == "" {
		*dest = src
	}
}

func SetCaseFormatIfEmpty(dest *formatter.CaseFormat, src formatter.CaseFormat) {
	if dest != nil && *dest == "" {
		*dest = src
	}
}

func SetBoolIfFalse(dest *bool, src bool) {
	if dest != nil && !*dest {
		*dest = src
	}
}

func SetIntIfZero(dest *int, src int) {
	if dest != nil && *dest == 0 {
		*dest = src
	}
}
