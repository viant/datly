package setter

import "github.com/viant/structology/format/text"

func SetStringIfEmpty(dest *string, src string) {
	if dest != nil && *dest == "" {
		*dest = src
	}
}

func SetCaseFormatIfEmpty(dest *text.CaseFormat, src text.CaseFormat) {
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

func SetIntIfNil(dest **int, src int) {
	if dest != nil && *dest == nil {
		*dest = &src
	}
}
