package generic

import (
	"github.com/pkg/errors"
	"github.com/viant/toolbox"
)

//CaseFormat defines case format map
var CaseFormat = map[string]int{
	CaseUpper:           toolbox.CaseUpper,
	CaseLower:           toolbox.CaseLower,
	CaseUpperCamel:      toolbox.CaseUpperCamel,
	CaseLowerCamel:      toolbox.CaseLowerCamel,
	CaseUpperUnderscore: toolbox.CaseUpperUnderscore,
	CaseLowerUnderscore: toolbox.CaseLowerUnderscore,
}

//ValidateCaseFormat checks if case format is valid
func ValidateCaseFormat(caseFormat string) error {
	_, ok := CaseFormat[caseFormat]
	if ok {
		return nil
	}
	supported := []string{}
	for valid := range CaseFormat {
		supported = append(supported, valid)
	}
	return errors.Errorf("unsupported case format: '%v', supported: %v", caseFormat, supported)
}
