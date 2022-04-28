package router

import "fmt"

type (
	UnsupportedFormat struct {
		paramName  string
		paramValue string
	}

	UnspecifiedPrefix struct {
		Prefix string
	}
)

func NewUnsupportedFormat(paramName, paramValue string) *UnsupportedFormat {
	return &UnsupportedFormat{
		paramName:  paramName,
		paramValue: paramValue,
	}
}

func (u *UnsupportedFormat) Error() string {
	return fmt.Sprintf("unsupported value %v of query param '%v' format, supported formats: [fieldName] || [prefix.FieldName]", u.paramValue, u.paramName)
}

func NewUnspecifiedPrefix(prefix string) *UnspecifiedPrefix {
	return &UnspecifiedPrefix{
		Prefix: prefix,
	}
}

func (u *UnspecifiedPrefix) Error() string {
	return "unspecified prefix " + u.Prefix
}
