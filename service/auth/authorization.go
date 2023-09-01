package auth

import (
	"github.com/viant/scy/auth"
	"strings"
)

type Authorization struct {
	Type     string
	Raw      string
	RawToken string
	OAuth    auth.Token
}

func NewAuthorization(raw string) *Authorization {
	result := &Authorization{
		Raw: raw,
	}
	if index := strings.Index(raw, " "); index != -1 {
		result.Type = raw[:index]
		result.RawToken = raw[index+1:]
	}
	return result
}
