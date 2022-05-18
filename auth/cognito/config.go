package cognito

import (
	"github.com/viant/scy/auth/cognito"
)

//Config represents a config
type Config struct {
	cognito.Config
	AuthCookie string
	SignInURL  string
}
