package options

import (
	"strconv"
	"strings"
)

type (
	Auth struct {
		HMAC     string     `short:"A" long:"jwtHMAC" description:"HMACKeyPath|EncKey" `
		RSA      string     `short:"J" long:"jwtRSA" description:"PublicKeyPath|EncKey" `
		Firebase string     `short:"F" long:"fsecret" description:"Firebase secrets" `
		Custom   CustomAuth `short:"E" long:"customAuth" description:"Custom AuthSQL" `
	}
)

type CustomAuth string

func (c *CustomAuth) Size() int {
	if c == nil {
		return 0
	}
	return len(string(*c))
}

func (c *CustomAuth) ShiftString() string {
	if c == nil {
		return ""
	}
	parts := strings.Split(string(*c), "|")
	if len(parts) == 0 {
		return ""
	}
	*c = CustomAuth(strings.Join(parts[1:], "|"))
	return parts[0]
}

func (c *CustomAuth) ShiftInt() (int, error) {
	value := c.ShiftString()
	if value == "" {
		return 0, nil
	}
	return strconv.Atoi(value)
}

func (v *Auth) Init() {
	v.HMAC = ensureAbsPath(v.HMAC)
	v.RSA = ensureAbsPath(v.RSA)
}
