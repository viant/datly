package locator

import (
	"fmt"
	"github.com/viant/datly/view/state"
	"net/http"
)

type Cookie struct {
	cookies []*http.Cookie
}

func (v *Cookie) Names() []string {
	var result = make([]string, 0)
	for _, cookie := range v.cookies {
		result = append(result, cookie.Name)
	}
	return result
}

func (v *Cookie) Value(name string) (interface{}, bool, error) {
	for _, cookie := range v.cookies {
		if cookie.Name == name {
			return cookie.Value, true, nil
		}
	}
	return nil, false, nil
}

// NewCookie returns cookie locator
func NewCookie(opts ...Option) (state.Locator, error) {
	options := NewOptions(opts)
	if options.Request == nil {
		return nil, fmt.Errorf("request was empty")
	}
	ret := &Cookie{cookies: options.Request.Cookies()}
	return ret, nil
}
