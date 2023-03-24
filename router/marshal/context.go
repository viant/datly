package marshal

import (
	"encoding/json"
	"github.com/francoispqt/gojay"
	"github.com/viant/xreflect"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
)

func init() {

}

type (
	CustomContext struct {
		Decoder *Decoder `velty:"names=decoder"`
		Request *Request `velty:"names=request"`
	}

	Decoder struct {
		Decoded    interface{}           `velty:"-"`
		typeLookup xreflect.TypeLookupFn `velty:"-"`
		decoder    *gojay.Decoder        `velty:"-"`
	}

	Request struct {
		Url           string            `velty:"-"`
		QueryParams   url.Values        `velty:"-"`
		PathVariables map[string]string `velty:"-"`
		Headers       http.Header       `velty:"-"`
		cookies       map[string]*http.Cookie
	}
)

func (d *Decoder) UnmarshallInto(typeName string, unescape bool) (string, error) {
	resultType, err := d.typeLookup("", "", typeName)
	if err != nil {
		return "", err
	}

	rValue := reflect.New(resultType)

	var jsonBody gojay.EmbeddedJSON
	if err = d.decoder.EmbeddedJSON(&jsonBody); err != nil {
		return "", err
	}

	if unescape {
		unquote, _ := strconv.Unquote(string(jsonBody))
		jsonBody = []byte(unquote)
	}

	if err = json.Unmarshal(jsonBody, rValue.Interface()); err != nil {
		return "", err
	}

	d.Decoded = rValue.Elem().Interface()
	return "", err
}

func (r *Request) QueryParam(name string) string {
	return r.QueryParams.Get(name)
}

func (r *Request) HasQuery(name string) bool {
	return r.QueryParams.Has(name)
}

func (r *Request) PathVariable(name string) string {
	return r.PathVariables[name]
}

func (r *Request) HasPathVariable(name string) bool {
	_, ok := r.PathVariables[name]
	return ok
}
func (r *Request) Cookie(name string) *http.Cookie {
	return r.cookies[name]
}

func (r *Request) HasCookie(name string) bool {
	_, ok := r.cookies[name]
	return ok
}
