package marshal

import (
	"encoding/json"
	"github.com/francoispqt/gojay"
	"github.com/viant/datly/utils/httputils"
	"github.com/viant/xreflect"
	"reflect"
	"strconv"
)

func init() {

}

type (
	CustomContext struct {
		Decoder *Decoder           `velty:"names=decoder"`
		Request *httputils.Request `velty:"names=request"`
	}

	Decoder struct {
		Decoded    interface{}         `velty:"-"`
		typeLookup xreflect.LookupType `velty:"-"`
		decoder    *gojay.Decoder      `velty:"-"`
	}
)

func (d *Decoder) UnmarshalInto(typeName string, unescape bool) (string, error) {
	resultType, err := d.typeLookup(typeName)
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
