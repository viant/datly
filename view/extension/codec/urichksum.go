package codec

import (
	"context"
	"crypto/sha1"
	"fmt"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xreflect"
	"reflect"
	"strings"
)

const (
	KeyURIChecksum = "URIChecksum"
)

type (
	UriChecksumFactory struct{}

	UriChecksum struct {
		URIRewriter
		Checksum string
	}
)

func (e *UriChecksumFactory) New(codecConfig *codec.Config, options ...codec.Option) (codec.Instance, error) {
	var exclusion []string
	var checksum = ""

	if len(codecConfig.Args) > 0 {
		exclusion = strings.Split(codecConfig.Args[0], ",")
	}
	for i, _ := range exclusion {
		exclusion[i] = strings.Trim(exclusion[i], "'")
	}

	if len(codecConfig.Args) > 1 {
		checksum = codecConfig.Args[1]
	}

	ret := &UriChecksum{}
	ret.Exclusion = exclusion
	ret.Checksum = checksum
	return ret, nil
}

func (u *UriChecksum) ResultType(paramType reflect.Type) (reflect.Type, error) {
	return xreflect.StringType, nil
}

func (u *UriChecksum) Value(ctx context.Context, raw interface{}, options ...codec.Option) (interface{}, error) {
	opts := codec.Options{}
	opts.Apply(options)
	rawValue, err := u.URIRewriter.Value(ctx, raw, options...)
	if err != nil {
		return nil, err
	}
	uri, _ := rawValue.(string)
	var result string
	switch u.Checksum {
	case "sha1":
		result = fmt.Sprintf("%x", sha1.Sum([]byte(uri)))
	default:
		result = uri
	}
	return result, nil
}
