package codec

import (
	"context"
	"fmt"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xreflect"
	"net/url"
	"reflect"
	"sort"
	"strings"
)

const (
	KeyUrlRewriterRewriter = "UrlRewriter"
)

type (
	UrlRewriterFactory struct{}

	UrlRewriter struct {
		Exclusion []string
	}
)

func (e *UrlRewriterFactory) New(codecConfig *codec.Config, options ...codec.Option) (codec.Instance, error) {
	var exclusion []string
	if len(codecConfig.Args) > 0 {
		exclusion = strings.Split(codecConfig.Args[0], ",")
	}
	ret := &UrlRewriter{
		Exclusion: exclusion,
	}
	return ret, nil
}

func (u *UrlRewriter) ResultType(paramType reflect.Type) (reflect.Type, error) {
	return xreflect.StringType, nil
}

func (u *UrlRewriter) Value(ctx context.Context, raw interface{}, options ...codec.Option) (interface{}, error) {
	opts := codec.Options{}
	opts.Apply(options)

	value, ok := raw.(string)
	if !ok {
		return nil, fmt.Errorf("expected %T, but had: %T", value, raw)
	}
	if value == "" {
		return raw, nil
	}
	values, err := url.ParseQuery(value)
	if err != nil {
		return raw, err
	}
	for _, exclusion := range u.Exclusion {
		values.Del(exclusion)
	}
	var keys []string
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	builder := strings.Builder{}
	for i, k := range keys {
		if i > 0 {
			builder.WriteByte('&')
		}
		builder.WriteString(k)
		builder.WriteString("=")
		builder.WriteString(values.Get(k))
	}
	return builder.String(), nil
}
