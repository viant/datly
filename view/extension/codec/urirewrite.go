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
	KeyURIRewrite = "URIRewrite"
)

type (
	URIRewriterFactory struct{}

	URIRewriter struct {
		Exclusion []string
	}
)

func (e *URIRewriterFactory) New(codecConfig *codec.Config, options ...codec.Option) (codec.Instance, error) {
	var exclusion []string
	if len(codecConfig.Args) > 0 {
		exclusion = strings.Split(codecConfig.Args[0], ",")
	}
	for i, _ := range exclusion {
		exclusion[i] = strings.Trim(exclusion[i], "'")
	}

	ret := &URIRewriter{
		Exclusion: exclusion,
	}
	return ret, nil
}

func (u *URIRewriter) ResultType(paramType reflect.Type) (reflect.Type, error) {
	return xreflect.StringType, nil
}

func (u *URIRewriter) Value(ctx context.Context, raw interface{}, options ...codec.Option) (interface{}, error) {
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
	for key, val := range values {
		if len(val) == 0 {
			values.Del(key)
			continue
		}

		empty := map[int]bool{}
		for i, s := range val {
			if s == "" {
				empty[i] = true
			}
		}

		if len(empty) != 0 {
			newVal := make([]string, len(val)-len(empty))
			z := 0
			for j, s := range val {
				if empty[j] {
					continue
				}

				newVal[z] = s
				z++
			}
			values[key] = newVal
		}
	}

	if err != nil {
		return raw, err
	}
	for _, exclusion := range u.Exclusion {
		values.Del(exclusion)
	}

	var keys []string
	for k, _ := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var buf strings.Builder
	for _, key := range keys {
		vs := values[key]
		sort.Strings(vs)
		for _, v := range vs {
			if buf.Len() > 0 {
				buf.WriteByte('&')
			}
			buf.WriteString(key)
			buf.WriteByte('=')
			buf.WriteString(v)
		}
	}
	return buf.String(), nil
}
