package marshal

import (
	"context"
	"github.com/francoispqt/gojay"
	"github.com/viant/afs"
	"github.com/viant/datly/httputils"
	"github.com/viant/datly/router/marshal/json"
	"github.com/viant/datly/template/expand"
	"github.com/viant/datly/utils/types"
	"github.com/viant/xreflect"
	"net/http"
	"net/url"
)

const (
	TransformKindUnmarshal = "Unmarshal"
	TransformKindMarshal   = "Marshal"
)

type (
	Transform struct {
		ParamName    string `json:",omitempty" yaml:",omitempty"`
		Kind         string `json:",omitempty" yaml:",omitempty"`
		Path         string `json:",omitempty" yaml:",omitempty"`
		Codec        string `json:",omitempty" yaml:",omitempty"`
		Source       string `json:",omitempty" yaml:",omitempty"`
		SourceURL    string `json:",omitempty" yaml:",omitempty"`
		Transformer  string `json:",omitempty" yaml:",omitempty"`
		_evaluator   *expand.Evaluator
		_unmarshaler json.UnmarshalerInto
	}
)

type Transforms []*Transform
type TransformIndex map[string]Transforms

func (t *Transform) Init(ctx context.Context, fs afs.Service, lookupType xreflect.LookupType) error {
	if t.SourceURL != "" {
		source, err := fs.DownloadWithURL(ctx, t.SourceURL)
		if err != nil {
			return err
		}

		t.Source = string(source)
	}

	if t.Source != "" {
		var err error
		t._evaluator, err = expand.NewEvaluator(t.Source, expand.WithTypeLookup(lookupType), expand.WithCustomContexts(t.newCtx(CustomContext{})))
		if err != nil {
			return err
		}
	}

	if t.Transformer != "" {
		rType, err := types.LookupType(lookupType, t.Transformer)
		if err != nil {
			return err
		}
		value := types.NewValue(rType)

		unmarshaler, ok := value.(json.UnmarshalerInto)
		if ok {
			t._unmarshaler = unmarshaler
		}

		t.Kind = TransformKindUnmarshal
	}

	return nil
}

func (t *Transform) Evaluate(cookies map[string]*http.Cookie, pathVariables map[string]string, queryParams url.Values, headers http.Header, decoder *gojay.Decoder, fn xreflect.LookupType) (*State, error) {
	d := &Decoder{
		typeLookup: fn,
		decoder:    decoder,
	}
	request := httputils.NewRequest(cookies, pathVariables, queryParams, headers)

	ctx := CustomContext{
		Decoder: d,
		Request: request,
	}

	evaluate, err := t._evaluator.Evaluate(nil, expand.WithCustomContext(t.newCtx(ctx)))
	return &State{
		Ctx:         ctx,
		ExpandState: evaluate,
	}, err
}

func (t *Transform) newCtx(ctx CustomContext) *expand.CustomContext {
	result := &expand.CustomContext{
		Type:  ctxType,
		Value: ctx,
	}

	return result
}

func (t *Transform) UnmarshalerInto() json.UnmarshalerInto {
	return t._unmarshaler
}

func (t Transforms) Index() map[string]*Transform {
	var result = map[string]*Transform{}
	for i, item := range t {
		result[item.Path] = t[i]
	}
	return result
}
