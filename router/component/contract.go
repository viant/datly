package component

import (
	"github.com/viant/datly/service"
	"github.com/viant/datly/utils/formatter"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/toolbox/format"
)

type (
	//Style defines style
	Style string

	Contract struct {
		Name    string `json:",omitempty" yaml:",omitempty"`
		Input   Input
		Output  Output
		Service service.Type `json:",omitempty"`
	}

	Input struct {
		Type state.Type
		//TODO add explicit body type when applicable
		//BodyType state.Type
	}

	Output struct {
		Cardinality state.Cardinality    `json:",omitempty"`
		CaseFormat  formatter.CaseFormat `json:",omitempty"`
		OmitEmpty   bool                 `json:",omitempty"`
		Style       Style                `json:",omitempty"`

		//Filed defines optional main view data holder
		Field            string `json:",omitempty"`
		Exclude          []string
		NormalizeExclude *bool

		RevealMetric *bool
		DebugKind    view.MetaKind

		DataFormat string `json:",omitempty"` //default data format

		ResponseBody *BodySelector

		Type state.Type

		_caser    *format.Case
		_excluded map[string]bool
	}

	// BodySelector deprecated,  use output parameter instead
	//deprecated
	BodySelector struct {
		StateValue string
	}
)

func (o *Output) Init() (err error) {
	if err = o.initCaser(); err != nil {
		return err
	}
	o.initExclude()
	return nil
}

func (r *Output) ShouldNormalizeExclude() bool {
	return r.NormalizeExclude == nil || *r.NormalizeExclude
}

func (o *Output) initExclude() {
	o._excluded = map[string]bool{}
	for _, excluded := range o.Exclude {
		o._excluded[excluded] = true
	}

	if !o.ShouldNormalizeExclude() {
		return
	}
	aBool := false
	o.NormalizeExclude = &aBool
	for i, excluded := range o.Exclude {
		o.Exclude[i] = formatter.NormalizePath(excluded)
	}

}

func (o *Output) initCaser() error {
	if o._caser != nil {
		return nil
	}

	if o.CaseFormat == "" {
		o.CaseFormat = formatter.UpperCamel
	}

	var err error
	caser, err := o.CaseFormat.Caser()
	if err != nil {
		return err
	}
	o._caser = &caser
	return nil
}

func (o *Output) Excluded() map[string]bool {
	return o._excluded
}

func (o *Output) FormatCase() *format.Case {
	return o._caser
}

const (
	BasicStyle         Style = "Basic"
	ComprehensiveStyle Style = "Comprehensive"
)
