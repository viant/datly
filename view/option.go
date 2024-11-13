package view

import (
	"embed"
	"github.com/viant/datly/gateway/router/marshal"
	"github.com/viant/datly/internal/setter"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/tags"
	"reflect"
)

type (
	//Option defines a view option
	Option func(v *View) error

	//Options defines a view options
	Options []Option

	//RelationOption defines relation options
	RelationOption func(r *Relation)
)

// Apply applies option
func (o Options) Apply(aView *View) error {
	if len(o) == 0 {
		return nil
	}
	for _, opt := range o {
		if err := opt(aView); err != nil {
			return err
		}
	}
	if aView.Selector == nil {
		aView.Selector = &Config{}
	}
	return nil
}

func WithTransforms(transforms marshal.Transforms) Option {
	return func(v *View) error {
		v._transforms = transforms
		return nil
	}
}

func WithColumns(columns Columns) Option {
	return func(v *View) error {
		v.Columns = columns
		return nil
	}
}

// WithFS creates fs options
func WithFS(fs *embed.FS) Option {
	return func(v *View) error {
		v._embedder = state.NewFSEmbedder(fs)
		if v.Schema != nil {
			v._embedder.SetType(v.Schema.Type())
		}
		return nil
	}
}

// WithFSEmbedder creates fs options
func WithFSEmbedder(embeder *state.FSEmbedder) Option {
	return func(v *View) error {
		v._embedder = embeder
		return nil
	}
}

// WithFS creates tag options
func WithTag(aTag *tags.Tag) Option {
	return func(v *View) error {
		opts, err := v.buildViewOptions(nil, aTag)
		if err != nil {
			return err
		}
		for _, opt := range opts {
			opt(v)
		}
		return nil
	}
}

// WithStructTag creates tag options
func WithStructTag(tag reflect.StructTag, fs *embed.FS) Option {
	return func(v *View) error {
		v._embedder = state.NewFSEmbedder(fs)
		if v.Schema != nil {
			v._embedder.SetType(v.Schema.Type())
		}
		aTag, err := tags.ParseViewTags(tag, v._embedder.EmbedFS())
		if err != nil {
			return err
		}
		opts, err := v.buildViewOptions(nil, aTag)
		if err != nil {
			return err
		}
		for _, opt := range opts {
			opt(v)
		}
		return nil
	}
}

// WithSQL creates SQL FROM View option
func WithSQL(SQL string, parameters ...*state.Parameter) Option {
	return func(v *View) error {
		v.EnsureTemplate()
		v.Template.Source = SQL
		v.Template.Parameters = parameters
		return nil
	}
}

// WithConnector creates connector View option
func WithConnector(connector *Connector) Option {
	return func(v *View) error {
		v.Connector = connector
		return nil
	}
}

// WithDBConfig creates connector View option
func WithDBConfig(dbConfig *DBConfig) Option {
	return func(v *View) error {
		v.Connector = &Connector{DBConfig: *dbConfig}
		return nil
	}
}

// WithConnectorRef creates connector View option
func WithConnectorRef(ref string) Option {
	return func(v *View) error {
		v.Connector = NewRefConnector(ref)
		return nil
	}
}

// WithSchema creates connector View option
func WithSchema(schema *state.Schema) Option {
	return func(v *View) error {
		v.Schema = schema
		return nil
	}
}

// WithMode creates mode View option
func WithMode(mode Mode) Option {
	return func(v *View) error {
		v.Mode = mode
		return nil
	}
}

// WithTemplate creates connector View option
func WithTemplate(template *Template) Option {
	return func(v *View) error {
		v.Template = template
		return nil
	}
}

// WithOneToMany creates to many relation View option
func WithOneToMany(holder string, on Links, ref *ReferenceView, opts ...RelationOption) Option {
	return func(v *View) error {
		relation := &Relation{Cardinality: state.Many, On: on, Holder: holder, Of: ref}
		for _, opt := range opts {
			opt(relation)
		}
		v.With = append(v.With, relation)
		return nil
	}
}

// WithOneToOne creates to one relation View option
func WithOneToOne(holder string, on Links, ref *ReferenceView, opts ...RelationOption) Option {
	return func(v *View) error {
		relation := &Relation{Cardinality: state.One, On: on, Holder: holder, Of: ref}
		for _, opt := range opts {
			opt(relation)
		}
		v.With = append(v.With, relation)
		setter.SetStringIfEmpty(&relation.Name, v.Name)
		return nil
	}
}

// WithCriteria creates criteria constraints View option
func WithCriteria(columns ...string) Option {
	return func(v *View) error {
		if v.Selector == nil {
			v.Selector = &Config{}
		}
		if v.Selector.Constraints == nil {
			v.Selector.Constraints = &Constraints{}
		}
		v.Selector.Constraints.Criteria = true
		v.Selector.Constraints.Filterable = columns
		return nil
	}
}

func WithPartitioned(partitioned *Partitioned) Option {
	return func(v *View) error {
		v.Partitioned = partitioned
		return nil
	}
}

// WithResource creates resource View option
func WithResource(resource state.Resource) Option {
	return func(v *View) error {
		if res, ok := resource.(*Resourcelet); ok {
			v._resource = res._resource
		}
		return nil
	}
}

// WithViewType creates schema type View option
func WithViewType(t reflect.Type, options ...state.SchemaOption) Option {
	return func(v *View) error {
		v.Schema = state.NewSchema(t, options...)
		return nil
	}
}

func WithViewKind(mode Mode) Option {
	return func(aView *View) error {
		aView.Mode = mode
		return nil
	}
}

func WithReinitialize() Option {
	return func(aView *View) error {
		aView._initialized = false
		return nil
	}
}
