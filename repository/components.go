package repository

import (
	"bytes"
	"context"
	"fmt"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/internal/translator/parser"
	"github.com/viant/datly/repository/codegen"
	"github.com/viant/datly/repository/version"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/discover"
	"github.com/viant/datly/view/state"
	"github.com/viant/toolbox"
	"github.com/viant/xreflect"
	"gopkg.in/yaml.v3"
	"reflect"
)

type Components struct {
	URL         string `json:",omitempty" yaml:",omitempty"`
	Version     version.Control
	With        []string //list of resource to inherit from  `json:",omitempty"`
	Components  []*Component
	Resource    *view.Resource
	columns     *discover.Columns
	resources   Resources
	options     *Options
	initialized bool
}

func (c *Components) Init(ctx context.Context) error {
	if c.initialized {
		return nil
	}
	c.initialized = true
	if err := c.ensureColumns(ctx); err != nil {
		return err
	}
	var columns = map[string]view.Columns{}
	if c.columns != nil {
		columns = c.columns.Items
	}

	//TODO replace with explicit option
	var options = []interface{}{c.options.extensions}
	if len(columns) > 0 {
		options = append(options, columns)
	}

	if c.options.metrics != nil && len(c.Components) > 0 {
		options = append(options, &view.Metrics{Method: c.Components[0].Method, Service: c.options.metrics})
	}
	for _, component := range c.Components {
		if len(component.with) > 0 {
			c.With = append(c.With, component.with...)
		}
	}

	err := c.mergeResources(ctx)
	if err != nil {
		return err
	}
	if inputSchema := c.Components[0].Input.Type.Schema; inputSchema != nil {
		if rType, _ := c.Resource.TypeRegistry().Lookup(inputSchema.Name, xreflect.WithPackage(inputSchema.Package)); rType != nil {
			embedder := state.NewFSEmbedder(nil)
			if embedder.SetType(rType) {
				c.Resource.FSEmbedder = embedder
			}
		}
	}

	if err = c.Resource.Init(ctx, options...); err != nil {
		return err
	}

	c.Resource.Lock()
	substitutes := c.Resource.Substitutes
	c.Resource.Unlock()

	for _, component := range c.Components {
		if err := component.Init(ctx, c.Resource); err != nil {
			return err
		}

		inputType := component.Input.Type.Type().Type()

		for _, parameter := range component.Input.Type.Parameters {
			if param := c.Resource.Parameters.Lookup(parameter.Name); param == nil {
				c.Resource.Parameters.Append(parameter)
			} else if parameter.In.Kind == state.KindConst {
				parameter.Value = param.Value
			}

			switch parameter.In.Kind {
			case state.KindConst:
				switch parameter.Value.(type) {
				case string:
					val, _ := parameter.Value.(string)
					parameter.Value = substitutes.Replace(val)
				case *string:
					val, _ := parameter.Value.(*string)
					parameter.Value = substitutes.Replace(*val)
				}
			case state.KindView:
				viewName := parameter.In.Name
				if prev, _ := c.Resource.View(viewName); prev != nil {
					if !codegen.IsGeneratorContext(ctx) && !prev.Schema.IsNamed() && inputType != nil { //adjust with named type
						if fName, ok := inputType.FieldByName(parameter.Name); ok {
							parameter.Schema.SetType(fName.Type)
							prev.Schema.SetType(fName.Type)
							viewParmeter := component.View.Template.Parameters.Lookup(parameter.Name)
							if viewParmeter != nil {
								viewParmeter.Schema.SetType(fName.Type)
							}
						}
					}
					continue
				}

				viewParameters := component.Input.Type.Parameters.UsedBy(parameter.SQL)
				viewSchema := parameterViewSchema(parameter)
				SQL := parameter.SQL
				if len(viewParameters) > 0 {
					aState := inference.State{}
					aState.AppendParameters(viewParameters)
					if tmpl, _ := parser.NewTemplate(SQL, &aState); tmpl != nil {
						SQL = tmpl.Sanitize()
					}

				}

				parameterView, err := view.New(viewName, "",
					view.WithMode(view.ModeQuery),
					view.WithSchema(viewSchema),
					view.WithConnector(component.View.Connector),
					view.WithTemplate(
						view.NewTemplate(SQL,
							view.WithTemplateParameters(viewParameters...))))

				if err != nil {
					return err
				}
				if err := parameterView.Init(ctx, c.Resource); err != nil {
					return fmt.Errorf("failed to initialize view parameter: %v, %w", parameter.Name, err)
				}
				component.indexedView[viewName] = parameterView
				c.Resource.Views = append(c.Resource.Views, parameterView)
			}
		}

	}

	return nil
}

func parameterViewSchema(parameter *state.Parameter) *state.Schema {
	rType := parameter.Schema.Type()
	var schemaOptions []state.SchemaOption
	if rType.Kind() == reflect.Slice {
		rType = rType.Elem()
		schemaOptions = append(schemaOptions, state.WithMany())
	}
	viewSchema := state.NewSchema(rType, schemaOptions...)
	return viewSchema
}

func (c *Components) columnsFile() string {
	parent, leaf := url.Split(c.URL, file.Scheme)
	return url.Join(parent, ".meta", leaf)
}

func (c *Components) mergeResources(ctx context.Context) error {
	if len(c.With) == 0 {
		return nil
	}
	for _, ref := range c.With {
		refResource, err := c.options.resources.Lookup(ref)
		if err != nil {
			return err
		}
		c.Resource.MergeFrom(refResource.Resource, c.options.extensions.Types)
	}
	return nil
}

func (c *Components) ensureColumns(ctx context.Context) error {
	columnFile := c.columnsFile()
	if ok, _ := c.options.fs.Exists(ctx, columnFile); !ok {
		return nil
	}
	if c.columns == nil {
		c.columns = discover.New(columnFile, c.options.fs)
	}
	if !c.options.UseColumn() {
		return nil
	}
	if len(c.columns.Items) > 0 {
		return nil
	}
	return c.columns.Load(ctx)
}

// NewComponents creates components
func NewComponents(ctx context.Context, options ...Option) *Components {
	opts := NewOptions(options)
	ret := &Components{Resource: &view.Resource{}}
	ret.options = opts
	ret.resources = opts.resources
	return ret
}

func LoadComponents(ctx context.Context, URL string, opts ...Option) (*Components, error) {
	options := NewOptions(opts)
	data, err := options.fs.DownloadWithURL(ctx, URL)
	if err != nil {
		return nil, err
	}

	substitutes := options.resources.Substitutes()
	for k, item := range substitutes {
		if options.path != nil && len(options.path.With) > 0 {
			if options.path.HasWith(k) {
				data = []byte(item.Replace(string(data)))
			}
		} else { //fallback fuzzy substitution
			if bytes.Contains(data, []byte(k)) {
				data = []byte(item.Replace(string(data)))
			}
		}
	}
	components, err := unmarshalComponent(data)
	if err != nil {
		return nil, err
	}
	components.URL = URL
	components.options = options
	if components.Resource == nil {
		return nil, fmt.Errorf("resources were empty: %v", URL)
	}
	if err = components.mergeResources(ctx); err != nil {
		return nil, err
	}

	//TODO make it working
	//components.Resources.Metrics = options.metrics

	components.Resource.SourceURL = URL
	components.Resource.SetTypes(options.extensions.Types)
	_ = components.Resource.UpdateTime(ctx, URL)
	return components, nil
}

func unmarshalComponent(data []byte) (*Components, error) {
	aMap := map[string]interface{}{}
	if err := yaml.Unmarshal(data, &aMap); err != nil {
		return nil, err
	}
	ensureComponents(aMap)
	components := &Components{}
	err := toolbox.DefaultConverter.AssignConverted(components, aMap)
	if err != nil {
		return nil, err
	}
	return components, err
}

func ensureComponents(aMap map[string]interface{}) {
	if _, ok := aMap["Components"]; !ok { //backward compatibility
		aMap["Components"] = aMap["Routes"]
	}
}
