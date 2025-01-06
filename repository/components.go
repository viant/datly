package repository

import (
	"bytes"
	"context"
	"embed"
	_ "embed"
	"fmt"
	"github.com/viant/afs"
	fembed "github.com/viant/afs/embed"
	"github.com/viant/afs/file"
	furl "github.com/viant/afs/url"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/internal/translator/parser"
	"github.com/viant/datly/repository/codegen"
	"github.com/viant/datly/repository/version"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/discover"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/tags"
	"github.com/viant/toolbox"
	"github.com/viant/xreflect"
	"gopkg.in/yaml.v3"
	"path"
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
	aComponent := c.Components[0]
	var embedFs *embed.FS
	if rType := c.ReflectType(c.Components[0].Input.Type.Schema); rType != nil {
		embedFs, _ = c.ensureEmbedder(ctx, rType)
		if ioType, _ := state.NewType(state.WithSchema(state.NewSchema(rType)), state.WithFS(embedFs), state.WithResource(view.NewResources(c.Resource, aComponent.View))); ioType != nil {
			_ = c.updateIOTypeDependencies(ctx, ioType, embedFs, c.Components[0].View, false)
		}
	}

	if err = c.Resource.Init(ctx, options...); err != nil {
		return err
	}
	for _, component := range c.Components {
		component.embedFs = embedFs
		if err = component.Init(ctx, c.Resource); err != nil {
			return err
		}
		if err = c.updateIOTypeDependencies(ctx, &component.Input.Type, embedFs, c.Components[0].View, true); err != nil {
			return fmt.Errorf("failed to update io dependencies:%w", err)
		}
		if err = c.updateIOTypeDependencies(ctx, &component.Output.Type, embedFs, c.Components[0].View, true); err != nil {
			return fmt.Errorf("failed to update io dependencies:%w", err)
		}
	}
	return nil
}

func (c *Components) ReflectType(schema *state.Schema) reflect.Type {
	if schema == nil {
		return nil
	}
	if schema.IsNamed() {
		return schema.Type()
	}
	rType, _ := c.Resource.TypeRegistry().Lookup(schema.Name, xreflect.WithPackage(schema.Package))
	return rType
}

func (c *Components) ensureEmbedder(ctx context.Context, rType reflect.Type) (*embed.FS, error) {
	embedder := state.NewFSEmbedder(nil)
	if embedder.SetType(rType) {
		c.Resource.FSEmbedder = embedder
	}
	embedFs := embedder.EmbedFS()
	if embedFs != nil {
		return embedFs, nil
	}
	embedFs, err := c.buildEmbedFs(ctx)
	if err != nil {
		return nil, err
	}
	c.Resource.FSEmbedder = state.NewFSEmbedder(embedFs)
	return embedFs, nil
}

func (c *Components) buildEmbedFs(ctx context.Context) (*embed.FS, error) {
	fs := afs.New()
	holder := fembed.NewHolder()
	var unique = map[string]bool{}
	baseURL, _ := furl.Split(c.URL, file.Scheme)
	for _, item := range c.Resource.Views {
		if tmpl := item.Template; tmpl != nil && tmpl.SourceURL != "" {
			folder, _ := path.Split(tmpl.SourceURL)
			embedFsURL := furl.Join(baseURL, folder)
			objects, err := fs.List(context.Background(), embedFsURL)
			if err != nil {
				return nil, err
			}
			for _, candidate := range objects {
				if candidate.IsDir() {
					continue
				}
				data, err := fs.DownloadWithURL(ctx, candidate.URL())
				if err != nil {
					return nil, err
				}
				key := folder + candidate.Name()
				if _, found := unique[key]; found {
					continue
				}
				unique[key] = true
				holder.Add(key, string(data))
			}
		}
	}
	embedFs := holder.EmbedFs()
	return embedFs, nil
}

func (c *Components) updateIOTypeDependencies(ctx context.Context, ioType *state.Type, fs *embed.FS, aView *view.View, isInput bool) error {
	if ioType == nil || ioType.Type() == nil {
		return nil
	}
	c.Resource.Lock()
	substitutes := c.Resource.Substitutes
	c.Resource.Unlock()
	inCodeGeneration := codegen.IsGeneratorContext(ctx)
	rType := types.EnsureStruct(ioType.Type().Type())
	for _, parameter := range ioType.Parameters {
		xField, ok := rType.FieldByName(parameter.Name)
		if !ok {
			continue
		}
		if !parameter.Schema.IsNamed() && !inCodeGeneration { //prefer named type over inlined type (except code generation)
			parameter.Schema.SetType(xField.Type)
		}

		if param := c.Resource.Parameters.Lookup(parameter.Name); param == nil && isInput {
			c.Resource.Parameters.Append(parameter)
			if parameter.In.Kind == state.KindConst {
				parameter.Value = param.Value
			}
		}

		switch parameter.In.Kind {
		case state.KindView:
			_, err := c.ensureView(ctx, ioType.Parameters, parameter, fs, aView.Connector)
			if err != nil {
				return err
			}

		case state.KindOutput:
			switch parameter.In.Name {
			case "summary":
				if aTag, _ := tags.Parse(reflect.StructTag(parameter.Tag), nil, tags.SQLSummaryTag); aTag != nil {
					if summary := aTag.SummarySQL; summary.SQL != "" {
						aView.Template.Summary = &view.TemplateSummary{Name: parameter.Name, Source: summary.SQL, Schema: parameter.Schema}
					}
				}
			case "view":
				if !aView.Schema.IsNamed() {
					if aView.Ref != "" {
						if baseView, _ := c.Resource.View(aView.Ref); baseView != nil {
							aView = baseView
						}
					}
					aView.Schema.SetType(parameter.Schema.Type())
				}
			}

		case state.KindConst:
			if isInput {
				switch parameter.Value.(type) {
				case string:
					val, _ := parameter.Value.(string)
					parameter.Value = substitutes.Replace(val)
				case *string:
					val, _ := parameter.Value.(*string)
					parameter.Value = substitutes.Replace(*val)
				}
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
	parent, leaf := furl.Split(c.URL, file.Scheme)
	return furl.Join(parent, ".meta", leaf)
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

func (c *Components) ensureView(ctx context.Context, parameters state.Parameters, parameter *state.Parameter, fs *embed.FS, connector *view.Connector) (*view.View, error) {
	aView, _ := c.Resource.View(parameter.In.Name)
	if aView != nil {
		if !aView.Schema.IsNamed() {
			aView.Schema.SetType(parameter.Schema.Type())
		}
		return aView, nil
	}
	viewParameters := parameters.UsedBy(parameter.SQL)
	viewSchema := parameterViewSchema(parameter)
	SQL := parameter.SQL
	if len(viewParameters) > 0 {
		aState := inference.State{}
		aState.AppendParameters(viewParameters)
		if tmpl, _ := parser.NewTemplate(SQL, &aState); tmpl != nil {
			SQL = tmpl.Sanitize()
		}

	}
	var err error
	viewName := parameter.In.Name
	aView, err = view.New(viewName, "",
		view.WithMode(view.ModeQuery),
		view.WithSchema(viewSchema),
		view.WithConnector(connector),
		view.WithFS(fs),
		view.WithTemplate(
			view.NewTemplate(SQL,
				view.WithTemplateParameters(viewParameters...))))

	if err != nil {
		return nil, err
	}
	if err := aView.Init(ctx, c.Resource); err != nil {
		return nil, fmt.Errorf("failed to initialize view parameter: %v, %w", parameter.Name, err)
	}
	c.Resource.Views = append(c.Resource.Views, aView)
	return aView, nil
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
