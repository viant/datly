package repository

import (
	"context"
	"github.com/viant/datly/gateway/router/marshal/config"
	"github.com/viant/datly/gateway/router/marshal/json"
	"github.com/viant/datly/repository/async"
	"github.com/viant/datly/repository/component"
	"github.com/viant/datly/repository/content"
	"github.com/viant/datly/service/executor/handler"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state/kind/locator"
	"net/http"
)

// Component represents abstract API view/handler based component
type (
	Component struct {
		component.Path
		component.Contract
		content.Content
		Async          *async.Module `json:",omitempty"`
		View           *view.View    `json:",omitempty"`
		NamespacedView *view.NamespacedView
		Handler        *handler.Handler `json:",omitempty"`
		indexedView    view.NamedViews
		SourceURL      string
	}
)

func (c *Component) Init(ctx context.Context, resource *view.Resource) (err error) {
	if c.Handler != nil {
		if err = c.Handler.Init(ctx, resource); err != nil {
			return err
		}
	}
	err = c.initView(ctx, resource)
	if err != nil {
		return err
	}
	if err = c.Contract.Init(ctx, &c.Path, c.View); err != nil {
		return err
	}
	c.Contract.Input.Parameters = resource.NamedParameters()
	return nil
}

func (c *Component) initView(ctx context.Context, resource *view.Resource) error {
	c.View.Standalone = true
	if c.View.Name == "" {
		c.View.Name = c.View.Ref
	}
	if err := c.View.Init(ctx, resource); err != nil {
		return err
	}
	c.NamespacedView = view.IndexViews(c.View)
	c.indexedView = resource.Views.Index()
	return nil
}

func (c *Component) Exclusion(state *view.State) []*json.FilterEntry {
	result := make([]*json.FilterEntry, 0)
	state.Lock()
	defer state.Unlock()
	for viewName, selector := range state.Views {
		if len(selector.Columns) == 0 {
			continue
		}
		var aPath string
		nsView := c.NamespacedView.ByName(viewName)
		if nsView == nil {
			aPath = ""
		} else {
			aPath = nsView.Path
		}
		fields := make([]string, len(selector.Fields))
		for i := range selector.Fields {
			fields[i] = selector.Fields[i]
		}
		result = append(result, &json.FilterEntry{
			Path:   aPath,
			Fields: fields,
		})

	}
	return result
}

func (c *Component) LocatorOptions(request *http.Request, unmarshal shared.Unmarshal) []locator.Option {
	var result []locator.Option
	result = append(result, locator.WithUnmarshal(unmarshal))
	result = append(result, locator.WithRequest(request))
	result = append(result, locator.WithURIPattern(c.URI))
	result = append(result, locator.WithIOConfig(c.IOConfig()))
	result = append(result, locator.WithInputParameters(c.Input.Parameters))
	result = append(result, locator.WithOutputParameters(c.Output.Type.Parameters))
	if c.Input.Body.Schema != nil {
		result = append(result, locator.WithBodyType(c.BodyType()))
	}
	if len(c.indexedView) > 0 {
		result = append(result, locator.WithViews(c.indexedView))
	}
	return result
}

func (c *Component) IOConfig() config.IOConfig {
	return config.IOConfig{
		OmitEmpty:  c.Output.OmitEmpty,
		CaseFormat: *c.Output.FormatCase(),
		Exclude:    config.Exclude(c.Output.Exclude).Index(),
		DateLayout: c.DateFormat,
	}
}

func (r *Component) initAsyncIfNeeded(ctx context.Context) error {
	//r._async = async.NewChecker()
	//if r.Async != nil {
	//	//if err := r.Async.Init(ctx, r._resource, r.View); err != nil {
	//	//	return err
	//	//}
	//
	//	//return r.ensureJobTable(ctx)
	//}

	return nil
}
