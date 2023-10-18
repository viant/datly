package datly

import (
	"context"
	"fmt"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	sjwt "github.com/viant/datly/service/auth/jwt"
	"github.com/viant/datly/service/auth/mock"
	"github.com/viant/datly/service/executor"
	"github.com/viant/datly/service/operator"
	"github.com/viant/datly/service/reader"
	"github.com/viant/datly/service/session"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/extension"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/scy/auth/jwt/signer"
	xhandler "github.com/viant/xdatly/handler"
	"net/http"
	"strings"
	"time"
)

type (
	Service struct {
		repository *repository.Service
		resource   *view.Resource
		reader     *reader.Service
		executor   *executor.Executor
		operator   *operator.Service
		options    []repository.Option
		signer     *signer.Service
	}
)

func (s *Service) NewComponentSession(aComponent *repository.Component, request *http.Request) *session.Session {
	options := aComponent.LocatorOptions(request, aComponent.UnmarshalFunc(request))
	return session.New(aComponent.View, session.WithLocatorOptions(options...))
}

// HandlerSession returns handler session
func (s *Service) HandlerSession(ctx context.Context, aComponent *repository.Component, aSession *session.Session) (xhandler.Session, error) {
	return s.operator.HandlerSession(ctx, aComponent, aSession)
}

// SignRequest signes http request with the supplied claim
func (s *Service) SignRequest(request *http.Request, claims *jwt.Claims) error {
	if claims != nil {
		aSigner := s.repository.JWTSigner()
		if aSigner == nil {
			return fmt.Errorf("JWT aSigner was empty")
		}
		token, err := aSigner.Create(time.Hour, claims)
		if err == nil {
			request.Header.Set("Authorization", "Bearer "+token)
		} else {
			return err
		}
	}
	return nil
}

// Operate performs respective operation on supplied component
func (s *Service) Operate(ctx context.Context, aComponent *repository.Component, aSession *session.Session) (interface{}, error) {
	return s.operator.Operate(ctx, aComponent, aSession)
}

// Read reads data from a view
func (s *Service) Read(ctx context.Context, viewId string, dest interface{}, option ...reader.Option) error {
	aView, err := s.View(ctx, wrapWithMethod(http.MethodGet, viewId))
	if err != nil {
		return err
	}
	return s.reader.ReadInto(ctx, dest, aView, option...)
}

// Exec executes view template
func (s *Service) Exec(ctx context.Context, viewId string, options ...executor.Option) error {
	execView, err := s.View(ctx, wrapWithMethod(http.MethodPost, viewId))
	if err != nil {
		return err
	}
	return s.executor.Execute(ctx, execView, options...)
}

// Component returns component matched by name, optionally you can use METHOD:component name notation
func (s *Service) Component(ctx context.Context, name string) (*repository.Component, error) {
	method := http.MethodGet
	if index := strings.Index(name, ":"); index != -1 {
		method = strings.ToUpper(name[:index])
		name = name[index+1:]
	}
	aPath := contract.NewPath(method, name)
	component, err := s.repository.Registry().Lookup(ctx, aPath)
	if component != nil {
		return component, err
	}
	aPath = contract.NewPath(method, internalPath(name))
	if component, _ = s.repository.Registry().Lookup(ctx, aPath); component != nil {
		return component, nil
	}
	return nil, err
}

// View returns a view matched by name, optionally you can use METHOD:component name notation
func (s *Service) View(ctx context.Context, name string) (*view.View, error) {
	component, err := s.Component(ctx, name)
	if err != nil {
		return nil, err
	}
	return component.View, nil
}

// AddViews adds views to the repository
func (s *Service) AddViews(ctx context.Context, views ...*view.View) (*repository.Component, error) {
	components, refConnector := s.buildDefaultComponents(ctx)
	components.Resource.Views = views
	if refConnector != "" {
		for _, aView := range views {
			if aView.Connector == nil {
				aView.Connector = &view.Connector{}
			}
			if aView.Connector.Driver == "" && aView.Connector.Ref == "" {
				aView.Connector = view.NewRefConnector(refConnector)
			}
		}
	}
	component := &repository.Component{}
	component.View = view.NewRefView(views[0].Name)
	component.Path.URI = internalPath(views[0].Name)

	switch views[0].Mode {
	case view.ModeExec:
		component.Path.Method = http.MethodPost
	default:
		component.Path.Method = http.MethodGet
	}
	components.Components = append(components.Components, component)
	if err := components.Init(ctx); err != nil {
		return nil, err
	}
	s.repository.Registry().Register(component)
	return component, nil
}

func (s *Service) buildDefaultComponents(ctx context.Context) (*repository.Components, string) {
	components := repository.NewComponents(ctx, s.options...)
	components.Resource.MergeFrom(s.resource, s.repository.Extensions().Types)
	refConnector := ""
	if len(s.resource.Connectors) > 0 {
		refConnector = s.resource.Connectors[0].Name
	}
	return components, refConnector
}

// AddComponents adds components to repository
func (s *Service) AddComponents(ctx context.Context, components *repository.Components) error {
	if err := components.Init(ctx); err != nil {
		return err
	}
	s.repository.Registry().Register(components.Components...)
	return nil
}

// AddComponents adds components to repository
func (s *Service) AddComponent(ctx context.Context, component *repository.Component) error {
	components, refConnector := s.buildDefaultComponents(ctx)
	if refConnector != "" {
		if component.View.Connector == nil {
			component.View.Connector = &view.Connector{}
		}
		if connector := component.View.Connector; connector.Driver == "" && connector.Ref == "" {
			component.View.Connector = view.NewRefConnector(refConnector)
		}
	}
	if err := components.Init(ctx); err != nil {
		return err
	}
	s.repository.Registry().Register(components.Components...)
	return nil
}

// AddHandler adds handler component to repository
func (s *Service) AddHandler(ctx context.Context, aPath contract.Path, handler xhandler.Handler) (*repository.Component, error) {
	component := repository.NewComponent(aPath, repository.WithHandler(handler))
	err := s.AddComponent(ctx, component)
	return component, err
}

// AddResource adds named resource
func (s *Service) AddResource(name string, resource *view.Resource) {
	s.repository.Resource().AddResource(name, resource)
}

// AddConnectors adds connectors
func (s *Service) AddConnectors(ctx context.Context, connectors ...*view.Connector) error {
	connectionResource, err := s.repository.Resource().Lookup(view.ResourceConnectors)
	if err != nil {
		return err
	}
	byName := connectionResource.ConnectorByName()
	for _, connector := range connectors {
		if conn, _ := connectionResource.Connector(connector.Name); conn != nil {
			continue
		}
		if err = connector.Init(ctx, byName); err != nil {
			return err
		}
	}
	s.resource.Connectors = append(s.resource.Connectors, connectors...)
	return nil
}

// AddConnector adds connector
func (s *Service) AddConnector(ctx context.Context, name string, driver string, dsn string) (*view.Connector, error) {
	connector := view.NewConnector(name, driver, dsn)
	err := s.AddConnectors(ctx, connector)
	return connector, err
}

// LoadComponents loads components into registry, it returns loaded components
func (s *Service) LoadComponents(ctx context.Context, URL string, opts ...repository.Option) (*repository.Components, error) {
	opts = append([]repository.Option{
		repository.WithResources(s.repository.Resource()),
		repository.WithExtensions(s.repository.Extensions()),
	}, opts...)
	components, err := repository.LoadComponents(ctx, URL, opts...)
	if err != nil {
		return nil, err
	}
	if err = components.Init(ctx); err != nil {
		return nil, err
	}
	s.repository.Registry().Register(components.Components...)
	return components, nil
}

// New creates a datly service, repository allows you to bootstrap empty or existing yaml repository
func New(ctx context.Context, options ...repository.Option) (*Service, error) {
	options = append([]repository.Option{
		repository.WithJWTSigner(mock.HmacJwtSigner()),
		repository.WithJWTVerifier(mock.HmacJwtVerifier()),
	}, options...)
	aRepository, err := repository.New(ctx, options...)
	if err != nil {
		return nil, err
	}

	if verifier := aRepository.JWTVerifier(); verifier != nil {
		codecs := aRepository.Extensions().Codecs
		codecs.RegisterInstance(
			extension.CodecKeyJwtClaim, sjwt.New(verifier.VerifyClaims), time.Time{},
		)
	}

	ret := &Service{
		reader:     reader.New(),
		executor:   executor.New(),
		repository: aRepository,
		resource:   &view.Resource{},
		options:    options,
		operator:   operator.New(),
	}
	return ret, nil
}

func internalPath(URI string) string {
	return "/internal/" + URI
}

func wrapWithMethod(method, name string) string {
	if index := strings.Index(name, ":"); index != -1 {
		return name
	}
	return method + ":" + name
}
