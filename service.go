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

func (s *Service) Read(ctx context.Context, viewId string, dest interface{}, option ...reader.Option) error {
	aView, err := s.View(ctx, wrapWithMethod(http.MethodGet, viewId))
	if err != nil {
		return err
	}
	return s.reader.ReadInto(ctx, dest, aView, option...)
}

// Exec executes
func (s *Service) Exec(ctx context.Context, viewId string, options ...executor.Option) error {
	execView, err := s.View(ctx, wrapWithMethod(http.MethodPost, viewId))
	if err != nil {
		return err
	}
	return s.executor.Execute(ctx, execView, options...)
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

func (s *Service) AddViews(ctx context.Context, views ...*view.View) error {
	components := repository.NewComponents(ctx, s.options...)
	components.Resource.MergeFrom(s.resource, s.repository.Extensions().Types)
	components.Resource.Views = views
	refConnector := ""
	if len(s.resource.Connectors) > 0 {
		refConnector = s.resource.Connectors[0].Name
	}
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
		return err
	}
	s.repository.Registry().Register(component)
	return nil
}

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

func (s *Service) View(ctx context.Context, name string) (*view.View, error) {
	component, err := s.Component(ctx, name)
	if err != nil {
		return nil, err
	}
	return component.View, nil
}

func (s *Service) NewComponentSession(aComponent *repository.Component, request *http.Request) *session.Session {
	options := aComponent.LocatorOptions(request, aComponent.UnmarshalFunc(request))
	return session.New(aComponent.View, session.WithLocatorOptions(options...))
}

func (s *Service) Operate(ctx context.Context, aComponent *repository.Component, aSession *session.Session) (interface{}, error) {
	return s.operator.Operate(ctx, aComponent, aSession)
}

func (s *Service) AddResource(name string, resource *view.Resource) {
	s.repository.Resource().AddResource(name, resource)
}

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

func (s *Service) AddConnector(ctx context.Context, name string, driver string, dsn string) (*view.Connector, error) {
	connector := view.NewConnector(name, driver, dsn)
	err := s.AddConnectors(ctx, connector)
	return connector, err
}

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

// New creates a datly service
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
