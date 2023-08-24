package datly

import (
	"bytes"
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/auth/jwt"
	"github.com/viant/datly/auth/mock"
	"github.com/viant/datly/config"
	"github.com/viant/datly/executor"
	"github.com/viant/datly/reader"
	"github.com/viant/datly/router"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	sjwt "github.com/viant/scy/auth/jwt"
	"github.com/viant/scy/auth/jwt/signer"
	"github.com/viant/scy/auth/jwt/verifier"
	"github.com/viant/xreflect"
	"io"
	"net/http"
	surl "net/url"
	"os"
	"path"
	"strings"
	"time"

	"sync/atomic"
)

type (
	Service struct {
		initialized    int32
		reader         *reader.Service
		executor       *executor.Executor
		jwtVerifier    *verifier.Service
		routerResource *router.Resource
		resource       *view.Resource
		JwtSigner      *signer.Service
		config         *Config
		connector      *view.Connector
		types          *xreflect.Types
		registry       *config.Registry
		fs             afs.Service
	}

	Config struct {
		Connector    *view.Connector
		Connectors   []*view.Connector
		JWTValidator *verifier.Config
		JWTSigner    *signer.Config
	}
)

// Read reads data
func (s *Service) Read(ctx context.Context, viewId string, dest interface{}, option ...reader.Option) error {
	aView, err := s.resource.View(viewId)
	if err != nil {
		return err
	}
	return s.reader.ReadInto(ctx, dest, aView, option...)
}

// Exec executes
func (s *Service) Exec(ctx context.Context, viewId string, options ...executor.Option) error {
	execView, err := s.resource.View(viewId)
	if err != nil {
		return err
	}
	return s.executor.Execute(ctx, execView, options...)
}

func (s *Service) VerifyClaims(ctx context.Context, tokenString string) (*sjwt.Claims, error) {
	if s.jwtVerifier == nil {
		return nil, fmt.Errorf("jwtVerifier was not configuered")
	}
	return s.jwtVerifier.VerifyClaims(ctx, tokenString)
}

// AddViews adds view
func (s *Service) AddViews(views ...*view.View) error {
	if err := s.ensureNotInitialised(); err != nil {
		return err
	}
	s.resource.AddViews(views...)
	return nil
}

// View returns registered view
func (s *Service) View(name string) (*view.View, error) {
	return s.resource.View(name)
}

// Connector returns registered connector or default connector
func (s *Service) Connector(name string) (*view.Connector, error) {
	if name == "" && s.connector != nil {
		return s.connector, nil
	}
	return s.resource.Connector(name)
}

func (s *Service) MergeResource(resource *view.Resource, types *xreflect.Types) error {
	if err := s.ensureNotInitialised(); err != nil {
		return err
	}
	s.resource.MergeFrom(resource, types)
	return nil
}

// AddParameter add global parameters
func (s *Service) AddParameter(parameters ...*state.Parameter) error {
	if err := s.ensureNotInitialised(); err != nil {
		return err
	}
	s.resource.AddParameters(parameters...)
	return nil
}

// AddConnectors adds connectors
func (s *Service) AddConnectors(connectors ...*view.Connector) error {
	if err := s.ensureNotInitialised(); err != nil {
		return err
	}
	s.resource.AddConnectors(connectors...)
	return nil
}

// AddConnector adds connector
func (s *Service) AddConnector(name, driver, dsn string, opts ...view.ConnectorOption) (*view.Connector, error) {
	connector := view.NewConnector(name, driver, dsn, opts...)
	err := s.AddConnectors(connector)
	return connector, err
}

func (s *Service) ensureNotInitialised() error {
	if atomic.LoadInt32(&s.initialized) == 1 {
		return fmt.Errorf("can not update resource after server was initialised")
	}
	return nil
}

func (s *Service) ensureInitialised() error {
	if atomic.LoadInt32(&s.initialized) == 0 {
		return fmt.Errorf("can not get route - not initialised")
	}
	return nil
}

// Init initialises service
func (s *Service) Init(ctx context.Context, options ...interface{}) error {
	if !atomic.CompareAndSwapInt32(&s.initialized, 0, 1) {
		return fmt.Errorf("already initialised")
	}

	if s.routerResource != nil {
		s.routerResource.Resource.SetFs(s.fs)
		if err := s.routerResource.Init(ctx); err != nil {
			return err
		}
	}
	if s.registry != nil {
		options = append(options, s.registry)
	}
	if s.types != nil {
		options = append(options, s.types)
	}
	return s.resource.Init(ctx, options...)
}

func (s *Service) Routes() (*router.Resource, error) {
	if err := s.ensureInitialised(); err != nil {
		return nil, err
	}
	if s.routerResource == nil {
		return nil, fmt.Errorf("route resource was not loaded")
	}
	return s.routerResource, nil
}

// NewHttpRequest creates http request with auth header
func (s *Service) NewHttpRequest(method, URL string, jwtClaim *sjwt.Claims, body []byte) (*http.Request, error) {
	reqURL, err := surl.Parse(URL)
	if err != nil {
		return nil, err
	}
	httpRequest := &http.Request{
		URL:    reqURL,
		Method: method,
		Header: http.Header{},
	}
	if jwtClaim != nil {
		token, err := s.JwtSigner.Create(time.Hour, jwtClaim)
		if err == nil {
			httpRequest.Header.Set("Authorization", "Bearer "+token)
		} else {
			return nil, err
		}
	}
	if len(body) > 0 {
		httpRequest.Body = io.NopCloser(bytes.NewReader(body))
	}
	return httpRequest, nil
}

func (s *Service) LoadRoute(ctx context.Context, URL string, types ...*view.PackagedType) error {
	if err := s.ensureNotInitialised(); err != nil {
		return err
	}
	s.types, s.registry = s.initTypes(types)
	datlyRootURL := s.datlyRootURL(URL)
	dependencies, err := s.loadDependencies(ctx, datlyRootURL, s.types)
	if err != nil {
		return err
	}
	baseURL, _ := url.Split(URL, file.Scheme)
	os.Chdir(baseURL)
	routeResource, err := router.LoadResource(ctx, s.fs, URL, false, dependencies, s.registry)
	if err != nil {
		return err
	}
	s.routerResource = routeResource
	s.resource = routeResource.Resource
	return nil
}

func (s *Service) initTypes(types []*view.PackagedType) (*xreflect.Types, *config.Registry) {
	viewTypes := xreflect.NewTypes(xreflect.WithRegistry(config.Config.Types))
	aConfig := config.Config
	for _, pType := range types {
		_ = viewTypes.Register(pType.Name, xreflect.WithPackage(pType.Package), xreflect.WithReflectType(pType.Type))
	}
	return viewTypes, aConfig
}

func (s *Service) datlyRootURL(URL string) string {
	baseURL := URL
	if index := strings.Index(URL, "Datly/routes"); index != -1 {
		baseURL = URL[:index+5]
	}
	return baseURL
}

func (s *Service) loadDependencies(ctx context.Context, datlyRootURL string, types *xreflect.Types) (map[string]*view.Resource, error) {
	dependencies := map[string]*view.Resource{}
	if candidates, err := s.fs.List(ctx, url.Join(datlyRootURL, "dependencies")); err == nil {
		for _, candidate := range candidates {
			if candidate.IsDir() {
				continue
			}
			ext := path.Ext(candidate.Name())
			switch ext {
			case ".yaml", ".yml":
			default:
				continue
			}
			URL := candidate.URL()
			dependency, err := view.NewResourceFromURL(ctx, URL, types, nil)
			if err != nil {
				return nil, err
			}
			key := candidate.Name()[:len(candidate.Name())-len(ext)]
			dependencies[key] = dependency
		}
	}
	return dependencies, nil
}

// New creates a datly service
func New(cfg *Config) *Service {
	ret := &Service{
		config:    cfg,
		reader:    reader.New(),
		executor:  executor.New(),
		connector: cfg.Connector,
		fs:        afs.New(),
	}
	if cfg.Connector != nil {
		_ = ret.AddConnectors(cfg.Connector)
	}
	if cfg.JWTValidator != nil {
		ret.jwtVerifier = verifier.New(cfg.JWTValidator)
		if ret.jwtVerifier != nil {
			if err := ret.jwtVerifier.Init(context.Background()); err == nil {
				config.Config.RegisterCodec(
					config.CodecKeyJwtClaim, jwt.New(ret.jwtVerifier.VerifyClaims), time.Time{})
			}
		}
	}
	if cfg.JWTSigner != nil {
		ret.JwtSigner = signer.New(cfg.JWTSigner)
		ret.JwtSigner.Init(context.Background())
	}
	return ret
}

// NewConfig creates default config
func NewConfig() *Config {
	return &Config{
		JWTSigner:    mock.HmacJwtSigner(),
		JWTValidator: mock.HmacJwtVerifier(),
	}
}

// WithExecHttpRequest create http based parameters set execution option
func WithExecHttpRequest(ctx context.Context, route *router.Route, request *http.Request) executor.Option {
	return func(session *executor.Session) error {
		selectors := session.Selectors()
		err := router.BuildRouteSelectors(ctx, selectors, route, request)
		sel := selectors.Lookup(route.View)
		paramState := session.Lookup(session.View)
		*paramState = *sel.Template
		return err
	}
}

// WithReadHttpRequest create http based parameters set execution option
func WithReadHttpRequest(ctx context.Context, route *router.Route, request *http.Request) reader.Option {
	return func(session *reader.Session) error {
		selectors := session.States
		aView := session.View
		err := router.BuildRouteSelectors(ctx, selectors, route, request)
		sel := selectors.Lookup(aView)
		paramState := session.Lookup(session.View)
		*paramState = *sel.Template
		return err
	}
}
