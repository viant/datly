package datly

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/viant/cloudless/async/mbus"
	"github.com/viant/datly/gateway"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/repository/locator/component/dispatcher"
	sjwt "github.com/viant/datly/service/auth/jwt"
	"github.com/viant/datly/service/auth/mock"
	"github.com/viant/datly/service/executor"
	"github.com/viant/datly/service/operator"
	"github.com/viant/datly/service/reader"
	"github.com/viant/datly/service/session"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/extension"
	dcodec "github.com/viant/datly/view/extension/codec"
	verifier2 "github.com/viant/scy/auth/jwt/verifier"
	hstate "github.com/viant/xdatly/handler/state"

	"github.com/viant/datly/view/state"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/scy/auth/jwt/signer"
	"github.com/viant/structology"
	"github.com/viant/xdatly/codec"
	xhandler "github.com/viant/xdatly/handler"
	"net/http"
	nurl "net/url"
	"reflect"
	"strings"
	"time"
)

//go:embed Version
var Version string

type (
	Service struct {
		repository  *repository.Service
		resource    *view.Resource
		reader      *reader.Service
		executor    *executor.Executor
		operator    *operator.Service
		options     []repository.Option
		signer      *signer.Service
		substitutes map[string]view.Substitutes
		handler     http.Handler
	}

	sessionOptions struct {
		request  *http.Request
		resource state.Resource
		form     *hstate.Form
	}
	SessionOption func(o *sessionOptions)

	operateOptions struct {
		path           *contract.Path
		component      *repository.Component
		session        *session.Session
		output         interface{}
		input          interface{}
		sessionOptions []SessionOption
	}
	OperateOption func(o *operateOptions)
)

func newOperateOptions(opts []OperateOption) *operateOptions {
	ret := &operateOptions{}
	for _, opt := range opts {
		opt(ret)
	}
	return ret
}

// WithPath returns
func WithPath(aPath *contract.Path) OperateOption {
	return func(o *operateOptions) {
		o.path = aPath
	}
}

func WithSessionOptions(options ...SessionOption) OperateOption {
	return func(o *operateOptions) {
		o.sessionOptions = options
	}
}

// WithURI returns with URI option
func WithURI(URI string) OperateOption {
	return func(o *operateOptions) {
		pair := strings.Split(URI, ":")
		method := http.MethodGet
		if len(pair) == 2 {
			method = pair[0]
			URI = pair[1]
		}
		o.path = contract.NewPath(method, URI)
	}
}

func WithOutput(output interface{}) OperateOption {
	return func(o *operateOptions) {
		o.output = output
	}
}

func WithSession(session *session.Session) OperateOption {
	return func(o *operateOptions) {
		o.session = session
	}
}

func WithComponent(component *repository.Component) OperateOption {
	return func(o *operateOptions) {
		o.component = component
	}
}

func WithInput(input interface{}) OperateOption {
	return func(o *operateOptions) {
		o.input = input
	}
}

func newSessionOptions(opts []SessionOption) *sessionOptions {
	sessionOpt := &sessionOptions{}
	for _, opt := range opts {
		opt(sessionOpt)
	}
	if sessionOpt.request == nil {
		URL, _ := nurl.Parse("http://127.0.0.1/")
		sessionOpt.request = &http.Request{Header: make(http.Header), URL: URL}
	}
	if sessionOpt.form == nil {
		sessionOpt.form = hstate.NewForm()
	}
	return sessionOpt
}
func WithRequest(request *http.Request) SessionOption {
	return func(o *sessionOptions) {
		o.request = request
	}
}

func WithForm(form *hstate.Form) SessionOption {
	return func(o *sessionOptions) {
		o.form = form
	}
}

func WithStateResource(resource state.Resource) SessionOption {
	return func(o *sessionOptions) {
		o.resource = resource
	}
}

func (s *Service) NewComponentSession(aComponent *repository.Component, opts ...SessionOption) *session.Session {
	sessionOpt := newSessionOptions(opts)
	options := aComponent.LocatorOptions(sessionOpt.request, sessionOpt.form, aComponent.UnmarshalFunc(sessionOpt.request))
	aSession := session.New(aComponent.View, session.WithLocatorOptions(options...), session.WithStateResource(sessionOpt.resource))
	return aSession
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
			if request.Header == nil {
				request.Header = make(http.Header)
			}
			request.Header.Set("Authorization", "Bearer "+token)
		} else {
			return err
		}
	}
	return nil
}

func LoadInput(ctx context.Context, aSession *session.Session, aComponent *repository.Component, input interface{}) error {
	ctx = aSession.Context(ctx, false)
	if err := aSession.LoadState(aComponent.Input.Type.Parameters, input); err != nil {
		return err
	}
	if err := aSession.Populate(ctx); err != nil {
		return err
	}
	return nil
}

// Operate performs respective operation on supplied component
func (s *Service) Operate(ctx context.Context, opts ...OperateOption) (interface{}, error) {
	options := newOperateOptions(opts)
	var err error
	if options.component == nil {
		if options.path == nil {
			return nil, fmt.Errorf("path/component were empty")
		}
		if options.component, err = s.Component(ctx, options.path.Method+":"+options.path.URI); err != nil {
			return nil, err
		}
	}
	if options.session == nil {
		sOptions := append(options.sessionOptions, WithStateResource(options.component.View.Resource()))
		options.session = s.NewComponentSession(options.component, sOptions...)
	}
	if input := options.input; input != nil {
		if err = LoadInput(ctx, options.session, options.component, input); err != nil {
			return nil, err
		}
	}

	response, err := s.operator.Operate(ctx, options.session, options.component)
	if err != nil {
		return nil, err
	}
	if output := options.output; output != nil {
		if err = s.Reconcile(response, output); err != nil {
			return nil, err
		}
	}
	return response, err
}

// Reconcile reconciles from with to
func (s *Service) Reconcile(from interface{}, to interface{}) error {
	responseType := reflect.TypeOf(from)
	outputType := reflect.TypeOf(to)
	if outputType.Elem() == responseType {
		responseReflect := reflect.ValueOf(from)
		outputReflect := reflect.ValueOf(to)
		outputReflect.Elem().Set(responseReflect)
		return nil
	}
	copier := session.NewCopier(reflect.TypeOf(from), reflect.TypeOf(to))
	return copier.Copy(from, to, session.WithDebug())
}

func (s *Service) PopulateInput(ctx context.Context, aComponent *repository.Component, request *http.Request, inputPtr interface{}) error {
	aSession := s.NewComponentSession(aComponent, WithRequest(request))
	inputValue := reflect.ValueOf(inputPtr)
	inputType := inputValue.Type()
	if inputValue.Type().Kind() != reflect.Ptr {
		return fmt.Errorf("expected input pointer, but had: %T", inputPtr)
	}
	aStateType := structology.NewStateType(inputType.Elem())
	aState := aStateType.NewState()
	opts := aSession.ViewOptions(aComponent.View, session.WithReportNotAssignable(false))
	err := aSession.SetState(ctx, aComponent.Input.Type.Parameters, aState, opts.Indirect(true))
	if err != nil {
		return err
	}
	inputValue.Elem().Set(reflect.ValueOf(aState.State()))
	return nil
}

// Read reads data from a view
func (s *Service) Read(ctx context.Context, locator string, dest interface{}, option ...reader.Option) error {
	aView, err := s.View(ctx, wrapWithMethod(http.MethodGet, locator))
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
func (s *Service) Component(ctx context.Context, name string, opts ...repository.Option) (*repository.Component, error) {
	method := http.MethodGet
	if index := strings.Index(name, ":"); index != -1 {
		method = strings.ToUpper(name[:index])
		name = name[index+1:]
	}
	aPath := contract.NewPath(method, name)
	component, err := s.repository.Registry().Lookup(ctx, aPath, opts...)
	if component != nil {
		return component, err
	}
	aPath = contract.NewPath(method, internalPath(name))
	if component, _ = s.repository.Registry().Lookup(ctx, aPath, opts...); component != nil {
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
	s.repository.Register(component)
	return component, nil
}

func (s *Service) buildDefaultComponents(ctx context.Context) (*repository.Components, string) {
	options := append(s.options, repository.WithResources(s.repository.Resources()))
	components := repository.NewComponents(ctx, options...)
	s.resource.Parameters = s.repository.Constants()
	components.Resource.MergeFrom(s.resource, s.repository.Extensions().Types)
	refConnector := ""
	if len(s.resource.Connectors) > 0 {
		refConnector = s.resource.Connectors[0].Name
	}
	return components, refConnector
}

// AddComponent adds components to repository
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
	aView := component.View
	if res := aView.GetResource(); res != nil {
		components.Resource = res
	}
	if aView.Name != "" { //swap with ref view
		components.Resource.Views = append(components.Resource.Views, aView)
		component.View = view.NewRefView(aView.Name)
	}
	components.Components = append(components.Components, component)
	if err := components.Init(ctx); err != nil {
		return err
	}

	s.repository.Register(components.Components...)

	return nil
}

// AddComponents adds components to repository
func (s *Service) AddComponents(ctx context.Context, components *repository.Components) error {
	if err := components.Init(ctx); err != nil {
		return err
	}
	s.repository.Register(components.Components...)
	return nil
}

// AddHandler adds handler component to repository
func (s *Service) AddHandler(ctx context.Context, aPath *contract.Path, handler xhandler.Handler, options ...repository.ComponentOption) (*repository.Component, error) {
	options = append([]repository.ComponentOption{repository.WithHandler(handler)}, options...)
	component, err := repository.NewComponent(aPath, options...)
	if err != nil {
		return nil, err
	}
	if component.View.Name == "" {
		rType := reflect.TypeOf(handler)
		if rType.Kind() == reflect.Ptr {
			rType = rType.Elem()
		}
		component.View.Name = rType.Name()
	}
	component.View.Mode = view.ModeHandler
	err = s.AddComponent(ctx, component)
	return component, err
}

// Resource returns resource
func (s *Service) Resource() *view.Resource {
	return s.resource
}

// Resource returns resource
func (s *Service) Resources() repository.Resources {
	return s.repository.Resources()
}

// AddResource adds named resource
func (s *Service) AddResource(name string, resource *view.Resource) {
	s.repository.Resources().AddResource(name, resource)
}

// AddConnectors adds connectors
func (s *Service) AddConnectors(ctx context.Context, connectors ...*view.Connector) error {
	connectionResource, err := s.repository.Resources().Lookup(view.ResourceConnectors)
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

// AddMBusResources adds message bus resources
func (s *Service) AddMBusResources(ctx context.Context, resource ...*mbus.Resource) error {
	mBusResources, _ := s.repository.Resources().Lookup(view.ResourceMBus)
	registerInResource := view.MessageBuses{}
	if mBusResources != nil {
		registerInResource = view.MessageBusSlice(mBusResources.MessageBuses).Index()
	}
	registerInService := view.MessageBusSlice(s.resource.MessageBuses).Index()
	for _, mResource := range resource {
		if _, ok := registerInService[mResource.Name]; !ok {
			s.resource.MessageBuses = append(s.resource.MessageBuses, mResource)
			continue
		}
		if _, ok := registerInResource[mResource.Name]; !ok && mBusResources != nil {
			mBusResources.MessageBuses = append(mBusResources.MessageBuses, mResource)
			continue
		}
	}
	return nil
}

// BuildPredicates added build predicate method
func (s *Service) BuildPredicates(ctx context.Context, expression string, input interface{}, baseView *view.View) (*codec.Criteria, error) {
	opts := &codec.CriteriaBuilderOptions{
		Expression: expression,
	}
	ctx = baseView.Context(ctx)
	return s.reader.BuildCriteria(ctx, input, opts)
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
		repository.WithResources(s.repository.Resources()),
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

func (s *Service) HTTPHandler(ctx context.Context, options ...gateway.Option) (http.Handler, error) {
	if s.handler != nil {
		return s.handler, nil
	}
	var err error
	options = append(options, gateway.WithRepository(s.repository))
	s.handler, err = gateway.New(ctx, options...)
	if err != nil {
		return nil, err
	}
	return s.handler, nil
}

// New creates a datly service, repository allows you to bootstrap empty or existing yaml repository
func New(ctx context.Context, options ...repository.Option) (*Service, error) {
	options = append([]repository.Option{
		repository.WithJWTSigner(mock.HmacJwtSigner()),
		repository.WithJWTVerifier(mock.HmacJwtVerifier()),
		repository.WithDispatcher(dispatcher.New),
	}, options...)
	aRepository, err := repository.New(ctx, options...)
	if err != nil {
		return nil, err
	}

	var verifier *verifier2.Service
	if verifier = aRepository.JWTVerifier(); verifier != nil {
		codecs := aRepository.Extensions().Codecs
		codecs.RegisterInstance(
			extension.CodecKeyJwtClaim, sjwt.New(verifier.VerifyClaims), time.Time{},
		)
	}

	if authService := aRepository.AuthService(); authService != nil {
		codecs := aRepository.Extensions().Codecs
		srv := dcodec.NewCustomAuth(authService)
		codecs.RegisterFactory(
			dcodec.KeyCustomAuth, srv, time.Time{},
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
