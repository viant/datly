package gateway

import (
	"fmt"
	"github.com/viant/datly/gateway/router"
	"github.com/viant/datly/view"
	"sync"
)

type Session struct {
	mux                 sync.Mutex
	FailureCounter      map[string]int
	dependenciesChanges *ExtIndex
	routerChanges       *RouterChanges
	Routers             map[string]*router.Resource
	Dependencies        map[string]*view.Resource
	config              *ChangeDetection
	LazyRoutes          map[string]*LazyRouterContract
}

func NewSession(config *ChangeDetection, routers map[string]*router.Router) *Session {
	return &Session{
		FailureCounter:      map[string]int{},
		routerChanges:       NewResourcesChange(routers),
		dependenciesChanges: NewExtIndex(),
		Routers:             map[string]*router.Resource{},
		config:              config,
		LazyRoutes:          map[string]*LazyRouterContract{},
	}
}

func (s *Session) OnDependencyUpdated(URLs ...string) {
	s.mux.Lock()
	defer s.mux.Unlock()

	for _, URL := range URLs {
		s.dependenciesChanges.Changed(URL)
	}
}

func (s *Session) OnDependencyDeleted(URLs ...string) {
	s.mux.Lock()
	defer s.mux.Unlock()
	for _, URL := range URLs {
		s.dependenciesChanges.deleted.RemoveEntry(URL)
	}
}

func (s *Session) OnFileChange(URLs ...string) {
	s.mux.Lock()
	defer s.mux.Unlock()
	for _, URL := range URLs {
		s.removeEach(URL)
	}
}

func (s *Session) removeEach(URL string) {
	delete(s.FailureCounter, URL)
	delete(s.Routers, URL)
	delete(s.Dependencies, URL)
}

func (s *Session) OnRouterUpdated(URLs ...string) {
	s.mux.Lock()
	defer s.mux.Unlock()

	for _, URL := range URLs {
		s.removeEach(URL)
		s.routerChanges.routersIndex.updated.MarkResolved(URL)
	}
}

func (s *Session) AddRouter(URL string, resource *router.Resource) {
	s.mux.Lock()
	defer s.mux.Unlock()

	s.Routers[URL] = resource
}

func (s *Session) UpdateFailureCounter() {
	for key, value := range s.FailureCounter {
		if value == s.config.NumOfRetries {
			fmt.Printf("[INFO] Failed to use resource %v %v times. Please check the validity of resource and try to reupload\n", key, value)
			s.removeEach(key)
			continue
		}

		s.FailureCounter[key] = value + 1
	}
}
