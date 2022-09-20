package gateway

import (
	"fmt"
	"github.com/viant/datly/router"
	"github.com/viant/datly/view"
	"sync"
)

type Session struct {
	mux                 sync.Mutex
	FailureCounter      map[string]int
	DeletedDependencies map[string]bool
	UpdatedDependencies map[string]bool
	DeletedRouters      map[string]bool
	UpdatedRouters      map[string]bool
	Routers             map[string]*router.Resource
	Dependencies        map[string]*view.Resource
	config              *ChangeDetection
}

func NewSession(config *ChangeDetection) *Session {
	return &Session{
		FailureCounter:      map[string]int{},
		DeletedDependencies: map[string]bool{},
		UpdatedDependencies: map[string]bool{},
		DeletedRouters:      map[string]bool{},
		UpdatedRouters:      map[string]bool{},
		Routers:             map[string]*router.Resource{},
		config:              config,
	}
}

func (s *Session) OnDependencyUpdated(URLs ...string) {
	s.mux.Lock()
	defer s.mux.Unlock()

	for _, URL := range URLs {
		s.removeEach(URL)
		s.UpdatedDependencies[URL] = true
	}
}

func (s *Session) OnDependencyDeleted(URLs ...string) {
	s.mux.Lock()
	defer s.mux.Unlock()
	for _, URL := range URLs {
		s.removeEach(URL)
		s.DeletedDependencies[URL] = true
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
	delete(s.DeletedDependencies, URL)
	delete(s.UpdatedDependencies, URL)
	delete(s.DeletedRouters, URL)
	delete(s.UpdatedRouters, URL)
	delete(s.FailureCounter, URL)
	delete(s.Routers, URL)
	delete(s.Dependencies, URL)
}

func (s *Session) OnRouterUpdated(URLs ...string) {
	s.mux.Lock()
	defer s.mux.Unlock()

	for _, URL := range URLs {
		s.removeEach(URL)
		s.UpdatedRouters[URL] = true
	}
}

func (s *Session) OnRouterDeleted(URLs ...string) {
	s.mux.Lock()
	defer s.mux.Unlock()

	for _, URL := range URLs {
		s.removeEach(URL)
		s.DeletedRouters[URL] = true
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
