package data

import (
	"database/sql"
	"fmt"
	"github.com/viant/datly/v1/config"
	"github.com/viant/datly/v1/utils"
	"sync"
)

type ViewVisitor = func(view *View) bool
type ReferenceVisitor = func(reference *Reference) bool

type Service struct {
	views  []*View
	_views map[string]*View

	connectors  []*config.Connector
	_connectors map[string]*config.Connector

	references  []*Reference
	_references map[string]*Reference
}

func (s *Service) Connection(connectorName string) (*sql.DB, error) {
	if connector, ok := s._connectors[connectorName]; ok {
		return sql.Open(connector.Driver, connector.DSN)
	}
	return nil, fmt.Errorf("not found connector with name: %v", connectorName)
}

func (s *Service) IsViewRegistered(view *View) error {
	var ok bool
	if _, ok = s._views[view.Name]; !ok {
		return fmt.Errorf("view with name %v not found", view.Name)
	}
	if _, ok = s._connectors[view.Connector]; !ok {
		return fmt.Errorf("connector with name %v not found", view.Connector)
	}
	return nil
}

func (s *Service) View(view string) (*View, bool) {
	result := s._views[view]
	return result, result != nil
}

func Configure(resource *Resource) (*Service, error) {
	connectors := make(map[string]*config.Connector)
	connectorVisitor := func(connector *config.Connector) {
		connectors[connector.Name] = connector
	}
	forEachConnector(resource.Connectors, connectorVisitor)

	references := make(map[string]*Reference)
	views := make(map[string]*View)

	// Running it twice, because view can have relation to reference by name, and reference can have relation to view
	for i := 1; i <= 2; i++ {
		forEachReference(resource.References, func(reference *Reference) bool {
			references[reference.Name] = reference
			if reference.ChildName != "" {
				reference.Child = views[reference.ChildName]
			}
			return true
		})

		forEachView(resource.Views, func(view *View) bool {
			if view == nil {
				return true
			}

			views[view.Name] = view
			if len(view.RefNames) != 0 {
				view.References = make([]*Reference, len(view.RefNames))
				for refNameIndex := range view.RefNames {
					view.References[refNameIndex] = references[view.RefNames[refNameIndex]]
				}
			}
			return true
		})
	}

	wg := sync.WaitGroup{}
	errors := utils.NewErrors(0)
	forEachView(resource.Views, func(view *View) bool {
		view.connector = connectors[view.Connector]
		if view.connector == nil {
			errors.Append(fmt.Errorf("not found connector %v for view %v", view.Connector, view.Name))
			return false
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := view.init(); err != nil {
				errors.Append(err)
			}
		}()
		return true
	})
	wg.Wait()

	if errors.Error() != nil {
		return nil, errors.Error()
	}

	return &Service{
		views:       resource.Views,
		_views:      views,
		connectors:  resource.Connectors,
		_connectors: connectors,
		references:  resource.References,
		_references: references,
	}, nil
}

func forEachReference(references []*Reference, referenceVisitor ReferenceVisitor) bool {
	for i := range references {
		if !referenceVisitor(references[i]) {
			return false
		}

		if references[i].Child == nil {
			continue
		}

		if !forEachReference(references[i].Child.References, referenceVisitor) {
			return false
		}
	}
	return true
}

func forEachView(views []*View, viewVisitor ViewVisitor) bool {
	for i := range views {
		if !forEachReferenceView(views[i], viewVisitor, nil) {
			return false
		}
	}
	return true
}

func forEachReferenceView(view *View, viewVisitor ViewVisitor, referenceVisitor ReferenceVisitor) bool {
	if viewVisitor != nil {
		viewVisitor(view)
	}

	if view == nil {
		return true
	}

	for i := range view.References {
		if referenceVisitor != nil {
			if !referenceVisitor(view.References[i]) {
				return false
			}
		}

		if viewVisitor != nil {
			if view.References[i] == nil {
				continue
			}

			if !viewVisitor(view.References[i].Child) {
				return false
			}
		}
		forEachReferenceView(view.References[i].Child, viewVisitor, referenceVisitor)
	}
	return true
}

func forEachConnector(connectors []*config.Connector, visitor func(connector *config.Connector)) {
	for i := range connectors {
		visitor(connectors[i])
	}
}
