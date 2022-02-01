package meta

import (
	"database/sql"
	"fmt"
	"github.com/viant/datly/v1/config"
	"github.com/viant/datly/v1/data"
	"github.com/viant/datly/v1/utils"
	"sync"
)

type Service struct {
	views  []*data.View
	_views map[string]*data.View

	relations  []*data.Relation
	_relations map[string]*data.Relation

	connectors  []*config.Connector
	_connectors map[string]*config.Connector

	references  []*data.Reference
	_references map[string]*data.Reference
}

func (s *Service) Connection(connectorName string) (*sql.DB, error) {
	if connector, ok := s._connectors[connectorName]; ok {
		return sql.Open(connector.Driver, connector.DSN)
	}
	return nil, fmt.Errorf("not found connector with name: %v", connectorName)
}

func (s *Service) IsViewRegistered(view *data.View) error {
	var ok bool
	if _, ok = s._views[view.Name]; !ok {
		return fmt.Errorf("view with name %v not found", view.Name)
	}
	if _, ok = s._connectors[view.Connector]; !ok {
		return fmt.Errorf("connector with name %v not found", view.Connector)
	}
	return nil
}

func Configure(connectors []*config.Connector, views []*data.View, relations []*data.Relation, references []*data.Reference) (*Service, error) {
	referencesAsMap := mapifyReferences(references)
	viewsAsMap := mapifyViews(views)
	err := ensureRelations(relations, viewsAsMap, referencesAsMap)
	if err != nil {
		return nil, err
	}
	relationsAsMap := mapifyRelations(relations)
	connectorsAsMap := mapifyConnectors(connectors)

	err = ensureViewColumns(views, connectorsAsMap)
	if err != nil {
		return nil, err
	}

	return &Service{
		views:       views,
		_views:      viewsAsMap,
		relations:   relations,
		_relations:  relationsAsMap,
		connectors:  connectors,
		_connectors: connectorsAsMap,
		references:  references,
		_references: referencesAsMap,
	}, nil
}

func ensureViewColumns(views []*data.View, connectors map[string]*config.Connector) error {
	wg := sync.WaitGroup{}
	viewsSize := len(views)
	wg.Add(viewsSize)
	errors := utils.NewErrors(viewsSize)

	for i := range views {
		iCoppy := i
		go func() {
			defer wg.Done()
			connectorName := views[iCoppy].Connector
			if connectorName == "" {
				errors.AddError(fmt.Errorf("view %v has no specified connector", views[iCoppy].Name), iCoppy)
				return
			}
			db, err := sql.Open(connectors[connectorName].Driver, connectors[connectorName].DSN)
			if err != nil {
				errors.AddError(err, iCoppy)
			}

			errors.AddError(views[iCoppy].EnsureColumns(db), iCoppy)
		}()
	}
	wg.Wait()
	return errors.Error()
}

func mapifyConnectors(connectors []*config.Connector) map[string]*config.Connector {
	result := make(map[string]*config.Connector)
	for i := range connectors {
		result[connectors[i].Name] = connectors[i]
	}
	return result
}

func ensureRelations(relations []*data.Relation, views map[string]*data.View, references map[string]*data.Reference) error {
	for i := range relations {
		if relations[i].Ref == nil {
			relations[i].Ref = references[relations[i].RefId]
		}

		if relations[i].Ref == nil {
			return fmt.Errorf("coulnd't find reference for relation %v", relations[i].Name)
		}
	}

	for i := range relations {
		if relations[i].Child == nil {
			relations[i].Child = views[relations[i].ChildName]
		}

		if relations[i].Child == nil {
			return fmt.Errorf("coulnd't find child view for relation %v", relations[i].Name)
		}
	}

	return nil
}

func mapifyViews(views []*data.View) map[string]*data.View {
	result := make(map[string]*data.View)
	for i := range views {
		result[views[i].Name] = views[i]
	}
	return result
}

func mapifyRelations(relations []*data.Relation) map[string]*data.Relation {
	result := make(map[string]*data.Relation)
	for i := range relations {
		result[relations[i].Name] = relations[i]
	}
	return result
}

func mapifyReferences(references []*data.Reference) map[string]*data.Reference {
	result := make(map[string]*data.Reference)
	for i := range references {
		result[references[i].Name] = references[i]
	}

	return result
}
