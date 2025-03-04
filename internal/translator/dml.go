package translator

import (
	"context"
	"github.com/viant/datly/internal/inference"
)

func (s *Service) extractDMLTables(ctx context.Context, resource *Resource) (*inference.Table, error) {
	db, err := s.Repository.LookupDb(s.DefaultConnector(resource.rule))
	if err != nil {
		return nil, err
	}
	if resource.RawSQL == "" {
		return nil, nil
	}
	var table *inference.Table
	tables := resource.Statements.DMLTables(resource.RawSQL)
	if len(tables) > 0 {
		table = &inference.Table{Name: tables[0], Namespace: tables[0]}
		if err := table.Detect(ctx, db); err != nil {
			return nil, err
		}
		for i := 1; i < len(tables); i++ {
			extraTable := &inference.Table{Name: tables[i], Namespace: tables[i]}
			if err := extraTable.Detect(ctx, db); err != nil {
				return nil, err
			}
			table.AppendTable(extraTable)
		}
	}
	return table, nil
}
