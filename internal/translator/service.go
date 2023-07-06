package translator

import (
	"context"
	"github.com/viant/sqlparser"
)

type Translator struct {
	Repository *Repository //TODO init repo with basic config and dependencies
}

func (t *Translator) Translate(ctx context.Context, dSQL string) error {
	resource := NewResource()
	if err := resource.InitRule(&dSQL); err != nil {
		return err
	}
	if err := resource.ExtractDeclared(&dSQL); err != nil {
		return err
	}

	if !resource.IsExec() {
		if err := t.translateQuery(ctx, resource, dSQL); err != nil {
			return err
		}
	}

	return nil
}

func (t *Translator) translateQuery(ctx context.Context, resource *Resource, dSQL string) error {
	query, err := sqlparser.ParseQuery(dSQL)
	if err != nil {
		return err
	}
	resource.Rule.Root = query.From.Alias
	if err = resource.Rule.Namespaces.Init(query); err != nil {
		return err
	}
	//TODO
	return nil
}
