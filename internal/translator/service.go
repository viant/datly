package translator

import (
	"context"
	"fmt"
	"github.com/viant/datly/internal/translator/parser"
)

type Translator struct {
	Repository *Repository //TODO init repo with basic config and dependencies
}

func (t *Translator) Translate(ctx context.Context, dSQL string) error {
	resource := NewResource()
	if err := resource.ExtractRouterOptions(&dSQL); err != nil {
		return err
	}

	if err := resource.ExtractExplicitParameter(&dSQL); err != nil {
		return err
	}

	statements := parser.NewStatements(dSQL)
	if len(statements) == 0 {
		return fmt.Errorf("invalid dSQL") //TODO what if handler
	}
	if statements.IsExec() {
		//TODO process exec

		return nil
	}

	querySQL := dSQL[statements[0].Start:]
	namespaes, err := NewNamespaces(querySQL)
	fmt.Printf("%v %v\n", namespaes, err)
	return err
}
