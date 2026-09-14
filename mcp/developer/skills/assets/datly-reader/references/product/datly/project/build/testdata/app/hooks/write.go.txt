package hooks

import (
	"context"
	models "example.com/buildmodel"
	"fmt"
	h "github.com/viant/xdatly/handler"
	"os"
)

type Input struct {
	Record      *models.Record `parameter:"Record,kind=body,in=data,required"`
	Initialized bool           `json:"-"`
}

func (i *Input) Init(context.Context) error { i.Initialized = true; return nil }

type Output struct {
	Data      *models.Record `json:"data"`
	Finalized bool           `json:"finalized"`
}

func (o *Output) Finalize(context.Context) error { o.Finalized = true; return nil }

type writer struct{}

func NewWrite() h.Contract[Input, Output] {
	if os.Getenv("DATLY_FACTORY_DISCOVERY_SENTINEL") != "" {
		panic("factory executed during discovery")
	}
	return &writer{}
}
func (*writer) Exec(ctx context.Context, s h.Session, i *Input, o *Output) error {
	if !i.Initialized {
		return fmt.Errorf("lifecycle missing")
	}
	value, found, err := s.Binder().Lookup(ctx, h.DMLKey)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("DML missing")
	}
	if err = value.(h.DML).Insert("records", i.Record); err != nil {
		return err
	}
	o.Data = i.Record
	return nil
}
