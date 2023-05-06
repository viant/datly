package view

import (
	"context"
	"fmt"
	"github.com/viant/datly/utils/types"
	"github.com/viant/structql"
	"strings"
)

type Qualifier struct {
	Value     string
	Column    string
	Parameter *Parameter

	_accessor   *types.Accessor
	_query      *structql.Query
	initialized bool
}

func (q *Qualifier) Init(ctx context.Context, resource *Resource, view *View, columns ColumnIndex) error {
	if q.initialized {
		return nil
	}
	q.initialized = true

	if q.Value == "" {
		return fmt.Errorf("qualifier Value can't be empty")
	}

	if q.Column == "" {
		return fmt.Errorf("qualifier Column can't be empty")
	}

	if _, err := columns.ColumnName(q.Column); err != nil {
		return err
	}

	if err := q.ensureParam(ctx, resource, view); err != nil {
		return err
	}

	return nil
}

func (q *Qualifier) ensureParam(ctx context.Context, resource *Resource, view *View) error {
	var aPath string
	split := strings.Split(q.Value, ".")

	if q.Parameter == nil {
		if len(split) == 0 {
			return fmt.Errorf("parameter or parameter qualifier has to be specified")
		}

		param, err := resource.ParamByName(split[0])
		if err != nil {
			return err
		}

		q.Parameter = param
		if len(split) > 1 {
			aPath = strings.Join(split[1:], ".")
			split = split[1:]
		}
	} else {
		aPath = q.Value
	}

	if err := q.Parameter.Init(ctx, view, resource, nil); err != nil {
		return err
	}

	if aPath == "" {
		return nil
	}

	accessors := types.NewAccessors(&types.VeltyNamer{})
	accessors.InitPath(q.Parameter.ActualParamType(), aPath)
	accessor, err := accessors.AccessorByName(aPath)
	if err != nil {
		return err
	}

	q._accessor = accessor
	q.Parameter.Required = boolPtr(true)

	if len(split) >= 1 {
		src := strings.Join(split[:len(split)-1], "/")

		aQuery := fmt.Sprintf("SELECT %v FROM `/%v`", split[len(split)-1], src)
		q._query, err = structql.NewQuery(aQuery, q.Parameter.ActualParamType(), nil)
		if err != nil {
			return err
		}
	}

	return nil
}

//TODO: extract values from Query, but currently SELECT * is not supported
//TODO: remove newer, replace just with new.
func (q *Qualifier) ExtractValues(value interface{}) {

}
