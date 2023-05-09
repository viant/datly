package view

import (
	"context"
	"fmt"
	"github.com/viant/datly/utils/types"
	"github.com/viant/structql"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
)

type Qualifier struct {
	Value     string
	Column    string
	Parameter *Parameter

	_accessor     *types.Accessor
	_query        *structql.Query
	_resultSlice  *xunsafe.Slice
	_xField       *xunsafe.Field
	_derefCounter int
	initialized   bool
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

		q._resultSlice = xunsafe.NewSlice(q._query.Type())
		queryType := q._query.Type().Elem()
		for queryType.Kind() == reflect.Ptr {
			q._derefCounter++
			queryType = queryType.Elem()
		}

		q._xField = xunsafe.FieldByIndex(queryType, 0)
	}

	return nil
}

func (q *Qualifier) ExtractValues(value interface{}) ([]interface{}, error) {
	selected, err := q._query.Select(value)
	if err != nil {
		return nil, err
	}

	pointer := xunsafe.AsPointer(selected)
	if pointer == nil {
		return []interface{}{}, nil
	}

	size := q._resultSlice.Len(pointer)
	result := make([]interface{}, 0, size)
	for i := 0; i < size; i++ {
		valuePtr := q._resultSlice.PointerAt(pointer, uintptr(i))
		for j := 0; j < q._derefCounter && valuePtr != nil; j++ {
			valuePtr = xunsafe.DerefPointer(valuePtr)
		}

		if valuePtr == nil {
			result = append(result, reflect.New(q._xField.Type).Elem().Interface())
		} else {
			fieldValue := q._xField.Value(valuePtr)
			result = append(result, fieldValue)
		}
	}

	return result, nil
}
