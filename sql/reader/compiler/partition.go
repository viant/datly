package compiler

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/data"
	xreader "github.com/viant/xdatly/reader"
)

func buildPartitioners(root *data.View, rowTypes map[*data.View]reflect.Type, lookup func(string) (reflect.Type, error)) (map[*data.View]xreader.Partitioner, error) {
	result := map[*data.View]xreader.Partitioner{}
	visited := map[*data.View]bool{}
	var walk func(*data.View) error
	walk = func(view *data.View) error {
		if view == nil || visited[view] {
			return nil
		}
		visited[view] = true
		if view.Spec.Partitioning != nil {
			partitioner, err := newPartitioner(view, rowTypes[view], lookup)
			if err != nil {
				return err
			}
			result[view] = partitioner
		}
		for _, relation := range view.Relations {
			if relation != nil && relation.Of != nil {
				if err := walk(relation.Of.View); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := walk(root); err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

func newPartitioner(view *data.View, rowType reflect.Type, lookup func(string) (reflect.Type, error)) (xreader.Partitioner, error) {
	typeName := strings.TrimSpace(view.Spec.Partitioning.Type)
	partitionerType := rowType
	if typeName != "" && typeName != "." {
		if lookup == nil {
			return nil, fmt.Errorf("resolve partitioner %s for view %s: type lookup is required", typeName, view.Spec.Name)
		}
		resolved, err := lookup(typeName)
		if err != nil {
			return nil, fmt.Errorf("resolve partitioner %s for view %s: %w", typeName, view.Spec.Name, err)
		}
		partitionerType = resolved
	}
	if partitionerType == nil {
		return nil, fmt.Errorf("partitioner type is required for view %s", view.Spec.Name)
	}
	for partitionerType.Kind() == reflect.Ptr {
		partitionerType = partitionerType.Elem()
	}
	partitioner, ok := reflect.New(partitionerType).Interface().(xreader.Partitioner)
	if !ok {
		return nil, fmt.Errorf("partitioner %s for view %s does not implement reader.Partitioner", partitionerType, view.Spec.Name)
	}
	return partitioner, nil
}
