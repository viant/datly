package handler

import (
	"github.com/viant/datly/service/executor/expand"
	"reflect"
)

type execution struct {
	tableName string
	rType     reflect.Type
	insert    []interface{}
	update    []interface{}
	delete    []interface{}
}

func (e *execution) populate(dest *[]interface{}, isLast bool) {
	if len(e.insert) > 0 {
		exec := expand.Executable{Table: e.tableName, IsLast: isLast, ExecType: expand.ExecTypeInsert, Data: e.asTypedSlice(e.insert)}

		*dest = append(*dest, &exec)
	}
	if len(e.update) > 0 {
		exec := expand.Executable{Table: e.tableName, IsLast: isLast, ExecType: expand.ExecTypeUpdate, Data: e.asTypedSlice(e.update)}
		*dest = append(*dest, &exec)
	}
	if len(e.delete) > 0 {
		exec := expand.Executable{Table: e.tableName, IsLast: isLast, ExecType: expand.ExecTypeDelete, Data: e.asTypedSlice(e.delete)}
		*dest = append(*dest, &exec)
	}
}

func (e *execution) asTypedSlice(items []interface{}) interface{} {
	var slice = reflect.MakeSlice(reflect.SliceOf(e.rType), 0, len(items))
	for _, item := range items {
		value := reflect.ValueOf(item)
		if value.Kind() == reflect.Slice {
			slice = reflect.AppendSlice(slice, value)
			continue
		}
		slice = reflect.Append(slice, value)
	}
	ret := slice.Interface()
	return ret
}

type executions struct {
	executions []*execution
	byTable    map[string]int
}

func (t *executions) add(tableName string, data interface{}, execType expand.ExecType) {
	idx, ok := t.byTable[tableName]
	if !ok {
		idx = len(t.executions)
		t.executions = append(t.executions, &execution{
			tableName: tableName,
		})
		t.byTable[tableName] = idx
	}
	if t.executions[idx].rType == nil {
		rType := reflect.TypeOf(data)
		if rType.Kind() == reflect.Slice {
			rType = rType.Elem()
		}
		t.executions[idx].rType = rType
	}
	switch execType {
	case expand.ExecTypeInsert:
		t.executions[idx].insert = append(t.executions[idx].insert, data)
	case expand.ExecTypeUpdate:
		t.executions[idx].update = append(t.executions[idx].update, data)
	case expand.ExecTypeDelete:
		t.executions[idx].delete = append(t.executions[idx].delete, data)
	}
}

func newExecutions() *executions {
	return &executions{
		byTable: map[string]int{},
	}
}

func newSqlxIterator(toExecute []interface{}) *sqlxIterator {
	var items []interface{}
	exec := newExecutions()
	for _, item := range toExecute {

		switch actual := item.(type) {
		case *expand.Executable:
			if actual.Executed() {
				continue
			}
			exec.add(actual.Table, actual.Data, actual.ExecType)
			continue
		}
		items = append(items, item)
	}

	if len(exec.executions) > 0 {
		for i, execution := range exec.executions {
			execution.populate(&items, len(exec.executions)-1 == i)
		}
	}
	return &sqlxIterator{
		toExecute: items,
	}
}
