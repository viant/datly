package executor

import "github.com/viant/datly/template/expand"

type TemplateStmtIterator struct {
	DataUnit *expand.DataUnit
	Data     []*SQLStatment
	index    int
}

func NewTemplateStmtIterator(dataUnit *expand.DataUnit, data []*SQLStatment) *TemplateStmtIterator {
	return &TemplateStmtIterator{
		DataUnit: dataUnit,
		Data:     data,
	}
}

func (t *TemplateStmtIterator) HasNext() bool {
	return t.index < len(t.Data)
}

func (t *TemplateStmtIterator) Next() interface{} {
	actualIndex := t.index
	t.index++

	statment := t.Data[actualIndex]
	if executable, ok := t.DataUnit.IsServiceExec(statment.SQL); ok {
		return executable
	}

	return statment
}

func (t *TemplateStmtIterator) HasAny() bool {
	return len(t.Data) > 0
}

func (t *TemplateStmtIterator) canBeBatchedGlobally(criteria *expand.DataUnit, data []*SQLStatment) bool {
	executables := criteria.FilterExecutables(extractStatements(data), true)
	if len(executables) != len(data) {
		return false
	}

	tableNamesIndex := map[string]bool{}
	for _, executable := range executables {
		if executable.ExecType == expand.ExecTypeUpdate {
			return false
		}

		tableNamesIndex[executable.Table] = true
	}

	return len(tableNamesIndex) == 1
}
