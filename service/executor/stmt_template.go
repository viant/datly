package executor

import "github.com/viant/datly/template/expand"

type TemplateStmtIterator struct {
	DataUnit *expand.DataUnit
	Data     []*expand.SQLStatment

	dataIndex      int
	exhaustedData  bool
	nextExecutable interface{}
}

func NewTemplateStmtIterator(dataUnit *expand.DataUnit, data []*expand.SQLStatment) *TemplateStmtIterator {
	return &TemplateStmtIterator{
		DataUnit: dataUnit,
		Data:     data,
	}
}

func (t *TemplateStmtIterator) HasNext() bool {
	t.exhaustedData = !(t.dataIndex < len(t.Data))
	if !t.exhaustedData {
		actualIndex := t.dataIndex
		t.dataIndex++

		statment := t.Data[actualIndex]
		if executable, ok := t.DataUnit.IsServiceExec(statment.SQL); ok {
			t.nextExecutable = executable
		} else {
			t.nextExecutable = statment
		}

		return true
	}

	executable, ok := t.DataUnit.Statements.NextNonExecuted()
	if ok {
		t.nextExecutable = executable
		return true
	}

	return false
}

func (t *TemplateStmtIterator) Next() interface{} {
	return t.nextExecutable
}

func (t *TemplateStmtIterator) HasAny() bool {
	return len(t.Data) > 0 || len(t.DataUnit.Statements.Executable) > 0
}

func (t *TemplateStmtIterator) canBeBatchedGlobally(criteria *expand.DataUnit, data []*expand.SQLStatment) bool {
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
