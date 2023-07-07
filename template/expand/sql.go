package expand

import (
	"github.com/google/uuid"
	"strings"
)

type (
	Statements struct {
		Executable []interface{}
		Index      ExecutablesIndex
		Markers    map[string]int
		currIndex  int
	}

	SQLStatment struct {
		SQL  string
		Args []interface{}
	}
)

func NewStmtHolder() *Statements {
	return &Statements{
		Executable: nil,
		Index:      map[string]*Executable{},
		Markers:    map[string]int{},
	}
}

func (s *Statements) Insert(tableName string, data interface{}) {
	s.appendExecutable(tableName, data, ExecTypeInsert)
}

func (s *Statements) InsertWithMarker(tableName string, data interface{}) string {
	return s.appendWithMarker(tableName, data, ExecTypeInsert)
}

func (s *Statements) UpdateWithMarker(tableName string, data interface{}) string {
	return s.appendWithMarker(tableName, data, ExecTypeUpdate)
}

func (s *Statements) DeleteWithMarker(tableName string, data interface{}) string {
	return s.appendWithMarker(tableName, data, ExecTypeDelete)
}

func (s *Statements) appendWithMarker(tableName string, data interface{}, insert ExecType) string {
	s.appendExecutable(tableName, data, insert)
	return s.generateMarker()
}

func (s *Statements) Update(tableName string, data interface{}) {
	s.appendExecutable(tableName, data, ExecTypeUpdate)
}

func (s *Statements) appendExecutable(tableName string, data interface{}, execType ExecType) {
	executable := &Executable{
		Table:    tableName,
		ExecType: execType,
		Data:     copyValue(data),
		IsLast:   true,
	}

	s.Index.UpdateLastExecutable(execType, tableName, executable)
	s.Executable = append(s.Executable, executable)
}

func (s *Statements) Delete(name string, data interface{}) {
	s.appendExecutable(name, data, ExecTypeDelete)
}

func (s *Statements) Execute(SQLStmt *SQLStatment) {
	s.Executable = append(s.Executable, SQLStmt)
}

func (s *Statements) generateMarker() string {
	marker := uuid.New().String()
	s.Markers[marker] = len(s.Executable) - 1
	return marker
}

func (s *Statements) LookupExecutable(sql string) (*Executable, bool) {
	sql = strings.TrimSpace(sql)
	i, ok := s.Markers[sql]
	if !ok {
		return nil, false
	}

	asExecutable, ok := s.Executable[i].(*Executable)
	return asExecutable, ok
}

func (s *Statements) FilterByTableName(name string) []interface{} {
	var result []interface{}
	for _, executable := range s.Executable {
		switch actual := executable.(type) {
		case *Executable:
			if strings.EqualFold(actual.Table, name) {
				result = append(result, actual)
			}
		}
	}

	return result
}

func (s *Statements) NextNonExecuted() (*Executable, bool) {
	if s.currIndex >= len(s.Executable) {
		return nil, false
	}

	for index, value := range s.Executable[s.currIndex:] {
		asExec, ok := value.(*Executable)
		if ok && !asExec.Executed() {
			s.currIndex += index + 1
			return asExec, true
		}
	}

	return nil, false
}
