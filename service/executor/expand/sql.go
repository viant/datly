package expand

import (
	"github.com/google/uuid"
	"strings"
	"sync"
)

type (
	Statements struct {
		Executable []interface{}
		Index      ExecutablesIndex
		Markers    map[string]int
		currIndex  int
		mu         sync.RWMutex
		onAppend   func(interface{})
	}

	SQLStatment struct {
		SQL      string
		Args     []interface{}
		executed bool
	}
)

// TableName identifies raw SQL as having no table-specific flush target.
func (s *SQLStatment) TableName() string { return "" }

func (s *SQLStatment) Executed() bool { return s != nil && s.executed }
func (s *SQLStatment) MarkAsExecuted() {
	if s != nil {
		s.executed = true
	}
}

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

func (s *Statements) appendWithMarker(tableName string, data interface{}, exectType ExecType) string {
	s.appendExecutable(tableName, data, exectType)
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

	s.append(executable, func() { s.Index.UpdateLastExecutable(execType, tableName, executable) })
}

func (s *Statements) Delete(name string, data interface{}) {
	s.appendExecutable(name, data, ExecTypeDelete)
}

func (s *Statements) Execute(SQLStmt *SQLStatment) {
	s.append(SQLStmt, nil)
}

// SetAppendObserver observes newly buffered statements in authored order.
func (s *Statements) SetAppendObserver(observer func(interface{})) {
	s.mu.Lock()
	s.onAppend = observer
	s.mu.Unlock()
}

// Snapshot returns a stable copy of all statements.
func (s *Statements) Snapshot() []interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]interface{}(nil), s.Executable...)
}

func (s *Statements) append(value interface{}, updateIndex func()) {
	s.mu.Lock()
	if updateIndex != nil {
		updateIndex()
	}
	s.Executable = append(s.Executable, value)
	observer := s.onAppend
	s.mu.Unlock()
	if observer != nil {
		observer(value)
	}
}

func (s *Statements) generateMarker() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	marker := uuid.New().String()
	s.Markers[marker] = len(s.Executable) - 1
	return marker
}

func (s *Statements) LookupExecutable(sql string) (*Executable, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sql = strings.TrimSpace(sql)
	i, ok := s.Markers[sql]
	if !ok {
		return nil, false
	}

	asExecutable, ok := s.Executable[i].(*Executable)
	return asExecutable, ok
}

func (s *Statements) FilterByTableName(name string) []interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
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

// CausalPrefixByTableName returns all pending authored statements through the
// last pending mutation of name. Raw SQL and other-table mutations before it
// are causal predecessors and therefore cannot be skipped.
func (s *Statements) CausalPrefixByTableName(name string) []interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if name == "" {
		result := make([]interface{}, 0, len(s.Executable))
		for _, candidate := range s.Executable {
			switch actual := candidate.(type) {
			case *Executable:
				if !actual.Executed() {
					result = append(result, actual)
				}
			case *SQLStatment:
				if !actual.Executed() {
					result = append(result, actual)
				}
			}
		}
		return result
	}
	last := -1
	for i, candidate := range s.Executable {
		if executable, ok := candidate.(*Executable); ok && !executable.Executed() && strings.EqualFold(executable.Table, name) {
			last = i
		}
	}
	if last < 0 {
		return nil
	}
	result := make([]interface{}, 0, last+1)
	for _, candidate := range s.Executable[:last+1] {
		switch actual := candidate.(type) {
		case *Executable:
			if !actual.Executed() {
				result = append(result, actual)
			}
		case *SQLStatment:
			if !actual.Executed() {
				result = append(result, actual)
			}
		}
	}
	return result
}

func (s *Statements) NextNonExecuted() (*Executable, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
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
