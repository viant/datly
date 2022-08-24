package logger

import (
	"fmt"
	"github.com/viant/datly/shared"
	"os"
	"time"
)

type (
	Adapters     []*Adapter
	AdapterIndex map[string]*Adapter

	Adapter struct {
		shared.Reference
		Name string

		readTime          ReadTime
		readingData       ReadingData
		objectReconciling ObjectReconciling
		columnsDetection  ColumnsDetection
		log               Log
	}
)

func (i AdapterIndex) Lookup(name string) (*Adapter, bool) {
	adapter, ok := i[name]
	return adapter, ok
}

func (i AdapterIndex) Register(adapter *Adapter) {
	i[adapter.Name] = adapter
}

func (a Adapters) Index() AdapterIndex {
	result := AdapterIndex{}
	for i := range a {
		result[a[i].Name] = a[i]
	}

	return result
}

func (l *Adapter) ColumnsDetection(sql, source string) {
	if l.columnsDetection == nil {
		return
	}

	l.columnsDetection(sql, source)
}

func (l *Adapter) ObjectReconciling(dst, item, parent interface{}, index int) {
	if l.objectReconciling == nil {
		return
	}

	l.objectReconciling(dst, item, parent, index)
}

func (l *Adapter) ReadingData(duration time.Duration, SQL string, read int, params []interface{}, err error) {
	if l.readingData == nil {
		return
	}

	l.readingData(duration, SQL, read, params, err)
}

func (l *Adapter) ReadTime(viewName string, start, end *time.Time, err error) {
	if l.readTime == nil {
		return
	}

	l.readTime(viewName, start, end, err)
}

func (l *Adapter) Inherit(adapter *Adapter) {
	l.readTime = adapter.readTime
	l.readingData = adapter.readingData
	l.objectReconciling = adapter.objectReconciling
	l.columnsDetection = adapter.columnsDetection
	l.log = adapter.log
}

func (l *Adapter) LogDatabaseErr(SQL string, err error) {
	fmt.Printf(fmt.Sprintf("error occured while fetching data from db: %v, SQL: %v\n", err, SQL))
}

func NewLogger(name string, logger Logger) *Adapter {
	if logger == nil {
		return &Adapter{
			Name: name,
		}
	}

	return &Adapter{
		Name:              name,
		Reference:         shared.Reference{},
		readTime:          logger.ViewReadTime(),
		readingData:       logger.ReadingData(),
		objectReconciling: logger.ObjectReconciling(),
		columnsDetection:  logger.ColumnsDetection(),
		log:               logger.Log(),
	}
}

func Default() *Adapter {
	if os.Getenv("DATLY_DEBUG") == "" {
		return NewLogger("", nil)
	}
	return NewLogger("", &defaultLogger{})
}
