package velty

import (
	"fmt"

	xhandler "github.com/viant/xdatly/handler"
)

// DML exposes buffered sqlx operations to templates. Flush and transaction
// completion intentionally remain engine-owned.
type DML struct {
	data xhandler.DML
}

func (d *DML) Insert(tableName string, value interface{}) (string, error) {
	if d == nil || d.data == nil {
		return "", fmt.Errorf("velty DML capability is not configured")
	}
	return "", d.data.Insert(tableName, value)
}

func (d *DML) Update(tableName string, value interface{}) (string, error) {
	if d == nil || d.data == nil {
		return "", fmt.Errorf("velty DML capability is not configured")
	}
	return "", d.data.Update(tableName, value)
}

func (d *DML) Delete(tableName string, value interface{}) (string, error) {
	if d == nil || d.data == nil {
		return "", fmt.Errorf("velty DML capability is not configured")
	}
	return "", d.data.Delete(tableName, value)
}

func (d *DML) Execute(statement string, args ...interface{}) (string, error) {
	if d == nil || d.data == nil {
		return "", fmt.Errorf("velty DML capability is not configured")
	}
	return "", d.data.Execute(statement, args...)
}
