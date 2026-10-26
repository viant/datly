package spec

import (
	"fmt"
	"strings"
)

// ExecutionOptions are authored, dialect-qualified planning controls.
type ExecutionOptions struct {
	MySQL *MySQLExecutionOptions `json:"mysql,omitempty" yaml:"mysql,omitempty"`
}

type MySQLExecutionOptions struct {
	ConditionFanoutFilter *bool `json:"conditionFanoutFilter,omitempty" yaml:"conditionFanoutFilter,omitempty"`
}

func (o *ExecutionOptions) Validate() error {
	if o != nil && o.MySQL != nil && o.MySQL.ConditionFanoutFilter != nil && *o.MySQL.ConditionFanoutFilter {
		return fmt.Errorf("mysql_condition_fanout_filter currently supports only false; omit it to retain the database default")
	}
	return nil
}

func (o *ExecutionOptions) Active(product string) bool {
	return strings.EqualFold(strings.TrimSpace(product), "mysql") && o != nil && o.MySQL != nil && o.MySQL.ConditionFanoutFilter != nil
}

func (o *ExecutionOptions) Clone() *ExecutionOptions {
	if o == nil {
		return nil
	}
	result := *o
	if o.MySQL != nil {
		mysql := *o.MySQL
		mysql.ConditionFanoutFilter = cloneBool(mysql.ConditionFanoutFilter)
		result.MySQL = &mysql
	}
	return &result
}
