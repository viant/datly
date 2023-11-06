package tags

import (
	"github.com/viant/tagly/tags"
)

const (
	SQLTag        = "sql"
	SQLSummaryTag = "sqlSummary"
)

type (
	ViewSQL        string
	ViewSQLSummary ViewSQL
)

func (v ViewSQL) Tag() *tags.Tag {
	if v == "" {
		return nil
	}
	return &tags.Tag{Name: SQLTag, Values: tags.Values(v)}
}

func (v ViewSQLSummary) Tag() *tags.Tag {
	if v == "" {
		return nil
	}
	return &tags.Tag{Name: SQLTag, Values: tags.Values(v)}
}
