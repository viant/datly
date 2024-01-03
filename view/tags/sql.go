package tags

import (
	"github.com/viant/tagly/tags"
)

const (
	SQLTag        = "sql"
	SQLSummaryTag = "sqlSummary"
)

type (
	ViewSQL struct {
		SQL string `tag:"sql,omitempty"`
		URI string `tag:"uri,omitempty"`
	}
	ViewSQLSummary ViewSQL
)

func NewViewSQL(sql, uri string) ViewSQL {
	return ViewSQL{SQL: sql, URI: uri}
}

func (v ViewSQL) Tag() *tags.Tag {
	if v.URI == "" || v.SQL == "" {
		return nil
	}
	if v.URI != "" {
		return &tags.Tag{Name: SQLTag, Values: tags.Values("uri" + v.URI)}
	}
	return &tags.Tag{Name: SQLTag, Values: tags.Values(v.SQL)}
}

func (v ViewSQLSummary) Tag() *tags.Tag {
	if v.URI == "" || v.SQL == "" {
		return nil
	}
	if v.URI != "" {
		return &tags.Tag{Name: SQLSummaryTag, Values: tags.Values("uri=" + v.URI)}
	}
	return &tags.Tag{Name: SQLSummaryTag, Values: tags.Values(v.SQL)}
}

func NewViewSQLSummary(sql, uri string) ViewSQLSummary {
	return ViewSQLSummary{SQL: sql, URI: uri}
}
