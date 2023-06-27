package codegen

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/datly/view"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/query"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
	"strings"
)

type (
	Relation struct {
		Name        string
		Join        *query.Join
		ParentField *Field
		KeyField    *Field
		Cardinality view.Cardinality
		*Spec
	}
	Spec struct {
		Parent       *Spec
		InnserColumn []string
		isAuxiliary  bool
		Table        string
		SQL          string
		Columns      []sink.Column
		pk           map[string]sink.Key
		Fk           map[string]sink.Key
		Type         *Type
		Relations    []*Relation
	}

	Selector []string
)

func (s *Spec) BuildType(pkg, name string, cardinality view.Cardinality, whitelist, blacklist map[string]bool) error {
	var aType = &Type{Package: pkg, Name: name, Cardinality: cardinality}

	for i, column := range s.Columns {
		if s.shouldSkipColumn(whitelist, blacklist, &column) {
			continue
		}
		field, err := aType.AppendColumnField(&s.Columns[i])
		if err != nil {
			return err
		}
		field.Tags.buildSqlxTag(s, field)
		field.Tags.buildJSONTag(field)
		field.Tags.buildValidateTag(field)
		field.Tag = field.Tags.Stringify()
		key := strings.ToLower(field.Column.Name)
		if pk, ok := s.pk[key]; ok {
			field.Pk = &pk
			aType.pkFields = append(aType.pkFields, field)
		}

	}
	s.Type = aType
	return nil
}

func (s *Spec) TypeDefinition(wrapper string) *view.TypeDefinition {
	typeDef := &view.TypeDefinition{
		Package:     s.Type.Package,
		Name:        s.Type.Name,
		Cardinality: s.Type.Cardinality,
		Fields:      s.Fields(),
	}
	if wrapper != "" {
		return &view.TypeDefinition{
			Name:    wrapper,
			Package: s.Type.Package,
			Fields: []*view.Field{
				{
					Name:        wrapper,
					Fields:      typeDef.Fields,
					Cardinality: typeDef.Cardinality,
					Tag:         fmt.Sprintf(`typeName:"%v"`, typeDef.Name),
					Ptr:         true,
				},
			},
		}
	}
	return typeDef
}

func (s *Spec) shouldSkipColumn(whitelist, blacklist map[string]bool, column *sink.Column) bool {
	columnName := strings.ToLower(column.Name)
	if len(blacklist) > 0 {
		return blacklist[columnName]
	}
	if len(whitelist) > 0 {
		return !whitelist[columnName]
	}
	return false
}

func (s *Spec) AddRelation(name string, join *query.Join, spec *Spec, cardinality view.Cardinality) {
	if isToOne(join) {
		cardinality = view.One
	}
	relColumn, refColumn := extractRelationColumns(join)
	rel := &Relation{Spec: spec,
		KeyField:    spec.Type.ByColumn(refColumn),
		ParentField: s.Type.ByColumn(relColumn),
		Name:        name,
		Join:        join,
		Cardinality: cardinality}
	s.Relations = append(s.Relations, rel)
	s.Type.AddRelation(name, spec, rel)
}

func readSinkColumns(ctx context.Context, db *sql.DB, table string) ([]sink.Column, error) {
	session, err := config.Session(ctx, db)
	if err != nil {
		return nil, err
	}
	return config.Columns(ctx, session, db, table)
}

func detectSinkColumn(ctx context.Context, db *sql.DB, SQL string) ([]sink.Column, error) {
	SQL = trimParenthesis(SQL)
	query, err := sqlparser.ParseQuery(SQL)
	var table string
	if query != nil {
		from := sqlparser.Stringify(query.From.X)
		if query.List.IsStarExpr() && !strings.Contains(from, "SELECT") {
			return nil, nil //use table metadata
		}
		query.Window = nil
		query.Qualify = nil
		query.Limit = nil
		query.Offset = nil
		SQL = sqlparser.Stringify(query)
		table = sqlparser.Stringify(query.From.X)
		SQL += " LIMIT 1"
	}

	var byName = map[string]sink.Column{}
	if table != "" && !strings.Contains(table, " ") {
		if sinkColumns, _ := readSinkColumns(ctx, db, table); len(sinkColumns) > 0 {
			byName = sink.Columns(sinkColumns).By(sink.ColumnName.Key)
		}
	}
	stmt, err := db.PrepareContext(ctx, SQL)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []sink.Column
	rows.Next()
	if rows != nil {
		if columnsTypes, _ := rows.ColumnTypes(); len(columnsTypes) != 0 {
			columns := io.TypesToColumns(columnsTypes)
			for _, item := range columns {
				sinkColumn := sink.Column{
					Name: item.Name(),
					Type: item.DatabaseTypeName(),
				}
				if match, ok := byName[sink.ColumnName.Key(&sinkColumn)]; ok {
					sinkColumn = match
				} else {
					if nullable, ok := item.Nullable(); ok && nullable {
						sinkColumn.Nullable = "1"
					}
					if length, ok := item.Length(); ok {
						sinkColumn.Length = &length
					}
				}
				result = append(result, sinkColumn)
			}
		}
	}

	return result, nil
}

func (s *Spec) Selector() Selector {
	if s.Parent != nil {
		return append(s.Parent.Selector(), s.Type.Name)
	}
	return []string{s.Type.Name}
}

func (s *Spec) pkStructQL(selector Selector) (*Field, string) {
	for _, field := range s.Type.pkFields { //TODO add  multi key support
		return field, fmt.Sprintf("? SELECT ARRAY_AGG(%v) AS Values FROM  `%v` LIMIT 1", field.Name, selector.Path())
	}
	return nil, ""
}

func (s *Spec) viewSQL(columnParameter ColumnParameterNamer) string {
	builder := &strings.Builder{}
	if s.SQL != "" {
		builder.WriteString(s.SQL)
	} else {
		builder.WriteString("SELECT * FROM ")
		builder.WriteString(s.Table)
	}

	var i int
	for _, field := range s.Type.pkFields {
		if i == 0 {
			builder.WriteString("\nWHERE ")
		} else {
			builder.WriteString("\nAND ")
		}
		i++
		structQLParam := columnParameter(field.Column)
		builder.WriteString(fmt.Sprintf(`$criteria.In("%v", $%v.Values)`, field.Column.Name, structQLParam))
	}
	return builder.String()
}

//NewSpec discover column derived type for supplied SQL/table
func NewSpec(ctx context.Context, db *sql.DB, table, SQL string) (*Spec, error) {
	var result = &Spec{Table: table, SQL: SQL, isAuxiliary: isAuxiliary(SQL)}
	var columns []sink.Column
	var err error
	table = trimParenthesis(table)
	SQL = strings.Replace(SQL, "("+table+")", table, 1)
	if SQL != "" {
		if columns, err = detectSinkColumn(ctx, db, SQL); err != nil {
			return nil, err
		}
	}
	result.Table = table
	result.SQL = SQL
	if len(columns) == 0 { //TODO mere column types
		if columns, err = readSinkColumns(ctx, db, table); err != nil {
			return nil, err
		}
	}
	result.Columns = columns

	meta := metadata.New()
	args := option.NewArgs("", "", table)
	var fkKeys, keys []sink.Key
	if err := meta.Info(ctx, db, info.KindForeignKeys, &fkKeys, args); err != nil {
		return nil, err
	}
	if err := meta.Info(ctx, db, info.KindPrimaryKeys, &keys, args); err != nil {
		return nil, err
	}
	result.pk = sink.Keys(keys).By(sink.KeyName.Column)
	result.Fk = sink.Keys(fkKeys).By(sink.KeyName.Column)
	return result, nil
}

func isAuxiliary(SQL string) bool {
	if SQL == "" {
		return false
	}
	SQL = trimParenthesis(SQL)
	aQuery, _ := sqlparser.ParseQuery(SQL)
	if aQuery == nil {
		return false
	}
	if aQuery.From.X == nil {
		return true
	}
	from := sqlparser.Stringify(aQuery.From.X)
	return strings.Contains(from, "(")
}

func isToOne(join *query.Join) bool {
	return strings.Contains(sqlparser.Stringify(join.On), "1 = 1")
}
