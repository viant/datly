package inference

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/datly/view"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/query"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
	"strings"
)

type (
	//Relation defines relation
	Relation struct {
		Name        string
		Join        *query.Join
		ParentField *Field
		KeyField    *Field
		Cardinality view.Cardinality
		*Spec
	}

	//Spec defines table/sql base specification
	Spec struct {
		Parent      *Spec
		IsAuxiliary bool
		Table       string
		SQL         string
		Columns     sqlparser.Columns
		pk          map[string]sink.Key
		Fk          map[string]sink.Key
		Type        *Type
		Relations   []*Relation
	}

	//Selector defines selector
	Selector []string
)

//BuildType build a type from infered table/SQL definition
func (s *Spec) BuildType(pkg, name string, cardinality view.Cardinality, whitelist, blacklist map[string]bool) error {
	var aType = &Type{Package: pkg, Name: name, Cardinality: cardinality}

	for i, column := range s.Columns {
		if s.shouldSkipColumn(whitelist, blacklist, column) {
			continue
		}
		field, err := aType.AppendColumnField(s.Columns[i])
		if err != nil {
			return err
		}
		field.Tags.Init(column.Tag)
		field.Tags.buildSqlxTag(s, field)
		field.Tags.buildJSONTag(field)
		field.Tags.buildValidateTag(field)
		field.Tag = field.Tags.Stringify()

		key := strings.ToLower(field.Column.Name)
		if pk, ok := s.pk[key]; ok {
			field.Pk = &pk
			aType.PkFields = append(aType.PkFields, field)
		}
	}
	s.Type = aType
	return nil
}

//TypeDefinition builds spec based tyep definition
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

func (s *Spec) shouldSkipColumn(whitelist, blacklist map[string]bool, column *sqlparser.Column) bool {
	name := column.Alias
	if name == "" {
		name = column.Name
	}
	columnName := strings.ToLower(name)
	if len(blacklist) > 0 {
		return blacklist[columnName]
	}
	if len(whitelist) > 0 {
		return !whitelist[columnName]
	}
	return false
}

//AddRelation adds relations
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

//Selector returns current sepcifiction selector (path from root)
func (s *Spec) Selector() Selector {
	if s.Parent != nil {
		return append(s.Parent.Selector(), s.Type.Name)
	}
	return []string{s.Type.Name}
}

//PkStructQL crates a PK struct SQL
func (s *Spec) PkStructQL(selector Selector) (*Field, string) {
	for _, field := range s.Type.PkFields { //TODO add  multi key support
		return field, fmt.Sprintf("? SELECT ARRAY_AGG(%v) AS Values FROM  `%v` LIMIT 1", field.Name, selector.Path())
	}
	return nil, ""
}

//ViewSQL return structQL SQL for relation
func (s *Spec) ViewSQL(columnParameter ColumnParameterNamer) string {
	builder := &strings.Builder{}
	if s.SQL != "" {
		builder.WriteString(s.SQL)
	} else {
		builder.WriteString("SELECT * FROM ")
		builder.WriteString(s.Table)
	}

	var i int
	for _, field := range s.Type.PkFields {
		if i == 0 {
			builder.WriteString("\nWHERE ")
		} else {
			builder.WriteString("\nAND ")
		}
		i++
		structQLParam := columnParameter(field)
		builder.WriteString(fmt.Sprintf(`$criteria.In("%v", $%v.Values)`, field.Column.Name, structQLParam))
	}
	return builder.String()
}

//NewSpec discover column derived type for supplied SQL/table
func NewSpec(ctx context.Context, db *sql.DB, table, SQL string) (*Spec, error) {
	isAuxiliary := isAuxiliary(SQL)
	table = normalizeTable(table)
	SQL = normalizeSQL(SQL, table)
	if table == "" && SQL == "" {
		return nil, fmt.Errorf("both table/SQL were empty")
	}
	var result = &Spec{Table: table, SQL: SQL, IsAuxiliary: isAuxiliary}
	var columns sqlparser.Columns
	var err error
	if SQL != "" {
		if columns, err = detectColumns(ctx, db, SQL, table); err != nil {
			return nil, err
		}
	}
	if len(columns) == 0 { //TODO mere column types
		sinkColumns, err := readSinkColumns(ctx, db, table)
		if err != nil {
			return nil, err
		}
		columns = asColumns(sinkColumns)
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

func normalizeSQL(SQL string, table string) string {
	SQL = strings.Replace(SQL, "("+table+")", table, 1)
	return SQL
}

func normalizeTable(table string) string {
	table = trimParenthesis(table)
	return table
}