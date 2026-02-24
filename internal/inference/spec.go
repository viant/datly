package inference

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/internal/msg"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/column"
	"github.com/viant/datly/view/state"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/query"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
)

type (
	//Relation defines relation
	Relation struct {
		Name        string
		Join        *query.Join
		ParentField *Field
		KeyField    *Field
		Cardinality state.Cardinality
		*Spec
	}

	//Spec defines table/sql base specification
	Spec struct {
		Namespace   string
		Parent      *Spec
		Package     string
		IsAuxiliary bool
		Table       string
		SQL         string
		SQLArgs     []interface{}
		Columns     sqlparser.Columns
		pk          map[string]sink.Key
		Fk          map[string]sink.Key
		Type        *Type
		Relations   []*Relation
	}

	//Selector defines selector
	Selector []string
)

func (s *Spec) NormalizeSQL() {
	if s.SQL == "" || s.Table == "" {
		return
	}
	s.SQL = strings.ReplaceAll(s.SQL, "("+s.Table+")", s.Table)
}

func (s *Spec) EnsureRelationType() {
	if len(s.Relations) == 0 {
		return
	}
	for i, field := range s.Type.RelationFields {
		if schema := field.EnsureSchema(); schema.Type() != nil {
			continue
		}
		rel := s.Relations[i]

		rel.EnsureRelationType()
		if rel.Type != nil {
			field.Schema = state.NewSchema(reflect.StructOf(rel.Type.Fields()))
		}
		field.Schema.Cardinality = rel.Cardinality
		field.Schema.SetPackage(s.Package)
	}
	if len(s.Type.RelationFields) > 0 {
		return
	}
	//checking case we had relation but not relation fields
	for _, rel := range s.Relations {
		field := &Field{}
		field.EnsureSchema()
		field.Name = rel.Name
		if rel.Type != nil {
			field.Schema = state.NewSchema(reflect.StructOf(rel.Type.Fields()))
		}
		field.Schema.Cardinality = rel.Cardinality
		field.Schema.SetPackage(s.Package)

		s.Type.RelationFields = append(s.Type.RelationFields, field)
	}
}

// BuildType build a type from infered table/SQL definition
func (s *Spec) BuildType(pkg, name string, cardinality state.Cardinality, whitelist, blacklist map[string]bool) error {
	var aType = &Type{Package: pkg, Name: name, Cardinality: cardinality}
	for i, column := range s.Columns {
		skipped := s.shouldSkipColumn(whitelist, blacklist, column)
		field, err := aType.AppendColumnField(s.Columns[i], skipped, s.Table)
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

// TypeDefinition builds spec based tyep definition
func (s *Spec) TypeDefinition(wrapper string, includeHas bool, doc state.Documentation) *view.TypeDefinition {
	typeDef := &view.TypeDefinition{
		Package:     s.Type.Package,
		Name:        s.Type.Name,
		Cardinality: s.Type.Cardinality,
		Fields:      s.Fields(includeHas, doc),
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

// AddRelation adds relations
func (s *Spec) AddRelation(name string, join *query.Join, spec *Spec, cardinality state.Cardinality) error {
	if IsToOne(join) {
		cardinality = state.One
	}
	relColumn, refColumn := ExtractRelationColumns(join)
	parentField := s.Type.ByColumn(relColumn)
	if parentField == nil {
		var available []string
		for _, item := range s.Type.columnFields {
			available = append(available, item.Column.Name)
		}
		return fmt.Errorf("failed to match rel field for %v, available: %v %v", relColumn, s.Type.Name, available)
	}

	keyField := spec.Type.ByColumn(refColumn)
	if keyField == nil {
		var available []string
		for _, item := range spec.Type.columnFields {
			available = append(available, item.Column.Name)
		}
		return fmt.Errorf("failed to ref field for %v, available: %v on  %v", refColumn, available, join.Alias)
	}

	rel := &Relation{Spec: spec,
		KeyField:    keyField,
		ParentField: parentField,
		Name:        name,
		Join:        join,
		Cardinality: cardinality}
	s.Relations = append(s.Relations, rel)
	s.Type.AddRelation(name, spec, rel)
	return nil
}

// Selector returns current sepcifiction selector (path from root)
func (s *Spec) Selector(rootPath string) Selector {
	if s.Parent != nil {
		return append(s.Parent.Selector(rootPath), s.Type.Name)
	}
	return []string{rootPath}
}

// PkStructQL crates a PK struct SQL
func (s *Spec) PkStructQL(selector Selector) (*Field, string) {
	for _, field := range s.Type.PkFields { //TODO add  multi key support
		return field, fmt.Sprintf("? SELECT ARRAY_AGG(%v) AS Values FROM  `%v` LIMIT 1", field.Name, selector.Path())
	}
	return nil, ""
}

// ViewSQL return structQL SQL for relation
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
		if field.Column.IsAutoincrement {
			break
		}
	}
	return builder.String()
}

// NewSpec discover column derived type for supplied SQL/table
func NewSpec(ctx context.Context, db *sql.DB, messages *msg.Messages, table string, columnsConfig view.ColumnConfigs, SQL string, SQLArgs ...interface{}) (*Spec, error) {
	isAuxiliary := isAuxiliary(&SQL)
	table = normalizeTable(table)
	SQL = normalizeSQL(SQL, table)
	if table == "" && SQL == "" {
		return nil, fmt.Errorf("both table/SQL were empty")
	}
	var result = &Spec{Table: table, SQL: SQL, SQLArgs: SQLArgs, IsAuxiliary: isAuxiliary}
	columns, err := column.Discover(ctx, db, table, SQL, SQLArgs...)
	if err != nil {
		columns = bestEffortColumnsFromSQL(SQL, columnsConfig)
		if len(columns) == 0 {
			return nil, err
		}
		if messages != nil {
			messages.AddWarning(result.Table, "detection", fmt.Sprintf("using best-effort SQL column inference due to discovery error: %v", err))
		}
	}
	result.Columns = columns
	byName := result.Columns.ByName()
	for _, columnConfig := range columnsConfig {
		specColumn, ok := byName[columnConfig.Name]
		if ok {
			if columnConfig.DataType != nil {
				specColumn.Type = *columnConfig.DataType
			}
			if columnConfig.Required != nil {
				specColumn.IsNullable = !*columnConfig.Required
			}
		}
	}

	meta := metadata.New()
	args := option.NewArgs("", "", table)
	var fkKeys, keys []sink.Key
	if err := meta.Info(ctx, db, info.KindForeignKeys, &fkKeys, args); err != nil {
		messages.AddWarning(result.Table, "detection", fmt.Sprintf("unable to detect foreign key: %v, %v", table, err))
	}
	if err := meta.Info(ctx, db, info.KindPrimaryKeys, &keys, args); err != nil {
		messages.AddWarning(result.Table, "detection", fmt.Sprintf("unable to detect primary key: %v, %v", table, err))
	}
	result.pk = sink.Keys(keys).By(sink.KeyName.Column)
	result.Fk = sink.Keys(fkKeys).By(sink.KeyName.Column)
	if len(result.pk) == 0 {
		for _, column := range columns {
			if column.IsAutoincrement || strings.ToLower(column.Name) == "id" {
				result.pk = map[string]sink.Key{strings.ToLower(column.Name): {Column: column.Name}}
				break
			}
		}
	}

	return result, nil
}

func bestEffortColumnsFromSQL(SQL string, columnsConfig view.ColumnConfigs) sqlparser.Columns {
	if strings.TrimSpace(SQL) == "" {
		return nil
	}
	query, err := sqlparser.ParseQuery(SQL)
	if err != nil || query == nil {
		return nil
	}
	queryColumns := sqlparser.NewColumns(query.List)
	if len(queryColumns) == 0 {
		return nil
	}
	cfgByLower := map[string]*view.ColumnConfig{}
	for _, cfg := range columnsConfig {
		if cfg == nil || cfg.Name == "" {
			continue
		}
		cfgByLower[strings.ToLower(cfg.Name)] = cfg
	}
	var result sqlparser.Columns
	for _, candidate := range queryColumns {
		if candidate == nil {
			continue
		}
		expression := strings.TrimSpace(candidate.Expression)
		if expression == "*" || strings.HasSuffix(expression, ".*") {
			continue
		}
		name := strings.TrimSpace(candidate.Alias)
		if name == "" {
			name = strings.TrimSpace(candidate.Name)
		}
		if name == "" {
			continue
		}
		if candidate.Type == "" {
			if cfg, ok := cfgByLower[strings.ToLower(name)]; ok && cfg.DataType != nil && *cfg.DataType != "" {
				candidate.Type = *cfg.DataType
			} else if cfg, ok = cfgByLower[strings.ToLower(candidate.Name)]; ok && cfg.DataType != nil && *cfg.DataType != "" {
				candidate.Type = *cfg.DataType
			}
		}
		if candidate.Type == "" {
			candidate.Type = "string"
		}
		result = append(result, candidate)
	}
	return result
}

func isAuxiliary(SQL *string) bool {
	if *SQL == "" {
		return false
	}
	*SQL = TrimParenthesis(*SQL)
	aQuery, _ := sqlparser.ParseQuery(*SQL)
	if aQuery == nil {
		return false
	}
	if aQuery.From.X == nil {
		return true
	}
	from := strings.TrimSpace(sqlparser.Stringify(aQuery.From.X))
	lowerCasedFrom := strings.ToLower(from)
	ret := strings.HasPrefix(from, "(") && !(strings.Contains(lowerCasedFrom, "select"))
	if ret {
		*SQL = strings.Replace(*SQL, from, TrimParenthesis(from), 1)
	}
	return ret
}

func IsToOne(join *query.Join) bool {
	return strings.Contains(sqlparser.Stringify(join.On), "1 = 1")
}

func normalizeSQL(SQL string, table string) string {
	if table == "" {
		return SQL
	}
	SQL = strings.Replace(SQL, "("+table+")", table, 1)
	return SQL
}

func normalizeTable(table string) string {
	table = TrimParenthesis(table)
	return table
}
