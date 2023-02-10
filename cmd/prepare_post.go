package cmd

import (
	"context"
	"fmt"
	"github.com/viant/datly/cmd/option"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/keywords"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/toolbox/format"
	"reflect"
	"strings"
)

type (
	inputMetadata struct {
		typeDef      *view.TypeDefinition
		meta         *typeMeta
		actualFields []*view.Field
		bodyHolder   string
		paramName    string
		relations    []*inputMetadata
		fkIndex      map[string]sink.Key
		pkIndex      map[string]sink.Key
		table        string
		config       *viewConfig
		sql          string
		sqlName      string
	}

	typeMeta struct {
		fieldIndex  map[string]int
		columnIndex map[string]int
		metas       []*fieldMeta
	}

	fieldMeta struct {
		primaryKey    bool
		autoincrement bool
		generator     string
		columnName    string
		fieldName     string
		required      bool
		columnCase    format.Case
		fkKey         *sink.Key
	}

	insertStmtBuilder struct {
		parent *insertStmtBuilder
		*stmtBuilder
	}
)

func (s *Builder) preparePostRule(ctx context.Context, sourceSQL []byte) (string, error) {
	routeOption, config, paramType, err := s.buildInputMetadata(ctx, sourceSQL)
	if err != nil {
		return "", err
	}

	template, err := s.buildInsertSQL(paramType, config, routeOption)
	if err != nil {
		return "", err
	}

	if _, err = s.upload(s.preGenSQLURL(s.fileNames.unique(config.fileName)), template); err != nil {
		return "", nil
	}

	return template, nil
}

func (s *Builder) buildInsertSQL(typeDef *inputMetadata, config *viewConfig, routeOption *option.RouteConfig) (string, error) {
	sb, err := s.prepareStringBuilder(typeDef, config, routeOption)
	if err != nil {
		return "", err
	}

	builder := newInsertStmtBuilder(sb, typeDef)
	if err := builder.appendHints(typeDef); err != nil {
		return "", err
	}

	builder.appendAllocation(typeDef, "", typeDef.paramName)

	return builder.build("", true)
}

func (isb *insertStmtBuilder) appendAllocation(def *inputMetadata, path, holderName string) {
	for _, meta := range def.meta.metas {
		if !meta.autoincrement {
			continue
		}

		isb.writeString("\n")
		isb.writeString(fmt.Sprintf(`$sequencer.Allocate("%v", $%v, "%v")`, def.table, holderName, path+meta.fieldName))
	}

	for _, relation := range def.relations {
		actualPath := path
		if actualPath == "" {
			actualPath = relation.paramName + "/"
		} else {
			actualPath += relation.paramName + "/"
		}
		isb.appendAllocation(relation, actualPath, holderName)
	}
}

func (s *Builder) recordName(recordName string, config *viewConfig) (string, bool) {
	if !config.outputConfig.IsMany() {
		return recordName, false
	}

	return "rec" + strings.Title(recordName), true
}

func (s *Builder) buildRequestBodyPostParam(config *viewConfig, def *inputMetadata) (reflect.Type, error) {
	if err := def.typeDef.Init(context.Background(), view.Types{}.LookupType); err != nil {
		return nil, err
	}

	return def.typeDef.Schema.Type(), nil
}

func newInsertStmtBuilder(sb *strings.Builder, def *inputMetadata) *insertStmtBuilder {
	return &insertStmtBuilder{
		stmtBuilder: newStmtBuilder(sb, def),
	}
}

func (isb *insertStmtBuilder) build(parentRecord string, withUnsafe bool) (string, error) {
	accessor, ok := isb.appendForEachIfNeeded(parentRecord, isb.paramName, withUnsafe)
	contentBuilder := isb
	if ok {
		contentBuilder = contentBuilder.withIndent()
	}

	withUnsafe = accessor.withUnsafe

	if contentBuilder.parent != nil {
		contentBuilder.appendSetFk(accessor, contentBuilder.parent.stmtBuilder)
	}

	if err := contentBuilder.appendInsert(accessor); err != nil {
		return "", err
	}

	for _, rel := range isb.typeDef.relations {
		_, err := contentBuilder.newRelation(rel).build(accessor.record, !contentBuilder.isMulti && withUnsafe)
		if err != nil {
			return "", err
		}
	}

	if ok {
		isb.writeString("\n#end")
	}
	return isb.sb.String(), nil
}

func (isb *insertStmtBuilder) appendInsert(accessor *paramAccessor) error {
	if strings.ToLower(isb.typeDef.config.expandedTable.ExecKind) == option.ExecKindService {
		isb.writeString(fmt.Sprintf("\n$%v.Insert($%v, \"%v\");", keywords.KeySQL, accessor.record, isb.typeDef.table))
		return nil
	}

	isb.writeString("\nINSERT INTO ")
	isb.writeString(isb.typeDef.table)
	isb.writeString("( ")
	if err := isb.stmtBuilder.appendColumnNames(accessor, false); err != nil {
		return err
	}

	isb.writeString("\n) VALUES (")
	if err := isb.appendColumnValues(accessor, false); err != nil {
		return err
	}
	isb.writeString("\n);\n")
	return nil
}

func (sb *stmtBuilder) appendSetFk(accessor *paramAccessor, parent *stmtBuilder) {
	if parent != nil {
		for _, meta := range sb.typeDef.meta.metas {
			if meta.fkKey == nil {
				continue
			}

			if meta.fkKey.ReferenceTable != parent.typeDef.table {
				continue
			}

			refMeta, ok := parent.typeDef.meta.metaByColName(meta.fkKey.ReferenceColumn)
			if !ok {
				continue
			}

			sb.writeString(fmt.Sprintf("\n#set($%v.%v = $%v.%v)", accessor.unsafeRecord, meta.fieldName, accessor.unsafeParent, refMeta.fieldName))
		}
	}
}

func (isb *insertStmtBuilder) newRelation(rel *inputMetadata) *insertStmtBuilder {
	builder := isb.stmtBuilder.newRelation(rel)
	return &insertStmtBuilder{
		stmtBuilder: builder,
		parent:      isb,
	}
}

func (isb *insertStmtBuilder) withIndent() *insertStmtBuilder {
	aCopy := *isb
	aCopy.stmtBuilder = aCopy.stmtBuilder.withIndent()
	return &aCopy
}
