package cmd

import (
	"context"
	"fmt"
	"github.com/viant/datly/cmd/option"
	"strings"
)

type (
	patchStmtBuilder struct {
		*stmtBuilder
		insert *insertStmtBuilder
		update *updateStmtBuilder
	}

	patchChecker struct {
		indexName string
		field     string
		paramName string
	}
)

func newPatchStmtBuilder(sb *strings.Builder, metadata *inputMetadata) *patchStmtBuilder {
	builder := newStmtBuilder(sb, metadata, withSQL(true))
	return &patchStmtBuilder{
		stmtBuilder: builder,
		insert: &insertStmtBuilder{
			stmtBuilder: builder,
		},
		update: &updateStmtBuilder{
			stmtBuilder: builder,
		},
	}
}

func (s *Builder) preparePatchRule(ctx context.Context, sourceSQL []byte) (string, error) {
	routeOption, config, paramType, err := s.buildInputMetadata(ctx, sourceSQL)
	if err != nil {
		return "", err
	}

	SQL, err := s.buildPatchSQL(routeOption, config, paramType)
	if err != nil {
		return "", err
	}

	if _, err = s.uploadSQL(folderSQL, s.fileNames.unique(config.fileName), SQL, false); err != nil {
		return "", err
	}

	return SQL, err
}

func (s *Builder) buildPatchSQL(routeOption *option.RouteConfig, config *viewConfig, metadata *inputMetadata) (string, error) {
	sb, err := s.prepareStringBuilder(metadata, config, routeOption)
	if err != nil {
		return "", err
	}

	patchBuilder := newPatchStmtBuilder(sb, metadata)
	return patchBuilder.buildWithMeta("", true)
}

func (b *patchStmtBuilder) buildWithMeta(parentRecord string, withUnsafe bool) (string, error) {
	if err := b.stmtBuilder.appendHints(b.typeDef); err != nil {
		return "", err
	}

	if err := b.stmtBuilder.appendSQLWithRelations(); err != nil {
		return "", err
	}

	b.insert.appendAllocation(b.typeDef, "", b.typeDef.paramName)
	indexes, err := b.generateIndexes()
	if err != nil {
		return "", nil
	}

	return b.build(parentRecord, withUnsafe, indexes)
}

func (b *patchStmtBuilder) build(parentRecord string, withUnsafe bool, indexes []*patchChecker) (string, error) {
	accessor, multi := b.appendForEachIfNeeded(parentRecord, b.paramName, withUnsafe)
	contentBuilder := b
	if multi {
		contentBuilder = b.withIndent()
	}
	withUnsafe = accessor.withUnsafe

	if err := contentBuilder.appendPatchContent(indexes, accessor); err != nil {
		return "", err
	}

	for _, relation := range contentBuilder.typeDef.relations {
		if _, err := contentBuilder.newRelation(relation).build(accessor.record, withUnsafe, indexes); err != nil {
			return "", nil
		}
	}

	if multi {
		b.writeString("\n#end")
	}

	return b.sb.String(), nil
}

func (b *patchStmtBuilder) appendPatchContent(indexes []*patchChecker, accessor *paramAccessor) error {
	contentBuilder := b.withIndent()

	b.writeString("\n#if(")
	var i = 0
	for _, index := range indexes {
		if index.paramName != b.typeDef.paramName {
			continue
		}

		if i != 0 {
			contentBuilder.writeString(" && ")
		}

		i++
		contentBuilder.writeString(fmt.Sprintf("($%v.HasKey($%v) == true)", index.indexName, accessor.unsafeRecord+"."+index.field))
	}

	contentBuilder.writeString(")")

	if err := contentBuilder.update.appendUpdate(accessor, b.update.parent); err != nil {
		return err
	}

	b.writeString("\n#else")
	if err := contentBuilder.insert.appendInsert(accessor); err != nil {
		return err
	}

	b.writeString("\n#end")
	return nil
}

func (b *patchStmtBuilder) generateIndexes() ([]*patchChecker, error) {
	var checkers []*patchChecker
	err := b.iterateOverHints(b.typeDef, func(def *inputMetadata) error {
		for _, field := range def.actualFields {
			aMeta, ok := def.meta.metaByColName(field.Column)
			if !ok || !aMeta.primaryKey {
				continue
			}

			indexName := fmt.Sprintf("%vIndex", def.sqlName)
			aFieldName := aMeta.fieldName

			b.sb.WriteString("\n")
			b.writeString(fmt.Sprintf("#set($%v = $%v.IndexBy(\"%v\"))", indexName, def.sqlName, aFieldName))

			checkers = append(checkers, &patchChecker{
				indexName: indexName,
				field:     aFieldName,
				paramName: def.paramName,
			})
		}

		return nil
	})

	return checkers, err
}

func (b *patchStmtBuilder) withIndent() *patchStmtBuilder {
	aCopy := *b
	aCopy.update = b.update.withIndent()
	aCopy.insert = b.insert.withIndent()
	aCopy.stmtBuilder = aCopy.stmtBuilder.withIndent()
	return &aCopy
}

func (b *patchStmtBuilder) newRelation(metadata *inputMetadata) *patchStmtBuilder {
	return &patchStmtBuilder{
		stmtBuilder: b.stmtBuilder.newRelation(metadata),
		insert:      b.insert.newRelation(metadata),
		update:      b.update.newRelation(metadata),
	}
}
