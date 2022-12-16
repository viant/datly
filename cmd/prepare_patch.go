package cmd

import (
	"context"
	"fmt"
	"github.com/viant/datly/cmd/option"
	"github.com/viant/datly/view"
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
	}
)

func newPatchStmtBuilder(sb *strings.Builder, metadata *inputMetadata) *patchStmtBuilder {
	builder := newStmtBuilder(sb, metadata, withSQL(true), view.KindRequestBody)
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
	return patchBuilder.build("", true)
}

func (b *patchStmtBuilder) build(parentRecord string, withUnsafe bool) (string, error) {
	if err := b.stmtBuilder.appendHints(b.typeDef); err != nil {
		return "", err
	}

	b.insert.appendAllocation(b.typeDef, "", b.typeDef.paramName)
	indexes, err := b.generateIndexes(b.typeDef)
	if err != nil {
		return "", nil
	}

	accessor, multi := b.appendForEachIfNeeded(parentRecord, b.paramName, withUnsafe)
	contentBuilder := b
	if multi {
		contentBuilder = b.withIndent()
	}

	withUnsafe = accessor.withUnsafe
	if err = contentBuilder.appendPatchContent(indexes, accessor); err != nil {
		return "", err
	}

	if multi {
		b.writeString("\n#end")
	}

	return b.sb.String(), nil
}

func (b *patchStmtBuilder) appendPatchContent(indexes []*patchChecker, accessor *paramAccessor) error {
	contentBuilder := b.withIndent()

	b.writeString("\n#if(")
	for i, index := range indexes {
		if i != 0 {
			contentBuilder.writeString(" && ")
		}
		contentBuilder.writeString(fmt.Sprintf("($%v.HasKey($%v) == true)", index.indexName, accessor.unsafeRecord+"."+index.field))
	}

	contentBuilder.writeString(")")
	if err := contentBuilder.update.appendUpdate(accessor); err != nil {
		return err
	}

	b.writeString("\n#else")
	if err := contentBuilder.insert.appendInsert(accessor); err != nil {
		return err
	}

	b.writeString("\n#end")
	return nil
}

func (b *patchStmtBuilder) generateIndexes(def *inputMetadata) ([]*patchChecker, error) {
	var checkers []*patchChecker
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
		})
	}

	return checkers, nil
}

func (b *patchStmtBuilder) withIndent() *patchStmtBuilder {
	aCopy := *b
	aCopy.update = b.update.withIndent()
	aCopy.insert = b.insert.withIndent()
	aCopy.stmtBuilder = aCopy.stmtBuilder.withIndent()
	return &aCopy
}
