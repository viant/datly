package transcribe

import (
	"context"

	"github.com/viant/bindly/resource"
	"github.com/viant/datly/transcribe/dql"
)

// sourcePreparation belongs to one authored-source/resource lineage. Discovery
// and package compilation share its canonical directives and staged SQL embeds.
type sourcePreparation struct {
	originalText      string
	originalResources *resource.Store
	path              string
	text              string
	resources         *resource.Store
	readerName        string
	prepared          *dql.PreparedSource
	sourceMap         *SourceMap
}

func (p *sourcePreparation) matches(source *Source) bool {
	return p != nil && p.path == source.Path &&
		((p.originalText == source.Text && p.originalResources == source.Resources) ||
			(p.text == source.Text && p.resources == source.Resources))
}

func (p *sourcePreparation) apply(source *Source) *Source {
	staged := *source
	staged.Text, staged.Resources, staged.preparation = p.text, p.resources, p
	return &staged
}

func prepareSourceCompilation(ctx context.Context, source *Source) (*sourcePreparation, error) {
	if source.preparation.matches(source) {
		return source.preparation, nil
	}
	staged, patches, err := prepareParameterizedEmbeds(ctx, source)
	if err != nil {
		return nil, err
	}
	readerName, err := dql.ParseReaderRouteName(staged.Text)
	if err != nil {
		return nil, err
	}
	prepared := dql.PrepareSource(staged.Text)
	authoredMap := newSourceMap(len(source.Text), patches, 0, source.Text)
	sourceMap := newSourceMap(len(source.Text), patches, prepared.TrimPrefix, source.Text)
	if len(prepared.Diagnostics) > 0 {
		diagnostics := make([]*Diagnostic, 0, len(prepared.Diagnostics))
		for _, item := range prepared.Diagnostics {
			diagnostics = append(diagnostics, &Diagnostic{
				Code: item.Code, Severity: SeverityError, Message: item.Message, Path: source.Path,
				Span: Span{Start: authoredMap.Position(item.Offset), End: authoredMap.Position(item.End)},
			})
		}
		return nil, &CompileError{Cause: prepared.Err(), Diagnostics: diagnostics}
	}
	preparation := &sourcePreparation{
		originalText: source.Text, originalResources: source.Resources, path: source.Path,
		text: staged.Text, resources: staged.Resources, readerName: readerName,
		prepared: prepared, sourceMap: sourceMap,
	}
	source.preparation = preparation
	return preparation, nil
}
