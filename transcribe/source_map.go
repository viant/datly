package transcribe

import (
	"sort"
	"unicode/utf8"

	"github.com/viant/velty"
)

// SourceMap maps offsets in preprocessed SQL or Velty text back to authored
// DQL. This is ported from the newer original shape preprocessor.
type SourceMap struct {
	trimPrefix int
	segments   []sourceMapSegment
	original   string
}

type sourceMapSegment struct {
	newStart int
	newEnd   int
	origBase int
	linear   bool
}

func (m *SourceMap) MapOffset(offset int) int {
	if m == nil {
		if offset < 0 {
			return 0
		}
		return offset
	}
	mapped := offset + m.trimPrefix
	if mapped < 0 {
		mapped = 0
	}
	for _, segment := range m.segments {
		if mapped < segment.newStart || mapped >= segment.newEnd {
			continue
		}
		if segment.linear {
			delta := mapped - segment.newStart
			if delta < 0 {
				delta = 0
			}
			return segment.origBase + delta
		}
		return segment.origBase
	}
	if len(m.segments) == 0 {
		return mapped
	}
	last := m.segments[len(m.segments)-1]
	if last.linear {
		return last.origBase + (last.newEnd - last.newStart)
	}
	return last.origBase
}

func (m *SourceMap) Position(offset int) Position {
	return positionAt(m.original, m.MapOffset(offset))
}

func (m *SourceMap) Remap(diagnostics []*Diagnostic) {
	if m == nil {
		return
	}
	for _, diagnostic := range diagnostics {
		if diagnostic == nil {
			continue
		}
		diagnostic.Span.Start = m.Position(diagnostic.Span.Start.Offset)
		diagnostic.Span.End = m.Position(diagnostic.Span.End.Offset)
	}
}

func newSourceMap(sourceLength int, patches []velty.Patch, trimPrefix int, original string) *SourceMap {
	if trimPrefix < 0 {
		trimPrefix = 0
	}
	ordered := append([]velty.Patch{}, patches...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Span.Start < ordered[j].Span.Start })
	segments := make([]sourceMapSegment, 0, len(ordered)*2+1)
	oldPosition := 0
	newPosition := 0
	for _, patch := range ordered {
		start := patch.Span.Start
		end := patch.Span.End + 1
		if start < oldPosition || start < 0 || end < start || end > sourceLength {
			continue
		}
		if start > oldPosition {
			blockLength := start - oldPosition
			segments = append(segments, sourceMapSegment{
				newStart: newPosition,
				newEnd:   newPosition + blockLength,
				origBase: oldPosition,
				linear:   true,
			})
			oldPosition = start
			newPosition += blockLength
		}
		if replacementLength := len(patch.Replacement); replacementLength > 0 {
			segments = append(segments, sourceMapSegment{
				newStart: newPosition,
				newEnd:   newPosition + replacementLength,
				origBase: start,
			})
			newPosition += replacementLength
		}
		oldPosition = end
	}
	if oldPosition < sourceLength {
		blockLength := sourceLength - oldPosition
		segments = append(segments, sourceMapSegment{
			newStart: newPosition,
			newEnd:   newPosition + blockLength,
			origBase: oldPosition,
			linear:   true,
		})
	}
	return &SourceMap{trimPrefix: trimPrefix, segments: segments, original: original}
}

func pointSpan(text string, offset int) Span {
	return Span{Start: positionAt(text, offset), End: positionAt(text, nextOffset(text, offset))}
}

func nextOffset(text string, offset int) int {
	if offset < 0 {
		return 0
	}
	if offset >= len(text) {
		return len(text)
	}
	_, width := utf8.DecodeRuneInString(text[offset:])
	if width <= 0 {
		return offset + 1
	}
	return offset + width
}

func positionAt(text string, offset int) Position {
	if offset < 0 {
		offset = 0
	}
	if offset > len(text) {
		offset = len(text)
	}
	line := 1
	char := 1
	for index := 0; index < offset; {
		r, width := utf8.DecodeRuneInString(text[index:])
		if width <= 0 {
			break
		}
		index += width
		if r == '\n' {
			line++
			char = 1
		} else {
			char++
		}
	}
	return Position{Offset: offset, Line: line, Char: char}
}
