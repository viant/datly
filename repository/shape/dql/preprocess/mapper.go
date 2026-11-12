package preprocess

import (
	"sort"
	"unicode/utf8"

	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	"github.com/viant/velty"
)

type Mapper struct {
	trimPrefix int
	segments   []mapSegment
	original   string
}

type mapSegment struct {
	newStart int
	newEnd   int
	origBase int
	linear   bool
}

func (m *Mapper) MapOffset(offset int) int {
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
	for _, seg := range m.segments {
		if mapped < seg.newStart || mapped > seg.newEnd {
			continue
		}
		if seg.linear {
			delta := mapped - seg.newStart
			if delta < 0 {
				delta = 0
			}
			return seg.origBase + delta
		}
		return seg.origBase
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

func (m *Mapper) Position(offset int) dqlshape.Position {
	return positionAt(m.original, m.MapOffset(offset))
}

func (m *Mapper) Remap(diags []*dqlshape.Diagnostic) {
	if m == nil || len(diags) == 0 {
		return
	}
	for _, diag := range diags {
		if diag == nil {
			continue
		}
		start := m.Position(diag.Span.Start.Offset)
		end := m.Position(diag.Span.End.Offset)
		diag.Span.Start = start
		diag.Span.End = end
	}
}

func newMapper(srcLen int, patches []velty.Patch, trimPrefix int, original string) *Mapper {
	if trimPrefix < 0 {
		trimPrefix = 0
	}
	ps := append([]velty.Patch{}, patches...)
	sort.Slice(ps, func(i, j int) bool { return ps[i].Span.Start < ps[j].Span.Start })
	segments := make([]mapSegment, 0, len(ps)*2+1)
	oldPos := 0
	newPos := 0
	for _, p := range ps {
		start := p.Span.Start
		end := p.Span.End + 1
		if start < oldPos || start < 0 || end < start || end > srcLen {
			continue
		}
		if start > oldPos {
			blockLen := start - oldPos
			segments = append(segments, mapSegment{
				newStart: newPos,
				newEnd:   newPos + blockLen,
				origBase: oldPos,
				linear:   true,
			})
			oldPos = start
			newPos += blockLen
		}
		replLen := len(p.Replacement)
		if replLen > 0 {
			segments = append(segments, mapSegment{
				newStart: newPos,
				newEnd:   newPos + replLen,
				origBase: start,
				linear:   false,
			})
			newPos += replLen
		}
		oldPos = end
	}
	if oldPos < srcLen {
		blockLen := srcLen - oldPos
		segments = append(segments, mapSegment{
			newStart: newPos,
			newEnd:   newPos + blockLen,
			origBase: oldPos,
			linear:   true,
		})
	}
	return &Mapper{trimPrefix: trimPrefix, segments: segments, original: original}
}

func pointSpan(text string, offset int) dqlshape.Span {
	start := positionAt(text, offset)
	end := positionAt(text, nextOffset(text, offset))
	return dqlshape.Span{Start: start, End: end}
}

// PointSpan returns a single-point span at offset with rune-aware line/char.
func PointSpan(text string, offset int) dqlshape.Span {
	return pointSpan(text, offset)
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

func positionAt(text string, offset int) dqlshape.Position {
	if offset < 0 {
		offset = 0
	}
	if offset > len(text) {
		offset = len(text)
	}
	line := 1
	char := 1
	index := 0
	for index < offset {
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
	return dqlshape.Position{Offset: offset, Line: line, Char: char}
}

// PositionAt returns rune-aware position for byte offset.
func PositionAt(text string, offset int) dqlshape.Position {
	return positionAt(text, offset)
}
