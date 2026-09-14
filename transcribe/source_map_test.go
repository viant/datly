package transcribe

import (
	"testing"

	"github.com/viant/velty"
)

func TestSourceMapMapsSanitizedExpansionToAuthoredSource(t *testing.T) {
	original := "SELECT café FROM t WHERE id = $ID"
	start := len("SELECT café FROM t WHERE id = ")
	mapper := newSourceMap(len(original), []velty.Patch{{
		Span:        velty.Span{Start: start, End: len(original) - 1},
		Replacement: []byte("$Unsafe.ID"),
	}}, 0, original)

	position := mapper.Position(start + len("$Unsafe"))
	if position.Offset != start || position.Line != 1 || position.Char != 31 {
		t.Fatalf("Position() = %+v", position)
	}
}

func TestSourceMapRemapsDiagnosticsAfterTrimAndPatch(t *testing.T) {
	original := "\nSELECT $ID"
	start := len("\nSELECT ")
	mapper := newSourceMap(len(original), []velty.Patch{{
		Span:        velty.Span{Start: start, End: len(original) - 1},
		Replacement: []byte("$Unsafe.ID"),
	}}, 1, original)
	diagnostic := &Diagnostic{Span: Span{
		Start: Position{Offset: len("SELECT $Unsafe")},
		End:   Position{Offset: len("SELECT $Unsafe.ID")},
	}}

	mapper.Remap([]*Diagnostic{diagnostic})
	if diagnostic.Span.Start.Line != 2 || diagnostic.Span.Start.Char != 8 {
		t.Fatalf("remapped start = %+v", diagnostic.Span.Start)
	}
}

func TestPointSpanIsRuneAware(t *testing.T) {
	span := pointSpan("aéz", 1)
	if span.Start.Char != 2 || span.End.Offset != 3 || span.End.Char != 3 {
		t.Fatalf("pointSpan() = %+v", span)
	}
}

func TestSourceMapMapsFirstOffsetAfterReplacementToFollowingSource(t *testing.T) {
	original := "a$Xb"
	mapper := newSourceMap(len(original), []velty.Patch{{
		Span:        velty.Span{Start: 1, End: 2},
		Replacement: []byte("$Unsafe.X"),
	}}, 0, original)

	position := mapper.Position(1 + len("$Unsafe.X"))
	if position.Offset != 3 || position.Char != 4 {
		t.Fatalf("Position(after replacement) = %+v", position)
	}
}
