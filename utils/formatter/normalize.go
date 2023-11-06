package formatter

import (
	"github.com/viant/tagly/format/text"
	"strings"
)

func NormalizePath(path string) string {
	segments := strings.Split(path, ".")
	for i, segment := range segments {
		segmentFormat := text.DetectCaseFormat(segment)
		if !segmentFormat.IsDefined() || segmentFormat == text.CaseFormatUpperCamel {
			continue
		}

		segments[i] = segmentFormat.Format(segment, text.CaseFormatUpperCamel)
	}
	return strings.Join(segments, ".")
}
