package formatter

import (
	"github.com/viant/toolbox/format"
	"strings"
)

func NormalizePath(path string) string {
	segments := strings.Split(path, ".")
	for i, segment := range segments {
		segmentFormat, err := format.NewCase(DetectCase(segment))
		if err != nil || segmentFormat == format.CaseUpperCamel {
			continue
		}

		segments[i] = segmentFormat.Format(segment, format.CaseUpperCamel)
	}

	return strings.Join(segments, ".")
}
