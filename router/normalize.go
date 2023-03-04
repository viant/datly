package router

import (
	"github.com/viant/datly/utils"
	"github.com/viant/toolbox/format"
	"strings"
)

func NormalizePath(path string) string {
	segments := strings.Split(path, ".")
	for i, segment := range segments {
		segmentFormat, err := format.NewCase(utils.DetectCase(segment))
		if err != nil || segmentFormat == format.CaseUpperCamel {
			continue
		}

		segments[i] = segmentFormat.Format(segment, format.CaseUpperCamel)
	}

	return strings.Join(segments, ".")
}
