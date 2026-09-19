package readerbuilder

import (
	"fmt"
	"strings"

	"github.com/viant/tagly/format/text"
)

func (s *Service) createReader(source string, mutation *ReaderMutation) (string, error) {
	if strings.TrimSpace(source) != "" {
		return "", fmt.Errorf("createReader requires empty source")
	}
	if mutation == nil {
		return "", fmt.Errorf("reader is required")
	}
	packagePath := strings.TrimSpace(mutation.Package)
	connector := strings.TrimSpace(mutation.Connector)
	route := strings.TrimSpace(mutation.Route)
	name := strings.TrimSpace(mutation.Name)
	sql := strings.TrimSpace(mutation.SQL)
	if packagePath == "" || connector == "" || route == "" || !validIdentifier(name) || sql == "" {
		return "", fmt.Errorf("reader package, connector, route, view name, and SQL are required")
	}
	if strings.ContainsAny(packagePath+connector+route, "'\"\r\n()") || !strings.HasPrefix(route, "/") {
		return "", fmt.Errorf("reader package, connector, or route is invalid")
	}
	if len(s.config.AvailableConnectors) > 0 && !slicesContainsFold(s.config.AvailableConnectors, connector) {
		return "", fmt.Errorf("connector %q is not available to the reader builder service", connector)
	}
	typeName := strings.TrimSpace(mutation.TypeName)
	if typeName == "" {
		typeName = text.DetectCaseFormat(name).Format(name, text.CaseFormatUpperCamel)
	}
	outputName := strings.TrimSpace(mutation.OutputName)
	if outputName == "" {
		outputName = typeName + "s"
	}
	if !validIdentifier(typeName) || !validIdentifier(outputName) {
		return "", fmt.Errorf("reader typeName and outputName must be identifiers")
	}
	return fmt.Sprintf(`#package('%s')

#setting($_ = $connector('%s'))
#setting($_ = $route('%s', 'GET'))
#setting($_ = $case_format('lc'))

#define($_ = $%s<[]*%s>(output/view))

SELECT %s.*,
       type(%s, '%s')
FROM (%s) %s`, packagePath, connector, route, outputName, typeName, name, name, typeName, sql, name), nil
}
