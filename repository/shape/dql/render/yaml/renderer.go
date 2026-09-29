package yaml

import (
	"fmt"

	"github.com/viant/datly/repository/shape/dql/ir"
	"gopkg.in/yaml.v3"
)

// Encode renders IR document into YAML bytes.
func Encode(doc *ir.Document) ([]byte, error) {
	if doc == nil || doc.Root == nil {
		return nil, fmt.Errorf("dql render yaml: nil IR document")
	}
	return yaml.Marshal(doc.Root)
}
