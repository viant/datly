package readerbuilder

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/viant/datly/spec"
)

// UnmarshalJSON preserves integer literals in authored configuration. Retain
// the HTTP author's strict unknown-field policy even through this custom decoder.
func (m *PredicateMutation) UnmarshalJSON(data []byte) error {
	type plain PredicateMutation
	var decoded plain
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&decoded); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("predicate mutation must contain one JSON object")
	}
	if decoded.HandlerArgs != nil {
		for i, value := range *decoded.HandlerArgs {
			if number, ok := value.(json.Number); ok {
				integer, err := spec.ParseHandlerArgument(string(number))
				if err != nil {
					return err
				}
				(*decoded.HandlerArgs)[i] = integer
			}
			if err := spec.ValidateHandlerArgument((*decoded.HandlerArgs)[i]); err != nil {
				return err
			}
		}
	}
	*m = PredicateMutation(decoded)
	return nil
}
