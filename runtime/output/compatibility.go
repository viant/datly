package output

import shared "github.com/viant/datly/output"

// These aliases preserve existing compiled output contracts and error identity.
type Marshaller = shared.Marshaller
type CompileInput = shared.CompileInput
type Compiler = shared.Compiler
type Plan = shared.Plan
type Result = shared.Result
type WireContract = shared.WireContract
type JSONWireType = shared.JSONWireType
type SelectorFieldName = shared.SelectorFieldName

var ErrSchemaUnavailable = shared.ErrSchemaUnavailable

func ContentType(format string) (string, error) { return shared.ContentType(format) }
