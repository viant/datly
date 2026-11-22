package spec

import "reflect"

// ErrorOutput selects JSON presentation for one concrete explicit error payload.
// Type identifies a named struct. Returning the struct or its pointer selects
// the same declaration.
type ErrorOutput struct {
	Type         reflect.Type
	Presentation ErrorOutputPresentation
}

type ErrorOutputPresentation string

const (
	// ErrorOutputJSON uses ordinary HTTP JSON and MCP structured text.
	ErrorOutputJSON ErrorOutputPresentation = "json"
	// ErrorOutputLegacyText frames JSON using the linked type's standard JSON tags as HTTP text/plain identity
	// content and presents the same media type as an embedded MCP blob.
	ErrorOutputLegacyText ErrorOutputPresentation = "legacyText"
)
