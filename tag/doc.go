// Package tag owns Datly-specific Go struct-tag metadata.
//
// Binding tags are delegated to Bindly and sqlx tags are delegated to SQLX;
// this package does not maintain compatibility parsers for either mechanism.
//
// Linked readers consume complete metadata without inspecting view SQL. In an
// on tag, Field:namespace.column identifies the typed field and predicate source;
// append |output_label when a result label cannot be derived from the row tags
// (for example, a hidden SQL key). Transcription emits this distinction.
// sqlOutput records a proven result label when it differs from the SQLX source
// mapping. selectorAlias remains separate selector-name authority.
package tag
