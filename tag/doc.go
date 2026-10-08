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
//
// Documentation origins are also metadata: scalar docTable/docColumn tags
// identify a physical dictionary column. docTable:"-" marks computed or
// ambiguous outputs so they do not inherit a misleading table description.
// A view's docTable option records its dictionary table independently of its
// executable SQL source. Transcription emits these facts; loading never
// reconstructs documentation lineage from SQL.
package tag
