// Package column refines canonical view columns during explicit transcription.
// It delegates database metadata detection to SQLX and query rewriting to
// SQLParser; it owns no runtime scanner, cache, connector registry, or row map.
package column
