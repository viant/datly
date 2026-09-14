// Package compiler converts canonical view metadata and typed contracts into
// immutable SQL reader plans. Each graph starts with a detached spec.View
// snapshot owned by data.View.Spec. Enrichment updates that snapshot; only
// resolved SQL columns and paired field links need reader projections.
// Publication ends enrichment. Collector state, native caches, partitioners,
// templates and execution remain with their existing reader owners.
// This package does not parse authored DQL, register runtime components, scan
// rows, or execute SQL.
package compiler
