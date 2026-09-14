// Package data owns immutable reader projections over detached spec snapshots.
// spec.View is the structural metadata authority; data adds resolved bindings,
// SQL columns and paired relation links. Compilation may enrich an exclusively
// owned snapshot before publication. Invocation state and database services
// belong to the SQL reader and never enter these metadata objects.
package data
