// Package dml provides the SQLX-backed buffered write and transaction scope.
//
// A serialized drain privately reuses one UPDATE statement for compatible
// consecutive operations. Non-UPDATE steps and both sides of append barriers
// invalidate reuse. SQLX still builds SQL, binds values and executes hooks for
// every operation. Cleanup finishes before transaction completion; cleanup
// errors prevent commit, and caller-owned transactions remain caller-owned.
package dml
