// Package cubecompose validates and renders bounded compositions of typed cube
// queries. Runtime execution and contract derivation remain report-owned.
//
// Computed outputs require explicit safe aliases and concrete scalar types.
// Structured searched and simple CASE expressions traverse every branch through
// the same source, function and operator allowlists as ordinary expressions.
// Opaque or raw CASE SQL is never rendered. Literals use bound parameters.
//
// CASE merges compatible numeric result arms and preserves NULL using pointers.
// Integer arithmetic and SUM widen narrow signed and unsigned types to int64
// and uint64 respectively; floating computations widen to float64. COUNT is
// int64; AVG and division are nullable float64. Division retains native SQL
// semantics: the scanner can represent a fractional or NULL result, but SQL is
// not rewritten to change a dialect's integer division. MIN/MAX and NULLIF
// preserve the operand type with nullable output. COALESCE conservatively keeps
// nullable output even when a nonnull fallback might prove it unnecessary.
// Signed/unsigned combinations, unsigned negation/subtraction and expressions
// whose scalar type cannot be established are rejected. Integer overflow remains
// governed by the SQL engine. Mixed floating/integer expressions follow SQL
// numeric conversion and do not promise exact integer preservation.
//
// Source authorization, exposed fields, join keys, frame grouping and bounded
// paging retain their existing contracts. Only referenced source dimensions are
// requested; callers must select a dimension to preserve that grouping. The
// pinned parser requires whitespace before a function used as a division's right
// operand (amount / NULLIF(...)); opaque compact forms fail closed.
package cubecompose
