package provider

// TypedQueryValue carries an already decoded invocation value. It separates
// typed report filters from HTTP query occurrences that still need CSV parsing.
type TypedQueryValue struct {
	Value any
}
