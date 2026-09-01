package rangeiter

import "golang.org/x/exp/constraints"

// Iterator is an interface for iterating through a range between two values.
// The range is a range of integers from a minimum to a maximum value (inclusive).
type Iterator[T Integer] interface {
	// Next returns the next value closer to the max value in the range
	// If the current value is the max value, Next will return the max value
	Next() T

	// Previous returns the previous value closer to the min value in the range
	// If the current value is the min value, Previous will return the min value
	Previous() T

	// Value returns the current value in the range
	Value() T

	// Reset resets the Iterator to its initial state
	Reset()
}

// Integer is a type constraint for the Iterator interface to ensure that only integer types are used.
type Integer = constraints.Integer
