package sqlite

import (
	"time"
)

// converter implements mysql.DataConverter
// SQLite does not require any conversion, so this is a no-op implementation
type converter struct{}

// newConverter returns a new instance of converter
func newConverter() *converter {
	return &converter{}
}

// ToDateTime returns the same time
func (c converter) ToDateTime(t time.Time) time.Time { return t }

// FromDateTime returns the same time
func (c converter) FromDateTime(t time.Time) time.Time { return t }
