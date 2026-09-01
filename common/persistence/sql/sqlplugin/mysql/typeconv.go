package mysql

import "time"

// DataConverter defines the API for conversions to/from
// go types to mysql datatypes
type DataConverter interface {
	ToDateTime(t time.Time) time.Time
	FromDateTime(t time.Time) time.Time
}

type converter struct{}

func NewConverter() DataConverter {
	return &converter{}
}

var minMySQLDateTime = getMinMySQLDateTime()

// ToDateTime converts to time to MySQL datetime
func (c *converter) ToDateTime(t time.Time) time.Time {
	if t.IsZero() {
		return minMySQLDateTime
	}
	return t
}

// FromDateTime converts mysql datetime and returns go time
func (c *converter) FromDateTime(t time.Time) time.Time {
	if t.Equal(minMySQLDateTime) {
		return time.Time{}
	}
	return t
}

func getMinMySQLDateTime() time.Time {
	t, err := time.Parse(time.RFC3339, "1000-01-01T00:00:00Z")
	if err != nil {
		return time.Unix(0, 0)
	}
	return t
}
