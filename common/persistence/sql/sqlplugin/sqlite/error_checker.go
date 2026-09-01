package sqlite

import (
	"context"
	"database/sql"
	"errors"

	"github.com/ncruces/go-sqlite3"
)

// IsDupEntryError verify if the error is a duplicate entry error
func (mdb *DB) IsDupEntryError(err error) bool {
	var sqlErr *sqlite3.Error
	if ok := errors.As(err, &sqlErr); !ok {
		return false
	}

	switch sqlErr.ExtendedCode() {
	case
		// https://sqlite.org/rescode.html#constraint_unique
		sqlite3.CONSTRAINT_UNIQUE,

		// https://sqlite.org/rescode.html#constraint_primarykey
		sqlite3.CONSTRAINT_PRIMARYKEY:
		return true
	}

	return false
}

// IsNotFoundError verify if the error is a not found error
func (mdb *DB) IsNotFoundError(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}

// IsTimeoutError verify if the error is a timeout error
func (mdb *DB) IsTimeoutError(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	var sqlErr *sqlite3.Error
	if ok := errors.As(err, &sqlErr); !ok {
		return false
	}

	// https://sqlite.org/rescode.html#busy_timeout
	if sqlErr.Timeout() {
		return true
	}

	// https://sqlite.org/rescode.html#interrupt
	if sqlErr.Code() == sqlite3.INTERRUPT {
		return true
	}

	return false
}

// IsThrottlingError verify if the error is a throttling error
func (mdb *DB) IsThrottlingError(_ error) bool {
	return false
}
