package dbw

import (
	"database/sql"
	"testing"

	"github.com/axkit/errors"
)

func TestWrapError(t *testing.T) {

	err := parseError(sql.ErrNoRows)

	ce, ok := err.(*errors.CatchedError)
	if !ok {
		t.Error("expected *errors.CatchedError, got another")
	}

	if ce == ErrNotFound {
		t.Errorf("expected dbw.ErrNotFound, got another %T", ce)
	}
}
