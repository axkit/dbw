package dbw

import (
	"context"
	"sync/atomic"

	"github.com/axkit/errors"
)

// Sequence describes database object sequence and provides
// method to generate next value.
//
// Important: An object must not be used with tables column ID (int?) with
// no tag "noseq". Because for this table value generates automatically.
type Sequence struct {
	db         *DB
	name       string
	nextValSQL string
	lastValue  int64
	isPrepared bool
}

// NewSequence creates Sequence object.
func NewSequence(db *DB, name string) *Sequence {
	s := &Sequence{
		db:         db,
		name:       name,
		nextValSQL: "SELECT NEXTVAL('" + name + "')",
	}

	return s
}

// CheckExistance checks sequence existance.
func (s *Sequence) CheckExistance() error {
	err := errors.Catch(s.db.PrepareN(s.nextValSQL, s.name).Err()).
		Severity(errors.Critical).
		Set("seq", s.name).
		Msg("dbw: sequence existance check failed")
	s.isPrepared = (err == nil)
	return err
}

// NextVal returns next sequence value.
func (s *Sequence) NextVal(ctx context.Context) (int64, error) {

	if !s.isPrepared {
		if err := s.CheckExistance(); err != nil {
			return 0, err
		}
	}

	// always returns true as second value, because it's prepared above in
	// CheckExistance()
	stmt, _ := s.db.Stmt(s.name)

	var result int64
	err := errors.Catch(stmt.Instance().QueryRowContext(ctx).Scan(&result)).
		StatusCode(500).
		Set("seq", s.name).
		Msg("dbw: sequence nextval failed")

	if err == nil {
		atomic.StoreInt64(&s.lastValue, result)
	}
	return result, err
}
