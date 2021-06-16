package dbw

import (
	"context"
	"database/sql"
	"sync/atomic"
	"time"

	"github.com/axkit/errors"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
)

type Stmt struct {
	db *DB

	// uid holds hash caclucated over SQL statement text.
	uid string

	// text holds formatted SQL statement text.
	text string

	sqlStmt *sql.Stmt

	logger StmtLogger
	err    error

	// lastInstanceTime holds a time when the statement has been instatiated last time.
	lastInstanceTime int64
}

func newStmt(ctx context.Context, db *DB, uid string, qry string) *Stmt {
	s := &Stmt{uid: uid, text: qry, db: db}
	s.sqlStmt, s.err = db.SQLDB().PrepareContext(ctx, qry)
	if s.err != nil {
		if pe, ok := s.err.(*pq.Error); ok {
			s.err = errors.Catch(pe).
				Set("query", qry).
				Set("code", pe.Code).
				Set("name", pe.Code.Name()).
				Severity(errors.Critical).
				Msg("prepare sql statement failed")
		} else {
			s.err = errors.Catch(s.err).Set("query", qry).Severity(errors.Critical).Msg("prepare sql statement failed")
		}
	}

	return s
}

func (s *Stmt) Instance() *StmtInstance {
	if s.err != nil {
		return newStmtInstance(s, 0, s.err)
	}
	si := newStmtInstance(s, s.db.nextStmtNum(), nil)
	atomic.StoreInt64(&s.lastInstanceTime, time.Now().Unix())
	return si
}

func (s *Stmt) InstanceTx(tx *Tx) *StmtInstance {
	if s.err != nil {
		return newStmtInstance(s, 0, s.err)
	}
	si := newStmtInstanceTx(tx, s, s.db.nextStmtNum(), nil)
	atomic.StoreInt64(&s.lastInstanceTime, time.Now().Unix())
	return si
}

func (s *Stmt) Err() error {
	return s.err
}

func (ss *Stmt) Close() error {
	ss.db.delStmt(ss.uid)
	return errors.Catch(ss.sqlStmt.Close()).Set("query", ss.text).Msg("sql statement close failed")
}

func (s *Stmt) DB() *DB {
	return s.db
}

func (s *Stmt) LastInstanceUnixTime() int64 {
	return atomic.LoadInt64(&s.lastInstanceTime)
}

func (s *Stmt) UID() string {
	return s.uid
}

func (s *Stmt) Text() string {
	return s.text
}
