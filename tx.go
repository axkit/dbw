package dbw

import (
	"context"
	"database/sql"
	"time"

	_ "github.com/lib/pq"
)

// Tx describes transaction.
type Tx struct {
	db       *DB
	sqlTx    *sql.Tx
	err      error
	id       uint64
	started  time.Time
	finished time.Time
}

func newTx(ctx context.Context, opts *sql.TxOptions, db *DB, transactionID uint64) *Tx {
	tx := &Tx{db: db, id: transactionID, started: time.Now()}
	tx.sqlTx, tx.err = db.SQLDB().BeginTx(ctx, opts)
	return tx
}

// Commit commits transaction Tx.
func (tx *Tx) Commit() *Tx {
	tx.finished = time.Now()
	tx.err = tx.sqlTx.Commit()
	return tx
}

// Rollback rollbacks transaction.
func (tx *Tx) Rollback() *Tx {
	tx.finished = time.Now()
	tx.err = tx.sqlTx.Rollback()
	return tx
}

// SQLTx returns original *sql.Tx object.
func (tx *Tx) SQLTx() *sql.Tx {
	return tx.sqlTx
}

// ID returns transaction ID. The number is usefull in logging, makes able to join
// many SQL statements under a single transaction number.
func (tx *Tx) ID() uint64 {
	return tx.id
}

// Duration returns transaction duration. If transaction finished, returns transaction duration.
// If transaction is still open, returns duration from transaction start time.
func (tx *Tx) Duration() time.Duration {
	if tx.finished.IsZero() {
		return time.Since(tx.started)
	}
	return tx.finished.Sub(tx.started)
}

// Err returns error if transaction failed.
func (tx *Tx) Err() error {
	return tx.err
}
