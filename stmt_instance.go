package dbw

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"time"

	"github.com/axkit/errors"
	"github.com/lib/pq"
)

// StmtInstance
type StmtInstance struct {
	stmt *Stmt

	sqlStmt *sql.Stmt
	// At holds time when instance created (sent to database).
	At time.Time

	// RespondedIn holds duration between At and moment when 1st row retrived.
	RespondedIn time.Duration

	// RowsFetched holds numner or rows fetched by SELECT.
	RowsFetched int

	// RowsFetchedIn holds duration between getting 1st and last rows.
	RowsFetchedIn time.Duration

	// QueryParams holds SQL query parameters.
	QueryParams string

	// CtxParams holds context/session related parameters.
	CtxParams string

	tx *Tx

	// Num holds SQL statement sequential number since application start.
	Num uint64

	// id holds unique sql statement number.
	id uint64

	err error

	result sql.Result
	row    *sql.Row
	rows   *sql.Rows
}

func newStmtInstance(stmt *Stmt, num uint64, err error) *StmtInstance {
	return &StmtInstance{stmt: stmt, sqlStmt: stmt.sqlStmt, Num: num, err: err}
}

func newStmtInstanceTx(tx *Tx, stmt *Stmt, num uint64, err error) *StmtInstance {
	return &StmtInstance{stmt: stmt, sqlStmt: tx.SQLTx().Stmt(stmt.sqlStmt), Num: num, err: err, tx: tx}
}

func (si *StmtInstance) ViaTx(tx *Tx) *StmtInstance {
	si.sqlStmt = tx.SQLTx().Stmt(si.sqlStmt)
	si.tx = tx
	return si
}

func (si *StmtInstance) responded(err error) *StmtInstance {
	si.RespondedIn = time.Since(si.At)
	si.err = err
	return si
}

// Scan read values from database driver to dest.
func (si *StmtInstance) Scan(dest ...interface{}) error {
	if si.err != nil {
		return si.err
	}

	nfmsg := ""
	switch {
	case si.rows != nil:
		si.err = si.rows.Scan(dest...)
		nfmsg = "rows not found"
	case si.row != nil:
		si.err = si.row.Scan(dest...)
		nfmsg = "row not found"
	default:
		si.err = errors.New("invalid Scan() call")
	}

	if si.err == nil {
		return nil
	}

	switch si.err == sql.ErrNoRows {
	case true:
		si.err = errors.NotFound(nfmsg)
	case false:
		si.err = errors.Catch(si.err).Severity(errors.Critical).StatusCode(500)
	}

	return si.err
}

// ScanAll
func (si *StmtInstance) Fetch(f func() error, dest ...interface{}) *StmtInstance {

	if si.err != nil {
		return si
	}

	// TODO: сохранять состояние при закрытии запроса
	defer si.rows.Close()

	for si.rows.Next() {
		if si.err = si.rows.Scan(dest...); si.err != nil {
			return si
		}

		if f != nil {
			if si.err = f(); si.err != nil {
				return si
			}
		}

		(*si).RowsFetched++
	}

	(*si).RowsFetchedIn = time.Now().Sub(si.At.Add(si.RespondedIn))
	return si
}

// Close closes statement instance with preserving error if was exist.
func (si *StmtInstance) Close() error {
	if si.rows != nil {
		err := si.rows.Close()
		// preserve existing error
		if si.err == nil {
			si.err = err
		}
		return si.err
	}

	return nil
}

// FetchAll automatically stores fetched rows into the slice arr.
func (si *StmtInstance) FetchAll(arr interface{}, row interface{}, dest ...interface{}) *StmtInstance {

	if si.err != nil {
		return si
	}

	// TODO: сохранять состояние при закрытии запроса
	defer si.rows.Close()

	for si.rows.Next() {
		if si.err = si.rows.Scan(dest...); si.err != nil {
			return si
		}

		Append(arr, row)

		si.RowsFetched++
	}

	si.RowsFetchedIn = time.Now().Sub(si.At.Add(si.RespondedIn))
	return si
}

// ScanAll
/*func (si *StmtInstance) SimpleFetchAll(arr interface{}, dest ...interface{}) *StmtInstance {

	if si.err != nil {
		return si
	}

	// TODO: сохранять состояние при закрытии запроса
	defer si.rows.Close()

	for si.rows.Next() {
		if si.err = si.rows.Scan(dest...); si.err != nil {
			return si
		}

		if si.err = f(); si.err != nil {
			return si
		}

		si.RowsFetched++
	}

	si.RowsFetchedIn = time.Now().Sub(si.At.Add(si.RespondedIn))
	return si
}
*/

// SaveStat
func (si *StmtInstance) saveStat() *StmtInstance {
	return si
}

func (si *StmtInstance) Err() error {
	return si.err
}

// Exec executes SQL command represented by StmtInstance. A SQL command does not return any values.
func (si *StmtInstance) Exec(args ...interface{}) (sql.Result, error) {
	return si.ExecContext(context.Background(), args...)
}

// ExecContext executes SQL command what not returns data back.
func (si *StmtInstance) ExecContext(ctx context.Context, args ...interface{}) (sql.Result, error) {
	if si.err != nil {
		return nil, si.err
	}

	si.At = time.Now()

	si.result, si.err = si.sqlStmt.ExecContext(ctx, args...)
	si.RespondedIn = time.Since(si.At)
	if si.err != nil {
		si.err = errors.Catch(si.err).
			SetVals("params", args...).
			Severity(errors.Medium).
			StatusCode(500).
			Msg("dbw: sql exec failed")
	}
	si.saveStat()
	return si.result, si.err
}

func (si *StmtInstance) QueryRow(args ...interface{}) *StmtInstance {
	return si.QueryRowContext(context.Background(), args...)
}

func (si *StmtInstance) QueryRowContext(ctx context.Context, args ...interface{}) *StmtInstance {
	if si.err != nil {
		return si
	}
	si.rows = nil
	si.At = time.Now()
	// there is no option to get acess to si.row.err immediately, it can be access only in Scan()
	si.row = si.sqlStmt.QueryRowContext(ctx, args...)

	if err := si.row.Err(); err != nil {
		ce := errors.Catch(err).
			Set("query", si.stmt.text).
			Severity(errors.Critical).
			SetVals("params", args...)
		perr, ok := err.(*pq.Error)
		if ok {
			ce.Set("code", string(perr.Code))
			ce.Set("constraint", perr.Constraint)
		}
		si.err = ce.Msg("dbw: sql query failed")
	}

	si.RespondedIn = time.Since(si.At)
	si.saveStat()
	return si
}

func (si *StmtInstance) QueryRowTx(tx *Tx, ctx context.Context, args ...interface{}) *StmtInstance {
	si.At = time.Now()
	si.row = tx.SQLTx().StmtContext(ctx, si.sqlStmt).QueryRowContext(ctx, args...)
	si.RespondedIn = time.Since(si.At)
	si.saveStat()
	return si
}

func (si *StmtInstance) QueryContext(ctx context.Context, args ...interface{}) *StmtInstance {

	if si.err != nil {
		return si
	}

	var err error
	si.At = time.Now()
	if si.rows, err = si.sqlStmt.QueryContext(ctx, args...); err != nil {
		ce := errors.Catch(err).
			Set("query", si.stmt.text).
			Severity(errors.Critical).
			SetVals("params", args...)
		fmt.Printf("\n%T\n", err)
		perr, ok := err.(*pq.Error)
		if ok {
			ce.Set("code", perr.Code)
		}
		si.err = ce.Msg("dbw: sql query failed")
	}

	si.RespondedIn = time.Since(si.At)
	si.saveStat()
	return si
}

func (si *StmtInstance) Query(args ...interface{}) *StmtInstance {
	return si.QueryContext(context.Background(), args...)
}

func (si *StmtInstance) Debug() *StmtInstance {
	fmt.Printf("Fetched rows %d. First: %s, all: %s; %s\n", si.RowsFetched, si.RespondedIn, si.RowsFetchedIn, si.stmt.text)
	return si
}

func (si *StmtInstance) Tx() *Tx {
	return si.tx
}

// Append appends elem into arr array.
func Append(arr interface{}, elem interface{}) error {
	value := reflect.ValueOf(arr).Elem()
	value.Set(reflect.Append(value, reflect.ValueOf(elem)))
	return nil
}
