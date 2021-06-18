package dbw

import (
	"context"
	"database/sql"
	"strconv"
	"strings"

	"github.com/axkit/errors"
	"github.com/rs/zerolog"
)

// AutoCheckTableExistance checks existance of table in NewTable() call.
var AutoCheckTableExistance bool = true

type Table struct {
	ctx     context.Context
	db      *DB
	name    string
	columns string

	withDeletedAt  bool
	withRowVersion bool
	withUpdatedAt  bool
	model          interface{}

	// true, if there is a sequence for ID column values
	isSequenceUsed bool

	// if true, stores previous version of row in special audit table.
	isAuditRequired bool

	SQL struct {
		SelectCache               string
		SelectCacheWithoutDeleted string
		Select                    string
		Insert                    string
		BasicUpdate               string
		HardDelete                string
		SoftDelete                string
		ExistByID                 string
		ExistByUID                string
		UpdateRowVersion          string
		SelectByID                string
		FlexDelete                string
		SelectCount               string
	}

	// coltag holds struct field name and field's tags
	coltag map[string]map[string]string

	log zerolog.Logger
}

// New automatically recognises autoincrement using sequecne by existance of
// field ID with type int? If there is no autoicrement requrements set tag "noseq".
// It extected, that sequence used for autoincrement has "tablename_seq".
func New(ctx context.Context, db *DB, name string, model interface{}) (*Table, error) {
	t := &Table{
		ctx:            ctx,
		db:             db,
		name:           name,
		model:          model,
		isSequenceUsed: HasAutoincrementFieldID(model),
	}

	t.SetLogger(db.Logger())

	//t.log.Debug().Msg("table.New() " + name + " ")

	if AutoCheckTableExistance {
		if err := t.CheckTableExistance(); err != nil {
			return nil, WrapError(t, err)
		}
	}

	t.columns = t.fieldNames(model, "", All)

	t.initColTag(model)

	t.withDeletedAt = strings.Contains(t.columns, "DeletetAd")
	t.withRowVersion = strings.Contains(t.columns, "RowVersion")
	t.withUpdatedAt = strings.Contains(t.columns, "UpdatedAt")

	t.genSQL()

	return t, nil
}

func NewTable(db *DB, name string, model interface{}) *Table {
	t := &Table{
		ctx:            context.Background(),
		db:             db,
		name:           name,
		model:          model,
		isSequenceUsed: HasAutoincrementFieldID(model),
	}

	t.SetLogger(db.Logger())

	t.columns = t.fieldNames(model, "", All)
	t.initColTag(model)

	t.withDeletedAt = strings.Contains(t.columns, "DeletetAd")
	t.withRowVersion = strings.Contains(t.columns, "RowVersion")
	t.withUpdatedAt = strings.Contains(t.columns, "UpdatedAt")

	t.genSQL()

	return t
}

func (t *Table) DB() *DB {
	return t.db
}

// SetAudit activates or diactivates audit mode for the table.
func (t *Table) SetAudit(b bool) {
	(*t).isAuditRequired = b
}

func (t *Table) Name() string {
	return t.name
}

func (t *Table) SetLogger(l *zerolog.Logger) {
	t.log = l.With().Str("table", t.name).Logger()
}

func (t *Table) genSQL() {

	allf := t.fieldNames(t.model, "", All)

	t.SQL.Select = "SELECT " + allf + " FROM " + t.name + " "
	t.SQL.SelectCache = "SELECT " + t.fieldNames(t.model, TagNoCache, Exclude) + " FROM " + t.name + " "
	t.SQL.SelectCacheWithoutDeleted = "SELECT " + t.fieldNames(t.model, TagNoCache, Exclude) + " FROM " + t.name

	if t.withDeletedAt {
		t.SQL.SelectCacheWithoutDeleted += " WHERE deleted_at IS NULL"
	}

	t.SQL.SelectByID = t.SQL.Select + " WHERE id=$1"
	t.SQL.ExistByID = "SELECT 1 FROM " + t.name + " WHERE id=$1"
	t.SQL.ExistByUID = "SELECT 1 FROM " + t.name + " WHERE uid=$1"
	t.SQL.HardDelete = "DELETE FROM " + t.name + " WHERE id=$1"
	t.SQL.SoftDelete = "UPDATE " + t.name + " SET deleted_at=$1, row_version=row_version+1 WHERE id=$2 RETURNING row_version"
	t.SQL.FlexDelete = "DELETE FROM " + t.name + " WHERE "
	t.SQL.UpdateRowVersion = "UPDATE " + t.name + " SET updated_at=$1, row_version=row_version+1 WHERE id=$2 RETURNING row_version"
	t.SQL.SelectCount = "SELECT COUNT(*) FROM " + t.name + " "
	t.SQL.Insert = t.genInsertSQL()
	t.SQL.BasicUpdate = t.genUpdateSQL(allf)
	//t.log.Debug().Msg(t.SQL.BasicUpdate)

}

func (t *Table) FieldNames(model interface{}, tags string, rule TagExclusionRule) string {
	return t.fieldNames(model, tags, rule)
}

func (t *Table) FieldAddrs(model interface{}, tags string, rule TagExclusionRule) []interface{} {
	return t.fieldAddrs(model, tags, rule)
}

func (t *Table) FieldAddrsUpdate(model interface{}, tags string, rule TagExclusionRule) ([]interface{}, *NullTime, *int) {
	return t.fieldAddrsUpdate(model, tags, rule)
}

func (t *Table) FieldAddrsSelect(model interface{}, tags string, rule TagExclusionRule) []interface{} {
	return t.fieldAddrsSelect(model, tags, rule)
}

func (t *Table) CheckTableExistance() error {
	var res int
	qry := "SELECT 1 FROM " + t.name + " UNION ALL SELECT 1 LIMIT 1"
	si := t.db.QueryRow(qry)

	err := si.Err()
	if err == nil {
		err = si.Scan(&res)
	}
	return WrapError(t, err)
}

func (t *Table) CheckColumns() error {
	qry := t.SQL.Select + " LIMIT 1"

	si := t.db.QueryRow(qry)

	err := si.Err()
	if err == nil {
		return nil
	}
	if err == sql.ErrNoRows {
		return nil
	}

	return WrapError(t, err)
}

func (t *Table) DoUpdate(row interface{}, tags string, rule TagExclusionRule, userID int) error {
	return WrapError(t, t.doUpdateTx(t.ctx, nil, row, tags, rule, userID))
}

func (t *Table) DoUpdateCtx(ctx context.Context, row interface{}, tags string, rule TagExclusionRule, userID int) error {
	return WrapError(t, t.doUpdateTx(ctx, nil, row, tags, rule, userID))
}

func (t *Table) DoUpdateTx(tx *Tx, row interface{}, tags string, rule TagExclusionRule, userID int) error {
	return WrapError(t, t.doUpdateTx(t.ctx, tx, row, tags, rule, userID))
}

func (t *Table) DoUpdateTxCtx(ctx context.Context, tx *Tx, row interface{}, tags string, rule TagExclusionRule, userID int) error {
	return WrapError(t, t.doUpdateTx(ctx, tx, row, tags, rule, userID))
}

func (t *Table) doUpdateTx(ctx context.Context, tx *Tx, row interface{}, tags string, rule TagExclusionRule, userID int) error {
	var (
		err error
		si  *StmtInstance
		oua NullTime
		rv  int
	)

	stmtUID := t.name + tags + "update" + strconv.Itoa(int(rule))

	stmt, ok := t.db.Stmt(stmtUID)
	if !ok {
		// если не найдено в подготовленных запросах.
		s := t.genUpdateSQL(t.fieldNamesUpdate(row, tags, rule))
		stmt = t.db.PrepareContextN(ctx, s, stmtUID)
	}

	if stmt.err != nil {
		return errors.Catch(stmt.err).
			Set("table", t.name).
			Set("tags", tags).
			Set("tagrule", rule.String()).
			Msg("query preparation failed")
	}

	addrs, updatedAt, rowVersion := t.fieldAddrsUpdate(row, tags, rule)

	if tx != nil {
		si = stmt.InstanceTx(tx)
	} else {
		si = stmt.Instance()
	}

	// struct.UpdatedAt exist and updatedAt holds reference to it.
	if updatedAt != nil {
		oua = *updatedAt
		updatedAt.SetNow() // set now() at struct.UpdatedAt
	}

	if t.withRowVersion {
		// if table has row_version column
		if err = si.QueryRowContext(ctx, addrs...).Scan(&rv); err == nil {
			*rowVersion = rv
		}
	} else {
		// if table has not row_version column
		_, err = si.ExecContext(ctx, addrs...)
	}

	// return back value of struct.UpdatedAt if it existed and was not nil.
	if err != nil && updatedAt != nil {
		*updatedAt = oua
	}

	return err
}

// DoSoftDelete marks rows as deleted. Works only if table has column deleted_at, panics if not.
func (t *Table) DoSoftDelete(id interface{}, deletedAt *NullTime, rowVersion *int) error {
	return WrapError(t, t.doSoftDeleteTx(t.ctx, nil, id, deletedAt, rowVersion))
}

func (t *Table) DoSoftDeleteCtx(ctx context.Context, id interface{}, deletedAt *NullTime, rowVersion *int) error {
	return WrapError(t, t.doSoftDeleteTx(ctx, nil, id, deletedAt, rowVersion))
}

func (t *Table) DoSoftDeleteTx(tx *Tx, id interface{}, deletedAt *NullTime, rowVersion *int) error {
	return WrapError(t, t.doSoftDeleteTx(t.ctx, tx, id, deletedAt, rowVersion))
}

func (t *Table) DoSoftDeleteTxCtx(ctx context.Context, tx *Tx, id interface{}, deletedAt *NullTime, rowVersion *int) error {
	return WrapError(t, t.doSoftDeleteTx(ctx, tx, id, deletedAt, rowVersion))
}

func (t *Table) doSoftDeleteTx(ctx context.Context, tx *Tx, id interface{}, deletedAt *NullTime, rowVersion *int) error {

	var (
		si   *StmtInstance
		stmt *Stmt
	)

	if stmt = t.db.PrepareContext(ctx, t.SQL.SoftDelete); stmt.Err() != nil {
		return stmt.Err()
	}

	if tx != nil {
		si = stmt.InstanceTx(tx)
	} else {
		si = stmt.Instance()
	}

	if err := si.Err(); err != nil {
		return err
	}

	var dat NullTime
	dat.SetNow()

	err := si.QueryRowContext(ctx, &dat, id).Scan(rowVersion)
	if err == nil {
		*deletedAt = dat
	}

	return err
}

// DoHardDelete deletes row completely.
func (t *Table) DoHardDelete(id interface{}) error {
	return WrapError(t, t.doHardDeleteTxCtx(t.ctx, nil, id))
}

// DoHardDelete deletes row completely.
func (t *Table) DoHardDeleteCtx(ctx context.Context, id interface{}) error {
	return WrapError(t, t.doHardDeleteTxCtx(ctx, nil, id))
}

func (t *Table) DoHardDeleteTx(tx *Tx, id interface{}) error {
	return WrapError(t, t.doHardDeleteTxCtx(t.ctx, tx, id))
}
func (t *Table) DoHardDeleteTxCtx(ctx context.Context, tx *Tx, id interface{}) error {
	return WrapError(t, t.doHardDeleteTxCtx(ctx, tx, id))
}

func (t *Table) doHardDeleteTxCtx(ctx context.Context, tx *Tx, id interface{}) error {
	var (
		si   *StmtInstance
		stmt *Stmt
	)

	if stmt = t.db.PrepareContext(ctx, t.SQL.HardDelete); stmt.Err() != nil {
		return stmt.Err()
	}

	if tx != nil {
		si = stmt.InstanceTx(tx)
	} else {
		si = stmt.Instance()
	}

	if err := si.Err(); err != nil {
		return err
	}

	_, err := si.ExecContext(ctx, id)
	return err
}

// DoHardDel deletes row completely using condition described in where.
func (t *Table) DoHardDel(where string, args ...interface{}) (sql.Result, error) {
	sr, err := t.doHardDelTxCtx(t.ctx, nil, where, args...)
	return sr, WrapError(t, err)
}

// DoHardDelTx deletes row completely using condition described in where in a transaction.
func (t *Table) DoHardDelTx(tx *Tx, where string, args ...interface{}) (sql.Result, error) {
	sr, err := t.doHardDelTxCtx(t.ctx, tx, where, args...)
	return sr, WrapError(t, err)
}

func (t *Table) doHardDelTxCtx(ctx context.Context, tx *Tx, where string, args ...interface{}) (sql.Result, error) {
	var (
		si   *StmtInstance
		stmt *Stmt
	)

	if stmt = t.db.PrepareContext(ctx, t.SQL.FlexDelete+where); stmt.Err() != nil {
		return nil, stmt.Err()
	}

	if tx != nil {
		si = stmt.InstanceTx(tx)
	} else {
		si = stmt.Instance()
	}

	if err := si.Err(); err != nil {
		return nil, err
	}

	return si.ExecContext(ctx, args...)
}

func (t *Table) DoInsert(row interface{}, returning ...interface{}) error {
	return WrapError(t, t.doInsertTxCtx(nil, nil, row, returning...))
}

func (t *Table) DoInsertCtx(ctx context.Context, row interface{}, returning ...interface{}) error {
	return WrapError(t, t.doInsertTxCtx(ctx, nil, row, returning...))
}

func (t *Table) DoInsertTx(tx *Tx, row interface{}, returning ...interface{}) error {
	return WrapError(t, t.doInsertTxCtx(t.ctx, tx, row, returning...))
}

func (t *Table) DoInsertTxCtx(ctx context.Context, tx *Tx, row interface{}, returning ...interface{}) error {
	return WrapError(t, t.doInsertTxCtx(ctx, tx, row, returning...))
}

func (t *Table) doInsertTxCtx(ctx context.Context, tx *Tx, row interface{}, returning ...interface{}) error {
	var (
		err  error
		si   *StmtInstance
		stmt *Stmt
	)

	if ctx == nil {
		ctx = t.ctx
	}

	if stmt = t.db.PrepareContext(ctx, t.SQL.Insert); stmt.Err() != nil {
		return stmt.Err()
	}

	if tx != nil {
		if si = stmt.InstanceTx(tx); si.Err() != nil {
			return si.Err()
		}
	} else {
		si = stmt.Instance()
	}

	addrs := t.FieldAddrs(row, TagNoIns, Exclude)

	if len(returning) > 0 && t.isSequenceUsed {
		err = si.QueryRowContext(ctx, addrs...).Scan(returning...)
	} else {
		_, err = si.ExecContext(ctx, addrs...)
	}
	return err
}

func (t *Table) DoSelectCache(f func() error, row interface{}) error {
	cols := t.fieldAddrsSelect(row, TagNoCache, Exclude)
	//t.log.Debug().Str("query", t.SQL.SelectCache).Int("targets", len(cols)).Msg("select cache prepared")
	if t.isAuditRequired {
		t.log.Debug().Str("query", t.SQL.SelectCache).Int("targets", len(cols)).Msg("select cache prepared")
	}
	err := t.db.QueryContext(t.ctx, t.SQL.SelectCache).Fetch(f, cols...).Err()
	if err == nil {
		return nil
	}
	return WrapError(t, err)
}

func (t *Table) DoSelectCacheCtx(ctx context.Context, f func() error, row interface{}) error {
	cols := t.fieldAddrsSelect(row, TagNoCache, Exclude)
	return t.db.QueryContext(ctx, t.SQL.SelectCache).Fetch(f, cols...).Err()
}

func (t *Table) DoSelect(where, order string, offset, limit int, f func() error, row interface{}, params ...interface{}) error {
	return WrapError(t, t.doSelectCtx(t.ctx, where, order, offset, limit, f, row, params...))
}

func (t *Table) DoSelectCtx(ctx context.Context, where, order string, offset, limit int, f func() error, row interface{}, params ...interface{}) error {
	return WrapError(t, t.doSelectCtx(ctx, where, order, offset, limit, f, row, params...))
}

func (t *Table) doSelectCtx(ctx context.Context, where, order string, offset, limit int, f func() error, row interface{}, params ...interface{}) error {
	cols := t.fieldAddrsSelect(row, "", All)

	qry := t.SQL.Select
	if len(where) > 0 {
		qry += " WHERE " + where
	}

	if len(order) > 0 {
		qry += " ORDER BY " + order
	}

	if offset > 0 {
		qry += " OFFSET " + strconv.Itoa(offset)
	}

	if limit > 0 {
		qry += " LIMIT " + strconv.Itoa(limit)
	}

	if f == nil {
		return t.db.QueryContext(ctx, qry, params...).Scan(cols...)
	}

	return t.db.QueryContext(ctx, qry, params...).Fetch(f, cols...).Err()
}

// Count returns amount of rows in the table complaints with condition in where.
func (t *Table) Count(where string, params ...interface{}) (int, error) {
	res, err := t.count(t.ctx, nil, where, params...)
	return res, WrapError(t, err, params...)
}

func (t *Table) count(ctx context.Context, tx *Tx, where string, params ...interface{}) (int, error) {
	var cnt int
	if len(where) > 0 {
		where = " WHERE " + where
	}
	err := t.db.QueryRow(t.SQL.SelectCount+where, params...).Scan(&cnt)
	return cnt, err
}

func (t *Table) DoUpdateRowVersionCtx(ctx context.Context, id interface{}, updatedAt *NullTime, rowVersion *int) error {
	return WrapError(t, t.doUpdateRowVersion(ctx, nil, id, updatedAt, rowVersion))
}

func (t *Table) DoUpdateRowVersionTx(tx *Tx, id interface{}, updatedAt *NullTime, rowVersion *int) error {
	return WrapError(t, t.doUpdateRowVersion(t.ctx, tx, id, updatedAt, rowVersion))
}

func (t *Table) DoUpdateRowVersionTxCtx(ctx context.Context, tx *Tx, id interface{}, updatedAt *NullTime, rowVersion *int) error {
	return WrapError(t, t.doUpdateRowVersion(ctx, tx, id, updatedAt, rowVersion))
}

func (t *Table) doUpdateRowVersion(ctx context.Context, tx *Tx, id interface{}, updatedAt *NullTime, rowVersion *int) error {

	var (
		si   *StmtInstance
		stmt *Stmt
	)

	if stmt = t.db.PrepareContext(ctx, t.SQL.UpdateRowVersion); stmt.Err() != nil {
		return stmt.Err()
	}

	if tx != nil {
		si = stmt.InstanceTx(tx)
	} else {
		si = stmt.Instance()
	}

	if err := si.Err(); err != nil {
		return err
	}

	var dat NullTime
	dat.SetNow()

	err := si.QueryRowContext(ctx, &dat, id).Scan(rowVersion)
	if err == nil {
		*updatedAt = dat
	}

	return err
}

func (t *Table) DoSelectByID(id interface{}, row interface{}) error {
	return WrapError(t, t.doSelectByIDCtx(t.ctx, id, row))
}
