package dbw

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type Option struct {
	tx  *Tx
	ctx context.Context

	// returnColNames holds column names to be returned
	// after command execution in section RETURNING
	returnColNames []string
	returnDests    []interface{}
	noReturningAll bool

	updateTag     string
	updateTagRule TagExclusionRule
	updateCols    map[string]interface{}
	updateRow     interface{}

	withID                   interface{}
	withCondition            string
	conditionParams          []interface{}
	ignoreConflictConditions string

	returnAllDest interface{}
}

func WithTx(tx *Tx) func(*Option) {
	return func(s *Option) {
		s.tx = tx
	}
}

func WithCtx(ctx context.Context) func(*Option) {
	return func(s *Option) {
		s.ctx = ctx
	}
}

func WithReturnID(dst interface{}) func(*Option) {
	return func(s *Option) {
		if s.noReturningAll {
			panic("WithReturnAll() called before")
		}
		s.returnDests = append(s.returnDests, dst)
		s.returnColNames = append(s.returnColNames, "id")
	}
}

func WithoutReturnAll() func(*Option) {
	return func(s *Option) {
		if len(s.returnDests) > 0 {
			panic("WithReturn***() called before")
		}
		s.noReturningAll = true
	}
}

func WithReturnAll(dst interface{}) func(*Option) {
	return func(s *Option) {
		if len(s.returnDests) > 0 {
			panic("WithReturn***() called before")
		}
		s.returnAllDest = dst
	}
}

func WithReturnVersion(dst interface{}) func(*Option) {
	return func(s *Option) {
		if s.noReturningAll {
			panic("WithReturnAll() called before")
		}
		s.returnDests = append(s.returnDests, dst)
		s.returnColNames = append(s.returnColNames, "row_version")
	}
}

func WithReturnDeletedAt(dst interface{}) func(*Option) {
	return func(s *Option) {
		if s.noReturningAll {
			panic("WithReturnAll() called before")
		}
		s.returnDests = append(s.returnDests, dst)
		s.returnColNames = append(s.returnColNames, "deleted_at")
	}
}

func WithIgnoreConflictColumn(column ...string) func(*Option) {
	return func(s *Option) {
		sep := ""
		s.ignoreConflictConditions = " ON CONFLICT ("
		for _, c := range column {
			s.ignoreConflictConditions += sep + c
			sep = ","
		}
		s.ignoreConflictConditions += ")"
	}
}

func WithIgnoreConflict(text string) func(*Option) {
	return func(s *Option) {
		s.ignoreConflictConditions = text
	}
}

func WithID(val interface{}) func(*Option) {
	return func(s *Option) {
		s.withID = val
	}
}

func WithWhere(cond string, val ...interface{}) func(*Option) {
	return func(s *Option) {
		s.withCondition = cond
		s.conditionParams = append(s.conditionParams, val)
	}
}

func WithTag(tag string, rule TagExclusionRule) func(*Option) {
	return func(s *Option) {
		s.updateTag = tag
		s.updateTagRule = rule
	}
}

func WithCols(cv map[string]interface{}) func(*Option) {
	return func(s *Option) {
		s.updateCols = cv
	}
}

func WithRow(row interface{}) func(*Option) {
	return func(s *Option) {
		s.updateRow = row
	}
}

func (t *Table) Insert(row interface{}, optFunc ...func(*Option)) error {

	option := Option{}
	for i := range optFunc {
		optFunc[i](&option)
	}

	return parseError(t.insert(row, option))
}

func (t *Table) insert(row interface{}, option Option) error {
	var (
		err  error
		si   *StmtInstance
		stmt *Stmt
	)

	qry := t.SQL.BasicInsert
	switch {
	case option.ignoreConflictConditions != "":
		qry += option.ignoreConflictConditions + " DO NOTHING "
		fallthrough
	case option.returnAllDest != nil:
		qry += " RETURNING " + t.columns
		option.returnDests = t.fieldAddrs(row, "", All)
		break
	case len(option.returnDests) > 0:
		qry += " RETURNING " + strings.Join(option.returnColNames, ",")
	default:
		break
	}
	if option.ctx == nil {
		option.ctx = context.Background()
	}

	stmt = t.db.PrepareContext(option.ctx, qry)

	if err := stmt.Err(); err != nil {
		return err
	}

	if option.tx != nil {
		if si = stmt.InstanceTx(option.tx); si.Err() != nil {
			return si.Err()
		}
	} else {
		si = stmt.Instance()
	}

	params := t.FieldAddrs(row, TagNoIns, Exclude)

	switch {
	case len(option.returnDests) > 0:
		err = si.QueryRowContext(option.ctx, params...).Scan(option.returnDests...)
	default:
		_, err = si.ExecContext(option.ctx, params...)
	}
	return err
}

func (t *Table) Delete(optFunc ...func(*Option)) error {

	option := Option{}
	for i := range optFunc {
		optFunc[i](&option)
	}

	return parseError(t.delete(&option))
}

func (t *Table) delete(option *Option) error {
	var (
		err  error
		si   *StmtInstance
		stmt *Stmt
	)

	pos := len(option.conditionParams) + 1
	if option.withID != nil {
		pos++
	}

	qry := "UPDATE " + t.name + " SET deleted_at=$" + strconv.Itoa(pos)
	if t.withRowVersion {
		qry += ", row_version=row_version+1"
	}
	qry += " WHERE true "

	if option.withCondition != "" {
		qry += " AND " + option.withCondition
	}

	if option.withID != nil {
		qry += " AND id = $" + strconv.Itoa(len(option.conditionParams)+1)
		option.conditionParams = append(option.conditionParams, option.withID)
	}

	// add deleted_at value
	option.conditionParams = append(option.conditionParams, time.Now())

	switch {
	case len(option.returnDests) > 0:
		qry += " RETURNING " + strings.Join(option.returnColNames, ",")
	case !option.noReturningAll:
		qry += " RETURNING " + t.columns
	default:
		break
	}

	if option.ctx == nil {
		option.ctx = context.Background()
	}

	fmt.Println(qry)

	stmt = t.db.PrepareContext(option.ctx, qry)

	if err := stmt.Err(); err != nil {
		return err
	}

	if option.tx != nil {
		if si = stmt.InstanceTx(option.tx); si.Err() != nil {
			return si.Err()
		}
	} else {
		si = stmt.Instance()
	}

	switch {
	case len(option.returnDests) > 0:
		err = si.QueryRowContext(option.ctx, option.conditionParams...).Scan(option.returnDests...)
	case option.returnAllDest != nil:
		returns := t.fieldAddrsSelect(option.returnAllDest, "", All)
		err = si.QueryRowContext(option.ctx, option.conditionParams...).Scan(returns...)
	default:
		_, err = si.ExecContext(option.ctx, option.conditionParams...)
	}

	return err
}

func (t *Table) Update(row interface{}, optFunc ...func(*Option)) error {

	option := Option{updateTagRule: All}
	for i := range optFunc {
		optFunc[i](&option)
	}

	return parseError(t.update(row, &option))
}

func (t *Table) update(row interface{}, option *Option) error {
	var (
		err  error
		si   *StmtInstance
		stmt *Stmt
	)

	cols, attrs, id := t.updateFields(row, option.updateTag, option.updateTagRule)

	qry := "UPDATE " + t.name + " SET "
	sep := ""
	for i := range cols {
		qry += sep + cols[i] + "=$" + strconv.Itoa(i+1)
		sep = ","
	}

	if t.withRowVersion {
		qry += sep + "row_version=row_version+1"
	}

	qry += " WHERE true "

	if option.withCondition != "" {
		qry += " AND " + option.withCondition
	}

	if option.withID != nil {
		qry += " AND id = $" + strconv.Itoa(len(cols)+1)
		option.conditionParams = append(option.conditionParams, option.withID)
	} else if id != nil {
		qry += " AND id = $" + strconv.Itoa(len(cols)+1)
		option.conditionParams = append(option.conditionParams, id)
	}

	fmt.Println(qry, cols, attrs, option.conditionParams)
	return nil

	// add deleted_at value
	//option.conditionParams = append(option.conditionParams, time.Now())

	switch {
	case len(option.returnDests) > 0:
		qry += " RETURNING " + strings.Join(option.returnColNames, ",")
	case !option.noReturningAll:
		qry += " RETURNING " + t.columns
	default:
		break
	}

	if option.ctx == nil {
		option.ctx = context.Background()
	}

	fmt.Println("update qry=", qry)
	return nil
	stmt = t.db.PrepareContext(option.ctx, qry)

	if err := stmt.Err(); err != nil {
		return err
	}

	if option.tx != nil {
		if si = stmt.InstanceTx(option.tx); si.Err() != nil {
			return si.Err()
		}
	} else {
		si = stmt.Instance()
	}

	switch {
	case len(option.returnDests) > 0:
		err = si.QueryRowContext(option.ctx, option.conditionParams...).Scan(option.returnDests...)
	case option.returnAllDest != nil:
		returns := t.FieldAddrs(option.returnAllDest, "", All)
		err = si.QueryRowContext(option.ctx, option.conditionParams...).Scan(returns...)
	default:
		_, err = si.ExecContext(option.ctx, option.conditionParams...)
	}

	return err
}

/*
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

*/

func (t *Table) updateFields(model interface{}, tags string, rule TagExclusionRule) ([]string, []interface{}, interface{}) {

	const ignoreColumns = " CreatedAt DeletedAt RowVersion "
	var (
		cols []string
		vals []interface{}
		id   interface{}
	)

	s := reflect.ValueOf(model).Elem()
	tof := s.Type()

	for i := 0; i < tof.NumField(); i++ {

		tf := tof.Field(i)
		sf := s.Field(i)

		// ignore private fields.
		if sf.CanSet() == false {
			continue
		}

		tag := tf.Tag.Get(FieldTagLabel)
		if tag == "-" {
			continue
		}

		if strings.Contains(ignoreColumns, tf.Name) {
			continue
		}

		if tf.Name == "ID" {
			id = sf.Addr().Interface()
			continue
		}

		//fmt.Println("tf=", tf.Name)

		if tf.Anonymous {
			var (
				ss []string
				pp []interface{}
			)
			if tf.Type.Kind() != reflect.Ptr {
				ss, pp, _ = t.updateFields(sf.Addr().Interface(), tags, rule)
			} else {
				if sf.IsNil() {
					mock := reflect.New(tf.Type.Elem())
					ss, pp, _ = t.updateFields(mock.Interface(), tags, rule)
				} else {
					ss, pp, _ = t.updateFields(sf.Interface(), tags, rule)
				}
			}
			//fmt.Println("ss=", ss, len(ss))
			if len(ss) == 0 {
				continue
			}
			cols = append(cols, ss...)
			vals = append(vals, pp...)
			continue
		}

		if tf.Name == "UpdatedAt" {
			if updatedAt, ok := sf.Addr().Interface().(*NullTime); ok {
				updatedAt.SetNow()
			} else if updatedAt, ok := sf.Addr().Interface().(*time.Time); ok {
				*updatedAt = time.Now()
			} else if updatedAt, ok := sf.Addr().Interface().(*sql.NullTime); ok {
				*updatedAt = sql.NullTime{Time: time.Now(), Valid: true}
			}
		} else {
			switch rule {
			case Exclude:
				if t.anyTagContains(tf.Name, tags) == true {
					continue
				}
			case Include:
				if t.anyTagContains(tf.Name, tags) == false {
					continue
				}
			case All:
				break
			default:
				panic("unknown tag exclusion rule")
			}
		}
		cols = append(cols, tf.Name)
		vals = append(vals, sf.Addr().Interface())
	}
	for i := range cols {
		cols[i] = t.SnakeIt(cols[i])
	}
	return cols, vals, id
}
