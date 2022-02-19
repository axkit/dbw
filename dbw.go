package dbw

import (
	"bytes"
	"context"
	"crypto/md5"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/axkit/errors"

	// подключаем реализацию драйвера postgresql.
	_ "github.com/lib/pq"
	"github.com/rs/zerolog"
)

// FieldTagLabel содержит наименование тэга используемого в аннотациях к атрибутам структуры.
var FieldTagLabel = "dbw"

// TagExclusionRule описывает тип исключения или включения атрибутов структур в SQL запрос.
type TagExclusionRule int

const (
	Exclude TagExclusionRule = 1
	Include TagExclusionRule = 2
	All     TagExclusionRule = 3
)

func (ter TagExclusionRule) String() string {
	switch ter {
	case Exclude:
		return "exclude"
	case Include:
		return "include"
	case All:
		return "all"
	}
	return ""
}

const (
	// TagNoSeq запрещает генерацию значения для поля ID, если поле типа int
	// используя последовательность tablename_SEQ.
	TagNoSeq = "noseq"

	// TagNoIns исключает поле при вставке.
	TagNoIns = "noins"

	// TagNoUpd исключает поле при Update
	TagNoUpd = "noupd"

	// TagNoCache исключает поле при чтении записей функцией DoSelectCache().
	TagNoCache = "nocache"
)

// ParamPlaceHolderType определяет тип плейсхолдера для переменных запроса.
type ParamPlaceHolderType int

const (
	// QuestionMark - вопросительный знак.
	QuestionMark ParamPlaceHolderType = iota
	// DollarPlusPosition - знак доллара и номер позиции.
	DollarPlusPosition
)

// DB wraps database/sql for better performance and logging.
// PostgreSQL RDBMS is supported only.
type DB struct {
	sqldb *sql.DB

	mux sync.RWMutex

	// ps holds prepared statements identified by name.
	ps map[string]*Stmt

	logger zerolog.Logger

	// txSeq holds random transaction number for output to log
	txSeq uint64

	stmtSeq uint64

	paramPlaceHolder ParamPlaceHolderType
}

// Open tries once to establish connection to database.
func Open(driverName, dataSourceName string) (*DB, error) {
	db, err := open(driverName, dataSourceName)
	if err == nil {
		err = db.Ping()
	}

	return db, err
}

func open(driverName, dataSourceName string) (*DB, error) {
	db := &DB{ps: make(map[string]*Stmt)}

	if driverName == "postgres" {
		db.paramPlaceHolder = DollarPlusPosition
	} else {
		db.paramPlaceHolder = QuestionMark
	}

	var err error
	db.sqldb, err = sql.Open(driverName, dataSourceName)

	return db, err
}

// Inherit creates DB object using already existing sql.DB object.
func Inherit(db *sql.DB) *DB {
	return &DB{sqldb: db,
		ps: make(map[string]*Stmt),
	}
}

func (db *DB) SetLogger(l *zerolog.Logger) {
	db.logger = l.With().Str("layer", "db").Logger()
}

// RepetableOpen tries to establish connection to database till ctx.Done() or
// success. It calls func aff() after every failed attempt.
func RepetableOpen(ctx context.Context, driverName, dataSourceName string, l *zerolog.Logger, aff func(string, int, error) time.Duration) (*DB, error) {

	a := 0
	dur := time.Second

	ctxt, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	for {
		db, err := open(driverName, dataSourceName)
		if err == nil {
			if err = db.PingContext(ctxt); err == nil {
				db.SetLogger(l)
				return db, nil
			}
		}
		a++
		if aff != nil {
			dur = aff(dataSourceName, a, err)
		}
		select {
		case _ = <-ctx.Done():
			return nil, context.Canceled
		default:
			break
		}
		time.Sleep(dur)
	}
	return nil, nil
}

func (db *DB) Close() error {
	return db.sqldb.Close()
}

func (db *DB) Ping() error {
	return db.ping(context.Background())
}

func (db *DB) PingContext(ctx context.Context) error {
	return db.ping(ctx)
}

func (db *DB) ping(ctx context.Context) error {
	return db.sqldb.PingContext(ctx)
}

func calcHash(buf []byte) string {
	return fmt.Sprintf("%x", md5.Sum([]byte(buf)))
}

// Prepare prepares SQL statement. Unique statement name is hash generated.
func (db *DB) Prepare(qry string) *Stmt {
	return db.prepareContext(context.Background(), calcHash([]byte(qry)), qry)
}

// PrepareN prepares SQL statement. Parameter uid holds user defined unique statement name.
func (db *DB) PrepareN(qry, uid string) *Stmt {
	return db.prepareContext(context.Background(), uid, qry)
}

// PrepareContext prepares SQL statement. Unique statement name is hash generated.
func (db *DB) PrepareContext(ctx context.Context, qry string) *Stmt {
	return db.prepareContext(ctx, calcHash([]byte(qry)), qry)
}

// PrepareNamed prepared statement referenced by unique name. Later,
// prepared statement can be taken from cache by uid.
// It's expected that SQL parameters are always used for performance reasons.
func (db *DB) PrepareContextN(ctx context.Context, qry, uid string) *Stmt {
	return db.prepareContext(ctx, uid, qry)
}

var ErrUnknownPreparedStatement = errors.New("unknown prepared statement")

func (db *DB) Stmt(uid string) (*Stmt, bool) {
	db.mux.RLock()
	s, ok := db.ps[uid]
	if ok {
		db.mux.RUnlock()
		return s, true
	}
	db.mux.RUnlock()
	return nil, false
}

func (db *DB) prepareContext(ctx context.Context, uid string, qry string) *Stmt {

	db.mux.RLock()
	s, ok := db.ps[uid]
	if ok {
		db.mux.RUnlock()
		return s
	}

	db.mux.RUnlock()

	qry = strings.Trim(qry, "\n\t")

	stmt := newStmt(ctx, db, uid, qry)
	if stmt.Err() != nil {
		return stmt
	}

	db.mux.Lock()
	db.ps[uid] = stmt
	db.mux.Unlock()

	return stmt
}

func (db *DB) delStmt(uid string) {
	db.mux.Lock()
	delete(db.ps, uid)
	db.mux.Unlock()
}

func (db *DB) SQLDB() *sql.DB {
	return db.sqldb
}

func (db *DB) nextTxNum() uint64 {
	return atomic.AddUint64(&db.txSeq, 1)
}

func (db *DB) nextStmtNum() uint64 {
	return atomic.AddUint64(&db.stmtSeq, 1)
}

func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) *Tx {
	return newTx(ctx, opts, db, db.nextTxNum())
}

func (db *DB) Begin() *Tx {
	return newTx(context.Background(), nil, db, db.nextTxNum())
}

func (db *DB) Logger() *zerolog.Logger {
	return &db.logger
}

func fmtArgs(args ...interface{}) string {

	var s, sep string
	for i := range args {
		s += sep + "$" + strconv.Itoa(i+1) + "="
		fmt.Sprintf("%s", args[i])
		sep = ";"
	}
	return s
}

func fmtContext(ctx context.Context) string {
	return "context=example;a=b;c=d"
}

func (db *DB) PreparedStatementCount() int {
	db.mux.RLock()
	cnt := len(db.ps)
	db.mux.RUnlock()
	return cnt
}

// CleanUnusedStatement deletes prepared statements not used last d.
func (db *DB) CleanUnusedStatement(d time.Duration) {

	var arr []*Stmt
	db.mux.RLock()
	for _, s := range db.ps {
		arr = append(arr, s)
	}
	db.mux.RUnlock()

	now := time.Now().Unix()

	for i := range arr {
		if now-arr[i].LastInstanceUnixTime() > d.Nanoseconds() {
			db.delStmt(arr[i].UID())
			_ = arr[i].Close()
		}
	}
	return
}

func (db *DB) Query(qry string, args ...interface{}) *StmtInstance {
	return db.Prepare(qry).Instance().Query(args...)
}

func (db *DB) QueryContext(ctx context.Context, qry string, args ...interface{}) *StmtInstance {
	return db.PrepareContext(ctx, qry).Instance().QueryContext(ctx, args...)
}

func (db *DB) QueryRow(qry string, args ...interface{}) *StmtInstance {
	return db.QueryRowContext(context.Background(), qry, args...)
}

func (db *DB) QueryRowContext(ctx context.Context, qry string, args ...interface{}) *StmtInstance {
	return db.PrepareContext(ctx, qry).Instance().QueryRowContext(ctx, args...)
}

func (db *DB) Exec(qry string, args ...interface{}) (sql.Result, error) {
	return db.PrepareContext(context.Background(), qry).Instance().Debug().Exec(args...)
}

func (db *DB) ExecContext(ctx context.Context, qry string, args ...interface{}) (sql.Result, error) {
	return db.PrepareContext(ctx, qry).Instance().ExecContext(ctx, args...)
}

func (db *DB) QueryTx(tx *Tx, qry string, args ...interface{}) *StmtInstance {
	return db.Prepare(qry).InstanceTx(tx).Query(args...)
}

func (db *DB) QueryContextTx(ctx context.Context, tx *Tx, qry string, args ...interface{}) *StmtInstance {
	return db.PrepareContext(ctx, qry).InstanceTx(tx).QueryContext(ctx, args...)
}

func (db *DB) QueryRowTx(tx *Tx, qry string, args ...interface{}) *StmtInstance {
	return db.Prepare(qry).InstanceTx(tx).QueryRow(args...)
}

func (db *DB) QueryRowContextTx(ctx context.Context, tx *Tx, qry string, args ...interface{}) *StmtInstance {
	return db.PrepareContext(ctx, qry).InstanceTx(tx).QueryRowContext(ctx, args...)
}

func (db *DB) ExecTx(tx *Tx, qry string, args ...interface{}) (sql.Result, error) {
	return db.PrepareContext(context.Background(), qry).InstanceTx(tx).Exec(args...)
}

func (db *DB) ExecContextTx(ctx context.Context, tx *Tx, qry string, args ...interface{}) (sql.Result, error) {
	return db.PrepareContext(ctx, qry).InstanceTx(tx).ExecContext(ctx, args...)
}

func (db *DB) SetPlaceHolderType(ph ParamPlaceHolderType) {
	db.paramPlaceHolder = ph
}

func (db *DB) PlaceHolderType() ParamPlaceHolderType {
	return db.paramPlaceHolder
}

func (db *DB) InTx(f func(*Tx) error) error {
	tx := db.Begin()
	if err := tx.Err(); err != nil {
		return errors.Catch(err).StatusCode(500).Msg("begin tx failed")
	}

	if err := f(tx); err != nil {
		tx.Rollback()
		return err
	}

	if err := tx.Commit().Err(); err != nil {
		return err
	}

	return nil
}

// MaskPostgreSQLConnectionString replaces password=12345 by password=*****.
func MaskPostgreSQLConnectionString(src string) string {

	res := []byte(src)

	from := bytes.Index(res, []byte("password"))
	if from < 0 {
		return src
	}

	from += bytes.IndexByte(res[from:], '=') + 1
	for _, c := range src[from:] {
		if c == ' ' {
			from++
		}
		break
	}

	to := bytes.IndexByte(res[from:], ' ')
	if to < 0 {
		to = len(src)
	} else {
		to += from
	}

	for i := from; i < to; i++ {
		res[i] = '*'
	}
	return string(res)
}
