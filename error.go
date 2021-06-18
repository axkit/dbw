package dbw

import (
	"fmt"
	"runtime"

	"github.com/axkit/errors"
	"github.com/lib/pq"
	"github.com/rs/zerolog"
)

type targetType string

const (
	targetTable targetType = "table"
	targetQuery targetType = "query"
)

// Error describes error raising by SQL queries related to a single table
// represented by *Table or by arbitrary SQL query.
type Error struct {
	targetType targetType
	table      string
	sqlQuery   string
	sqlErrCode string
	sqlerr     error
	params     []interface{}
	frames     []runtime.Frame
}

func (e Error) Error() string {
	s := "query execution failed: " + e.sqlerr.Error() + "("
	if e.targetType == targetTable {
		s += string(e.targetType) + ": " + e.table + ";"
	}

	if e.sqlErrCode != "" {
		s += " [code: " + e.sqlErrCode + "]"
	}
	s += " sql: " + e.sqlQuery
	s += ")"
	return s
}

func (err Error) MarshalZerologObject(e *zerolog.Event) {
	switch err.targetType {
	case targetTable:
		e.Str("object", err.table)
	case targetQuery:
		e.Str("object", "sqlQuery")
	}
	e.Str("sql", err.sqlQuery).Str("sqlerrcode", err.sqlErrCode).Str("sqlerrmsg", err.Error())

	if len(err.params) > 0 {
		s := "["
		for i := range err.params {
			s += fmt.Sprintf("%v,", err.params[i])
		}
		s += "]"
		e.Str("params", s)
	}
}

func WrapError(t interface{}, err error, params ...interface{}) error {
	if err == nil {
		return nil
	}

	ce := errors.Catch(err).Severity(errors.Critical)

	switch t.(type) {
	case *string:
		ce.Set("target", targetQuery)
		ce.Set("sql", *t.(*string))
	case string:
		ce.Set("target", targetQuery)
		ce.Set("sql", t.(string))
	case *Table:
		ce.Set("target", targetTable)
		ce.Set("sql", t.(*Table).name)
	default:
		break
	}

	if pgerr, ok := err.(*pq.Error); ok {
		ce.Set("code", pgerr.Code)
		//e.sqlErrCode = string(pgerr.Code)
	}
	if len(params) > 0 {
		ce.SetVals("params", params...)
	}

	//e.params = append(e.params, params...)

	return ce
}

// WrapError .
func WrapErrorOld(t interface{}, err error, params ...interface{}) error {
	if err == nil {
		return nil
	}

	e := Error{sqlerr: err}
	switch t.(type) {
	case *string:
		e.targetType = targetQuery
		e.sqlQuery = *t.(*string)
	case string:
		e.targetType = targetQuery
		e.sqlQuery = t.(string)
	case *Table:
		e.targetType = targetTable
		e.table = t.(*Table).name
	default:
		break
	}

	if pgerr, ok := err.(*pq.Error); ok {
		e.sqlErrCode = string(pgerr.Code)
	}

	e.params = append(e.params, params...)

	return e
}

func (e *Error) SQLErrCode() string {
	return e.sqlErrCode
}
