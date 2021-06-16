package dbw

import (
	"context"
)

type StmtLogger interface {
	// New(s Statementer, ctx context.Context, qparams string) *StmtInstance
	// NewTx(tx *Tx, s Statementer, ctx context.Context, qparams string) *StmtInstance
	Before(*StmtInstance)
	After(*StmtInstance)
	ContextFormat(ctx context.Context) string
	ArgsFormat(args ...interface{}) string
}

type DefaultStmtLogger struct {
}

func NewDefaultStmtLogger() *DefaultStmtLogger {
	return &DefaultStmtLogger{}
}

func (dsl *DefaultStmtLogger) Before(sli *StmtInstance) {

}

func (dsl *DefaultStmtLogger) After(sli *StmtInstance) {

}

func (dsl *DefaultStmtLogger) ContextFormat(ctx context.Context) string {

	return ""
}

func (dsl *DefaultStmtLogger) ArgsFormat(args ...interface{}) string {

	return ""
}

/*
func (dsl *DefaultStmtLogger) New(s *Stmt, ctx context.Context, qparams string) *StatementInstance {
	sli := s.Instance()
	sli.sl = dsl
	sli.CtxParams = dsl.fmtFunc(ctx)
	sli.QueryParams = qparams
	return sli
}

func (dsl *DefaultStmtLogger) NewTx(t *Tx, s *Stmt, ctx context.Context, qparams string) *StatementInstance {
	sli := s.Instance()
	sli.sl = dsl
	sli.CtxParams = dsl.fmtFunc(ctx)
	sli.QueryParams = qparams
	sli.TxSeq = t.Num()
	return sli
}
*/
