package dbw

import (
	"context"
	"sync"
)

type TableDesc struct {
	Ref **Table
	Row interface{}
}

// BindTables
func BindTables(ctx context.Context, db *DB, tt map[string]TableDesc, parallel bool) map[string]error {

	var (
		errs = make(map[string]error)
		wg   sync.WaitGroup
		mx   sync.Mutex
	)

	f := func(tn string, ref **Table, row interface{}) {
		defer wg.Done()

		tbl, err := New(ctx, db, tn, row)
		if err != nil {
			mx.Lock()
			errs[tn] = err
			mx.Unlock()
			return
		}
		tbl.SetLogger(db.Logger())
		*ref = tbl
	}

	wg.Add(len(tt))
	for tname, t := range tt {
		if parallel {
			go func() {
				f(tname, t.Ref, t.Row)
				tt[tname] = TableDesc{t.Ref, t.Row}
			}()
		} else {
			f(tname, t.Ref, t.Row)
			tt[tname] = TableDesc{t.Ref, t.Row}
		}
	}

	wg.Wait()

	if len(errs) > 0 {
		return errs
	}

	return nil
}
