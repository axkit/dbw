package dbw

import (
	"testing"
	"time"
)

func TestFieldNames(t *testing.T) {

	db, err := Open("postgres", "host=localhost port=5432 dbname=postgres user=admin password=admin sslmode='disable' bytea_output='hex'")
	if err != nil {
		t.Error(err)
	}

	_, err = db.Exec(`
	create table x_haha( 
	 id 			int8 not null,  
     name 			text,
	 row_version 	int8 not null default 1,
	 created_at 	TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	 updated_at 	TIMESTAMPTZ,
	 deleted_at 	TIMESTAMPTZ,
	 constraint x_haha_pk primary key (id)	
	)`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("create sequence x_haha_seq")
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		db.Exec("drop table x_haha")
		db.Exec("drop sequence x_haha_seq")
	}()

	type Xhaha struct {
		ID         int
		Name       *string
		RowVersion int
		CreatedAt  time.Time
		UpdatedAt  *time.Time
		DeletedAt  *time.Time
	}

	tbl := NewTable(db, "x_haha", &Xhaha{})

	s := "Robert"
	row := Xhaha{}
	row.Name = &s
	row.CreatedAt = time.Now()
	if err := tbl.Insert(&row); err != nil {
		t.Error(err)
	}

	t.Logf("version before: %v", row.RowVersion)

	s = "Rimma"
	if err := tbl.Update(&row); err != nil {
		t.Error(err)
	}
	// tbl.Update().Row(&row).NoReturning()

	t.Logf("version after: %v ", row.RowVersion)

	if err := tbl.Delete(WithID(1), WithReturnVersion(&row.RowVersion)); err != nil {
		t.Error(err)
	}

	t.Logf("version after: %v", row.RowVersion)

	if err := tbl.Delete(WithID(1), WithReturnAll(&row)); err != nil {
		t.Error(err)
	}

	t.Logf("version after: %v", row.RowVersion)
}
