package dbw

import (
	"testing"
)

func TestTable_fieldNamesUpdate(t *testing.T) {

	type Header struct {
		ID         int
		Name       string
		UpdatedAt  NullTime
		RowVersion int
	}

	type Row struct {
		Header
		Permissions []string `dbw:"perms"`
	}

	tbl := NewTable(&DB{}, "x", &Row{})
	s := tbl.fieldNamesUpdate(&Row{}, "perms", Include)
	t.Log(s)

	t.Log(tbl.fieldNamesUpdate(&Header{}, "", All))

}

func TestTable_doUpdateTx(t *testing.T) {

	type Header struct {
		ID            int
		Name          string   `dbw:"name"`
		Description   NullTime `dbw:"desc"`
		JiraTicket    string
		CustomerEmail string `dbw:"customer_email"`
		RowVersion    int
	}

	var row Header

	tbl := NewTable(&DB{}, "x", &row)
	s := tbl.fieldNamesUpdate(&row, "name,desc", Include)
	t.Log(s)
}
