package main

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestTableInsert(t *testing.T) {
	cleanup()
	table, err := OpenDB(Options{DBPath: "db.sqlite"})
	defer cleanup()

	assert.Nil(t, err)
	i := int32(0)
	for ; i <= RowsPerPage; i++ {
		name := fmt.Sprintf("name-{%d}", i+1)
		email := fmt.Sprintf("%s@example.com", name)
		var a [32]byte
		copy(a[:], name)
		var b [256]byte
		copy(b[:], email)
		err := table.InsertRow(Row{
			ID:    i+1,
			Name:  a,
			Email: b,
		})
		assert.Equal(t, nil, err)
		assert.EqualValues(t, i+1, table.RowNum)
	}
}

func TestInsertAndSelect(t *testing.T) {
	table, err := OpenDB(Options{DBPath: "db.sqlite"})
	defer cleanup()

	assert.Nil(t, err)

	var name [32]byte
	copy(name[:], "john")
	var email [256]byte
	copy(email[:], "john@example.com")
	row := Row{
		ID:    1,
		Name:  name,
		Email: email,
	}
	err = table.InsertRow(row)
	assert.EqualValues(t, nil, err)
	assert.EqualValues(t, 1, table.RowNum)
	rows, err := table.SelectAll()
	assert.EqualValues(t, 1, len(rows))
	assert.EqualValues(t, 1, rows[0].ID)
	assert.EqualValues(t, "john", string(bytes.Trim(rows[0].Name[:], "\x00")))
	assert.Equal(t, "john@example.com", string(bytes.Trim(rows[0].Email[:], "\x00")))
}

func TestInsertAndSelectInOrder(t *testing.T) {
	table, err := OpenDB(Options{DBPath: "db.sqlite"})
	defer cleanup()
	assert.Nil(t, err)

	for i:=int32(1); i<10; i++ {
		name := fmt.Sprintf("name-{%d}", i)
		email := fmt.Sprintf("%s@example.com", name)
		var a [32]byte
		copy(a[:], name)
		var b [256]byte
		copy(b[:], email)
		err := table.InsertRow(Row{
			ID:    i,
			Name:  a,
			Email: b,
		})
		assert.Nil(t, err)
		assert.EqualValues(t, i, table.RowNum)
	}
	rows, err := table.SelectAll()
	assert.Nil(t, err)
	for idx, row := range rows {
		assert.EqualValues(t, idx+1, row.ID)
		name := fmt.Sprintf("name-{%d}", idx+1)
		email := fmt.Sprintf("%s@example.com", name)
		var a [32]byte
		copy(a[:], name)
		var b [256]byte
		copy(b[:], email)
		assert.EqualValues(t, a, row.Name)
		assert.EqualValues(t, b, row.Email)
	}
}

func cleanup() {
	os.Remove("db.sqlite")
}