package main

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"sort"
	"testing"
)

func TestInsertAndSelect(t *testing.T) {
	cleanup()
	table, err := OpenDB(Options{DBPath: "db.sqlite"})
	defer cleanup()

	assert.Nil(t, err)

	assert.Nil(t, err)

	for i := int32(0); i < RowsPerPage; i++ {
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
		assert.Equal(t, nil, err)
	}

	rows, err := table.SelectAll()
	assert.Equal(t, nil, err)
	assert.EqualValues(t, RowsPerPage, len(rows))
	for i := int32(0); i < RowsPerPage; i++ {
		name := fmt.Sprintf("name-{%d}", i)
		email := fmt.Sprintf("%s@example.com", name)
		var a [32]byte
		copy(a[:], name)
		var b [256]byte
		copy(b[:], email)

		assert.EqualValues(t, i, rows[i].ID)
		assert.EqualValues(t, name, string(bytes.Trim(rows[i].Name[:], "\x00")))
		assert.Equal(t, email, string(bytes.Trim(rows[i].Email[:], "\x00")))
	}
}

func TestInsertAndSelectInOrder(t *testing.T) {
	cleanup()
	table, err := OpenDB(Options{DBPath: "db.sqlite"})
	defer cleanup()
	assert.Nil(t, err)
	idxSlice := []int32{2, 10, 11, 3, 5, 7, 1, 4, 8, 6, 9, 0, 15, 14, 1000, 12, 10000, 9000, 8000, 7000, 6000, 5000, 4000}
	for _, idx := range idxSlice{
		name := fmt.Sprintf("name-{%d}", idx)
		email := fmt.Sprintf("%s@example.com", name)
		var a [32]byte
		copy(a[:], name)
		var b [256]byte
		copy(b[:], email)
		err := table.InsertRow(Row{
			ID:    idx,
			Name:  a,
			Email: b,
		})
		assert.Nil(t, err)
	}
	rows, err := table.SelectAll()
	assert.Nil(t, err)
	sort.Slice(idxSlice, func(i, j int) bool {
		return idxSlice[i] < idxSlice[j]
	})
	for idx, row := range rows {
		id := idxSlice[idx]
		assert.EqualValues(t, id, row.ID)
		name := fmt.Sprintf("name-{%d}", id)
		email := fmt.Sprintf("%s@example.com", name)
		var a [32]byte
		copy(a[:], name)
		var b [256]byte
		copy(b[:], email)
		assert.EqualValues(t, a, row.Name)
		assert.EqualValues(t, b, row.Email)
	}
}

func TestPersistence(t *testing.T) {
	cleanup()
	table, err := OpenDB(Options{DBPath: "db.sqlite"})
	assert.Nil(t, err)
	defer cleanup()
	idxSlice := []int32{2, 10, 11, 3, 5, 7, 1, 4, 8, 6, 9, 0, 15, 14, 1000, 12, 10000, 9000, 8000, 7000, 6000, 4000, 3000, 2000}
	for _, idx := range idxSlice{
		name := fmt.Sprintf("name-{%d}", idx)
		email := fmt.Sprintf("%s@example.com", name)
		var a [32]byte
		copy(a[:], name)
		var b [256]byte
		copy(b[:], email)
		err := table.InsertRow(Row{
			ID:    idx,
			Name:  a,
			Email: b,
		})
		assert.Nil(t, err)
	}
	assert.Nil(t, table.Close())
	table, err = OpenDB(Options{DBPath: "db.sqlite"})
	assert.Nil(t, err)

	rows, err := table.SelectAll()
	assert.Nil(t, err)
	sort.Slice(idxSlice, func(i, j int) bool {
		return idxSlice[i] < idxSlice[j]
	})
	for idx, row := range rows {
		id := idxSlice[idx]
		assert.EqualValues(t, id, row.ID)
		name := fmt.Sprintf("name-{%d}", id)
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