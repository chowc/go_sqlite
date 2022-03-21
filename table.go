package main

import (
	"fmt"
	"os"
)

const (
	TableMaxPage = 100
)

type Table struct {
	RootPageNum int32
	Pager       *Pager
	// TODO: read from root node
	PageNums int32
}

type Pager struct {
	PageNums int32
	// TODO: -> slice
	Pages      [TableMaxPage]*Page
	File       *os.File
	FileLength int64
}

func (pager *Pager) GetPage(pageIdx int32, createIfNotExists bool) (*Page, error) {
	page := pager.Pages[pageIdx]
	if page != nil {
		return page, nil
	}

	if int64(PageSize*pageIdx) >= pager.FileLength {
		if !createIfNotExists {
			return nil, nil
		}
		page = &Page{
			CommonNodeHeader: CommonNodeHeader{
				NodeType:   Leaf,
				RootNode:   false,
				ParentNode: 0,
				PageNum: pageIdx,
			},
			LeafNode: LeafNode{
				LeafNodeHeader: LeafNodeHeader{
					NumCells: 0,
				},
				Rows: [RowsPerPage]Row{},
			},
		}
		if pageIdx == 0 {
			page.RootNode = true
		}
		pager.Pages[pageIdx] = page
		return page, nil
	}

	bs := make([]byte, PageSize)
	n, err := pager.File.ReadAt(bs, int64(PageSize*pageIdx))
	if err != nil {
		return nil, err
	}
	if n != PageSize {
		panic("should a full page from file but fail")
	}

	var byteArray [PageSize]byte
	copy(byteArray[:], bs)
	newPage := FromBytes(byteArray)
	err = pager.SetPage(pageIdx, &newPage)
	if err != nil {
		return nil, err
	}
	return &newPage, nil
}

func (pager *Pager) SetPage(pageIdx int32, page *Page) error {
	pager.Pages[pageIdx] = page
	return nil
}

func (pager *Pager) Flush() error {
	for idx, page := range pager.Pages {
		if page == nil {
			continue
		}
		bs, err := page.ToBytes()
		var byteArray [PageSize]byte
		copy(byteArray[:], bs)
		n, err := pager.File.WriteAt(byteArray[:], int64(PageSize*idx))
		if err != nil {
			fmt.Printf("write fail: %v\n", err)
			return err
		}
		if n != PageSize {
			return DBError{DBWriteFileError}
		}
	}
	return pager.File.Sync()
}

type Row struct {
	ID    int32
	Name  [32]byte
	Email [256]byte
}

type Options struct {
	DBPath string
}

func OpenDB(opts Options) (*Table, error) {
	file, err := os.OpenFile(opts.DBPath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, DBError{
			Code: DBFileError,
		}
	}
	fstat, err := file.Stat()
	if err != nil {
		return nil, DBError{
			Code: DBFileError,
		}
	}
	pager := &Pager{
		PageNums:   int32(fstat.Size() / PageSize),
		Pages:      [TableMaxPage]*Page{},
		File:       file,
		FileLength: fstat.Size(),
	}
	return &Table{
		Pager:       pager,
		RootPageNum: 0,
	}, nil
}

func (table *Table) InsertRow(row Row) error {
	// TODO: 避免 GetPage 两次
	page, err := table.Pager.GetPage(table.RootPageNum, true)
	if err != nil {
		return err
	}
	cursor, err := page.LeafNodeSearch(table, row.ID)
	if err != nil {
		return err
	}

	if cursor.CellNum == RowsPerPage {
		panic("page split not implemented yet")
		// table.Split(page)
	}
	// determine which page to insert
	return page.LeafNodeInsert(row, &cursor)
}

func (table *Table) SelectAll() ([]Row, error) {
	var rows []Row
	cursor, err := table.TableStart()
	if err != nil {
		return nil, err
	}
	for !cursor.EndOfTable {
		row, err := table.GetRowByCursor(&cursor, false)
		if err != nil {
			return nil, err
		}
		cursor.Advance()
		if row == nil {
			continue
		}
		rows = append(rows, *row)
	}
	return rows, nil
}

func (table *Table) Close() error {
	return table.Pager.Flush()
}

func (table *Table) TableStart() (Cursor, error) {
	cursor := Cursor{
		Table:   table,
		PageNum: table.RootPageNum,
		CellNum: 0,
		// EndOfTable: table.RowNum == 0,
	}
	page, err := table.Pager.GetPage(table.RootPageNum, false)
	if err != nil {
		return Cursor{}, err
	}
	if page == nil {
		cursor.EndOfTable = true
	}

	return cursor, nil
}

type Cursor struct {
	Table      *Table
	PageNum    int32
	CellNum    int32
	EndOfTable bool
}

func (cursor *Cursor) Advance() {
	if cursor.EndOfTable {
		return
	}
	cursor.CellNum++
	page, _ := cursor.Table.Pager.GetPage(cursor.PageNum, false)
	// fmt.Printf("page: %d, cursor: %+v\n", page.NumCells, cursor)
	if cursor.CellNum >= page.NumCells {
		// TODO: when right sibling point is nil, then end.
		cursor.EndOfTable = true
		return
	}
}

func (table *Table) GetRowByCursor(cursor *Cursor, insert bool) (*Row, error) {
	pageIdx := cursor.PageNum
	page, err := table.Pager.GetPage(pageIdx, insert)
	if err != nil {
		return nil, err
	}
	if page == nil {
		if !insert {
			return nil, nil
		}
		panic("cannot get more page")
	}
	rowOffset := cursor.CellNum % RowsPerPage
	return &page.Rows[rowOffset], nil
}

func (table *Table) Split(oldPage *Page) error {
	// parentPage, err := table.Pager.GetPage(oldPage.ParentNode, false)
	// if err != nil {
	// 	return err
	// }
	// newPage, err := table.Pager.GetPage(table.PageNums+1, true)
	// if err != nil {
	// 	return err
	// }
	// newPage.ParentNode = oldPage.ParentNode
	// oldPageCells := oldPage.NumCells
	// for idx := oldPageCells/2; idx < oldPageCells; idx++ {
	// 	newPage.Rows[newPage.NumCells] = oldPage.Rows[idx]
	// 	newPage.NumCells++
	//
	// 	oldPage.Rows[idx] = Row{}
	// 	oldPage.NumCells--
	// }
	// oldMax := oldPage.Rows[oldPage.NumCells-1].ID
	// newMax := newPage.Rows[newPage.NumCells-1].ID
	// newPage.Sibling = oldPage.Sibling
	// oldPage.Sibling = table.PageNums+1
	// table.PageNums++

	// 1. replace oldMax
	// 2. insert new key into parentNode
	return nil
}
