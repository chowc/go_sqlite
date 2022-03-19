package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"unsafe"
)

const (
	PageSize     = 1 << 12
	RowSize = int32(unsafe.Sizeof(Row{}))
	RowsPerPage  = PageSize / RowSize
	TableMaxPage = 100
	TableMxRows  = TableMaxPage * RowsPerPage
)

type Table struct {
	RowNum int32
	Pager  *Pager
}

type Pager struct {
	Pages      [TableMaxPage]*Page
	File       *os.File
	FileLength int64
}

func (pager *Pager) GetPage(pageIdx int32) (*Page, error) {
	if pageIdx >= TableMaxPage {
		return nil, DBError{TableFull}
	}
	page := pager.Pages[pageIdx]
	if page != nil {
		return page, nil
	}

	if int64(PageSize*pageIdx) >= pager.FileLength {
		return nil, nil
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
	if pageIdx >= TableMaxPage {
		return DBError{TableFull}
	}
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

type Page struct {
	Rows [RowsPerPage]Row
}

func (page *Page) ToBytes() ([]byte, error) {
	buf := &bytes.Buffer{}
	err := binary.Write(buf, binary.BigEndian, page)
	if err != nil {
		return nil, err
	}
	if buf.Len() > PageSize {
		panic("page size > PageSize, seems like a bug")
	}
	return buf.Bytes(), nil
}

func FromBytes(bs [PageSize]byte) Page {
	var page Page
	buf := bytes.NewBuffer(bs[:])
	err := binary.Read(buf, binary.BigEndian, &page)
	if err != nil {
		panic(err)
	}
	return page
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
		Pages:      [100]*Page{},
		File:       file,
		FileLength: fstat.Size(),
	}
	return &Table{
		Pager:  pager,
		RowNum: int32(pager.FileLength / int64(RowSize)),
	}, nil
}

func (table *Table) InsertRow(row Row) error {
	if table.RowNum >= TableMxRows {
		return DBError{TableFull}
	}
	pageIndex := table.RowNum / RowsPerPage
	page, err := table.Pager.GetPage(pageIndex)
	if err != nil {
		return err
	}
	if page == nil {
		page = &Page{
			Rows: [RowsPerPage]Row{},
		}
		if err := table.Pager.SetPage(pageIndex, page); err != nil {
			return err
		}
	}
	rowIndex := table.RowNum % RowsPerPage
	page.Rows[rowIndex] = row
	table.RowNum++
	return nil
}

func (table *Table) GetRow(rowIndex int32) (*Row, error) {
	if rowIndex >= table.RowNum {
		return nil, DBError{Code: RowNotFound}
	}
	pageIdx := rowIndex / RowsPerPage
	page, err := table.Pager.GetPage(pageIdx)
	if err != nil {
		return nil, err
	}
	rowOffset := rowIndex % RowsPerPage
	row := page.Rows[rowOffset]
	return &row, nil
}

func (table *Table) SelectAll() ([]Row, error) {
	if table.RowNum == 0 {
		return nil, nil
	}
	var rows []Row
	for i := int32(0); i < table.RowNum; i++ {
		row, err := table.GetRow(i)
		if err != nil {
			return nil, err
		}
		if row.ID == 0 {
			continue
		}
		rows = append(rows, *row)
	}
	return rows, nil
}

func (table *Table) Close() error {
	return table.Pager.Flush()
}
