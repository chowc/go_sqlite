package main

import (
	"bytes"
	"encoding/binary"
	"unsafe"
)

const (
	PageSize     = 1 << 12
	RowSize = int32(unsafe.Sizeof(Row{}))
	NodeTypeSize = int32(unsafe.Sizeof(Internal))
	CommonNodeHeaderSize = int32(unsafe.Sizeof(CommonNodeHeader{}))
	LeafNodeHeaderSize = int32(unsafe.Sizeof(LeafNodeHeader{}))
	InternalNodeHeaderSize = int32(unsafe.Sizeof(InternalNodeHeader{}))
	RowsPerPage  = (PageSize-CommonNodeHeaderSize-LeafNodeHeaderSize) / RowSize
	ChildSize = int32(unsafe.Sizeof(Child{}))
	ChildrenPerPage = (PageSize-CommonNodeHeaderSize-InternalNodeHeaderSize) / ChildSize
)

type Page struct {
	CommonNodeHeader
	LeafNode
	InternalNode
	// Rows [RowsPerPage]Row
}

type NodeType uint8

const (
	Internal NodeType = iota
	Leaf
)

type CommonNodeHeader struct {
	NodeType NodeType
	RootNode bool
	_ int16
	ParentNode int32
	PageNum int32
}

type LeafNodeHeader struct {
	NumCells int32
	Sibling int32
}

type InternalNodeHeader struct {
	KeyNums int32
}

type Child struct {
	// max child key
	Key int32
	PageNum int32
}

type LeafNode struct {
	LeafNodeHeader
	Rows [RowsPerPage]Row
}

type InternalNode struct {
	InternalNodeHeader
	Children [ChildrenPerPage]Child
}

func (page *Page) ToBytes() ([]byte, error) {
	headerBuf := &bytes.Buffer{}
	err := binary.Write(headerBuf, binary.BigEndian, page.CommonNodeHeader)
	if err != nil {
		return nil, err
	}
	buf := &bytes.Buffer{}
	switch page.NodeType {
	case Internal:
		err = binary.Write(buf, binary.BigEndian, page.InternalNode)
	case Leaf:
		err = binary.Write(buf, binary.BigEndian, page.LeafNode)
	}
	if err != nil {
		return nil, err
	}
	pageBytes := headerBuf.Bytes()
	pageBytes = append(pageBytes, buf.Bytes()...)
	// fmt.Printf("len: %d\n", len(pageBytes))
	// fmt.Printf("page: %+v\n", pageBytes)
	if len(pageBytes) > PageSize {
		panic("page size > PageSize, seems like a bug")
	}
	return pageBytes, nil
}

func FromBytes(bs [PageSize]byte) Page {
	nodeType := NodeType(bs[0]) // TODO: fix
	var page Page
	buf := bytes.NewBuffer(bs[:CommonNodeHeaderSize])
	err := binary.Read(buf, binary.BigEndian, &page.CommonNodeHeader)
	if err != nil {
		panic(err)
	}
	buf = bytes.NewBuffer(bs[CommonNodeHeaderSize:])
	switch nodeType {
	case Internal:
		err = binary.Read(buf, binary.BigEndian, &page.InternalNode)
	case Leaf:
		err = binary.Read(buf, binary.BigEndian, &page.LeafNode)
	}
	if err != nil {
		panic(err)
	}
	// fmt.Printf("page: %+v", page)
	return page
}

func (page *Page) LeafNodeInsert(row Row, cursor *Cursor) error {
	if page.NodeType != Leaf {
		panic("insert into internal node not implemented yet")
	}
	// if cursor.CellNum >= RowsPerPage {
	// 	return DBError{PageFull}
	// }
	if page.NumCells >= RowsPerPage {
		panic("page full")
	}

	for i:=page.NumCells; i>cursor.CellNum; i-- {
		// fmt.Printf("i=%d, page.NumCells=%d, cursor.CellNum=%d\n", i, page.NumCells, cursor.CellNum)
		page.Rows[i] = page.Rows[i-1]
	}
	page.Rows[cursor.CellNum] = row
	page.NumCells++
	return nil
}

func (page *Page) LeafNodeSearch(table *Table, key int32) (Cursor, error) {
	cursor := Cursor{
		Table:      table,
		PageNum:    page.PageNum,
		CellNum:    0,
		EndOfTable: false,
	}
	switch page.NodeType {
	case Leaf:
		left := int32(0)
		right := page.NumCells
		mid := left + (right-left)/2
		for left < right {
			midRow := page.Rows[mid]
			if midRow.ID == key {
				break
			} else if midRow.ID < key {
				left = mid+1
			} else {
				right = mid
			}
			mid = left + (right-left)/2
		}
		cursor.CellNum = left
	case Internal:
		panic("internal node search not implemented yet")
	}

	return cursor, nil
}