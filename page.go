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
	RowsPerPage  = (PageSize-LeafNodeHeaderSize) / RowSize
)
type Page struct {
	LeafNodeHeader
	Rows [RowsPerPage]Row
}

type NodeType uint8

const (
	Internal NodeType = iota
	Leaf
)

type CommonNodeHeader struct {
	NodeType NodeType
	RootNode bool
	ParentNode int32
}

type LeafNodeHeader struct {
	CommonNodeHeader
	NumCells int32
}

type InternalNodeHeader struct {
	CommonNodeHeader
	KeyNums int32
}

type LeafNode struct {
	LeafNodeHeader
}

type InternalNode struct {
	LeafNodeHeader
}

func (page *Page) ToBytes(nodeType NodeType) ([]byte, error) {
	switch nodeType {
	case Internal:
		panic("internal node not implemented yet")
	case Leaf:
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
	return nil, nil
}

func FromBytes(bs [PageSize]byte) Page {
	nodeType := NodeType(bs[0]) // TODO: fix
	switch nodeType {
	case Internal:
		panic("internal node not implemented yet")
	case Leaf:
		var page Page
		buf := bytes.NewBuffer(bs[:])
		err := binary.Read(buf, binary.BigEndian, &page)
		if err != nil {
			panic(err)
		}
		return page
	}
	return Page{}
}

func (page *Page) LeafNodeInsert(row Row, cursor *Cursor) error {
	if page.NodeType != Leaf {
		panic("Cannot insert row into internal node")
	}
	if cursor.CellNum >= RowsPerPage {
		return DBError{PageFull}
	}
	rowOffset := cursor.CellNum % RowsPerPage
	page.Rows[rowOffset] = row
	page.NumCells++
	return nil
}