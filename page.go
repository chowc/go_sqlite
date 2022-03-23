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
	RightmostChild int32
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

func (page *Page) Insert(row Row, cursor *Cursor) error {
	if page.NodeType != Leaf {
		panic("page should be leaf node")
	}

	if page.NumCells >= RowsPerPage {
		return page.SplitAndInsert(row, cursor)
	}

	for i:=page.NumCells; i>cursor.CellNum; i-- {
		// fmt.Printf("i=%d, page.NumCells=%d, cursor.CellNum=%d\n", i, page.NumCells, cursor.CellNum)
		page.Rows[i] = page.Rows[i-1]
	}
	page.Rows[cursor.CellNum] = row
	page.NumCells++
	return nil
}

func (page *Page) SplitAndInsert(row Row, cursor *Cursor) error {
	table := cursor.Table
	newPageIdx := table.Pager.GetNewPageNum()
	newPage, err := table.Pager.GetPage(newPageIdx, true)
	if err != nil {
		return err
	}
	// move right half rows from old page to new page
	halfPageCount := (RowsPerPage+1)/2
	for i:=RowsPerPage; i>=0; i-- {
		var target *Page
		if i >= halfPageCount {
			target = newPage
			// fmt.Printf("i %d -> newPage\n", i)
		} else {
			target = page
			// fmt.Printf("i %d -> oldPage\n", i)
		}

		if i == cursor.CellNum {
			target.Rows[i%halfPageCount] = row
			// fmt.Printf("insert row %d at index %d\n", row.ID, i%halfPageCount)
		} else if i > cursor.CellNum {
			// fmt.Printf("insert row %d at index %d\n", page.Rows[i-1].ID, i%halfPageCount)
			target.Rows[i%halfPageCount] = page.Rows[i-1]
		} else {
			// fmt.Printf("insert row %d at index %d\n", page.Rows[i].ID, i%halfPageCount)
			target.Rows[i%halfPageCount] = page.Rows[i]
		}
	}
	newPage.NumCells = halfPageCount
	page.NumCells = halfPageCount
	if page.RootNode {
		err = CreateNewRoot(table, newPageIdx)
		if err != nil {
			return err
		}
		newPage.ParentNode = table.RootPageNum
		newPage.Sibling = 0 // End
	} else {
		panic("Need to implement updating parent after split")
	}

	return nil
}

func CreateNewRoot(table *Table, rightChildIdx int32) error {
	leftChildIdx := table.Pager.GetNewPageNum()
	leftChild, err := table.Pager.GetPage(leftChildIdx, true)
	if err != nil {
		return err
	}
	// copy root to left child
	rootPage, err := table.Pager.GetPage(table.RootPageNum, false)
	if err != nil {
		return err
	}
	leftChild.LeafNode = rootPage.LeafNode
	leftChild.RootNode = false
	leftChild.NodeType = Leaf
	leftChild.ParentNode = table.RootPageNum
	rootPage.LeafNode = LeafNode{}
	rootPage.KeyNums = 1
	rootPage.NodeType = Internal
	rootPage.Children[0].PageNum = leftChildIdx
	rootPage.Children[0].Key = leftChild.Rows[leftChild.NumCells-1].ID
	rootPage.RightmostChild = rightChildIdx
	leftChild.Sibling = rightChildIdx
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
		if page.KeyNums == 0 {
			panic("empty children cannot be internal")
		}
		left := int32(0)
		right := page.KeyNums
		mid := left + (right-left)/2
		for left < right {
			midKey := page.Children[mid]
			if midKey.Key == key {
				break
			} else if midKey.Key < key {
				left = mid+1
			} else {
				right = mid
			}
			mid = left + (right-left)/2
		}
		cursor.PageNum = mid
		newPage, err := table.Pager.GetPage(mid, false)
		if err != nil {
			return cursor, err
		}
		return newPage.LeafNodeSearch(table, key)
	}

	return cursor, nil
}