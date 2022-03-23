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
	ChildrenNum    int32
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
		page.Rows[i] = page.Rows[i-1]
	}
	page.Rows[cursor.CellNum] = row
	if cursor.CellNum == page.NumCells && !page.RootNode {
		var oldMax int32
		if page.NumCells > 0 {
			oldMax = page.Rows[page.NumCells-1].ID
		}
		parent, err := cursor.Table.Pager.GetPage(page.ParentNode, false)
		if err != nil {
			return err
		}
		idx := parent.InternalNodeFindChild(oldMax)
		parent.Children[idx].Key = row.ID
	}
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
	oldLeftMax := page.Rows[page.NumCells-1].ID
	for i:=RowsPerPage; i>=0; i-- {
		var target *Page
		if i >= halfPageCount {
			target = newPage
		} else {
			target = page
		}

		if i == cursor.CellNum {
			target.Rows[i%halfPageCount] = row
		} else if i > cursor.CellNum {
			target.Rows[i%halfPageCount] = page.Rows[i-1]
		} else {
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
		parent, err := table.Pager.GetPage(page.ParentNode, false)
		if err != nil {
			return err
		}
		newLeftMax := page.Rows[page.NumCells-1].ID
		if parent.ChildrenNum >= ChildrenPerPage {
			panic("need to implement internal node split")
		}
		if page.Sibling == 0 { // page is the rightmost page
			parent.Children[parent.ChildrenNum] = Child{
				Key:     newLeftMax,
				PageNum: page.PageNum,
			}
			parent.ChildrenNum++
			parent.RightmostChild = newPageIdx
		} else {
			childIdx := parent.InternalNodeFindChild(oldLeftMax)
			parent.Children[childIdx].Key = newLeftMax
			// insert new page
			for i:=parent.ChildrenNum; i>childIdx+1; i-- {
				parent.Children[i] = parent.Children[i-1]
			}
			parent.Children[childIdx+1].Key = newPage.Rows[newPage.NumCells-1].ID
			parent.Children[childIdx+1].PageNum = newPageIdx

			parent.ChildrenNum++
		}
		page.Sibling = newPageIdx
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
	rootPage.ChildrenNum = 1
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
		if page.ChildrenNum == 0 {
			panic("empty children cannot be internal")
		}
		var left int32
		if key > page.Children[page.ChildrenNum-1].Key {
			left = page.RightmostChild
		} else {
			left = int32(0)
			right := page.ChildrenNum
			mid := left + (right-left)/2
			for left < right {
				midKey := page.Children[mid]
				if midKey.Key == key {
					break
				} else if midKey.Key < key {
					left = mid + 1
				} else {
					right = mid
				}
				mid = left + (right-left)/2
			}
		}
		cursor.PageNum = page.Children[left].PageNum
		newPage, err := table.Pager.GetPage(left, false)
		if err != nil {
			return cursor, err
		}
		return newPage.LeafNodeSearch(table, key)
	}

	return cursor, nil
}

func (page *Page) InternalNodeFindChild(key int32) int32 {
	left := int32(0)
	right := page.ChildrenNum
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
	return mid
}