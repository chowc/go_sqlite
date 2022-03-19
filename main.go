package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
)

var table *Table

func main() {
	var dbPath string
	flag.StringVar(&dbPath, "file", "db.sqlite", "the db file")
	flag.Parse()

	t, err := OpenDB(Options{dbPath})
	if err != nil {
		fmt.Printf("OpenDB fail:%v\n", err)
		os.Exit(1)
	}
	table = t
	for true {
		print("> ")
		reader := bufio.NewReader(os.Stdin)
		// fmt.Print("Enter text: ")
		line, err := reader.ReadString('\n')
		if err != nil {
			fmt.Printf("read input fail: %v\n", err)
			continue
		}
		metaResult, err := DoMetaCommand(line)
		if err != nil {
			fmt.Printf("%v\n", err)
			continue
		}
		switch metaResult {
		case MetaCommandSuccess:
			println("Meta command executed")
			continue
		}
		s, err := PrepareStatement(line)
		if err != nil {
			fmt.Printf("%v\n", err)
			continue
		}
		err = ExecuteStatement(table, *s)
		if err != nil {
			fmt.Printf("%v\n", err)
			continue
		}
		fmt.Println()
	}
}

type StatementType int

const (
	StatementSelect StatementType = iota
	StatementInsert
)

type Statement struct {
	StatementType StatementType
	Row           Row
}

type MetaCommandResult int

const (
	MetaCommandSuccess MetaCommandResult = iota
	MetaCommandUnknown
)

func DoMetaCommand(line string) (MetaCommandResult, error) {
	ss := strings.Split(strings.TrimSpace(line), " ")
	if len(ss) == 0 {
		return 0, DBError{InvalidStatement}
	}
	switch strings.ToUpper(ss[0]) {
	case ".EXIT":
		table.Close()
		os.Exit(0)
		return MetaCommandSuccess, nil
	}
	return MetaCommandUnknown, nil
}

func PrepareStatement(line string) (*Statement, error) {
	ss := strings.Split(strings.TrimSpace(line), " ")
	if len(ss) == 0 {
		return nil, DBError{InvalidStatement}
	}
	switch strings.ToUpper(ss[0]) {
	case "INSERT":
		// insert 1 john john@example.com
		if len(ss) < 4 {
			return nil, DBError{InvalidStatement}
		}
		id, err := strconv.Atoi(ss[1])
		if err != nil {
			return nil, DBError{InvalidStatement}
		}
		if len(ss[2]) > 32 {
			return nil, DBError{NameTooLong}
		}
		if len(ss[3]) > 256 {
			return nil, DBError{EmailTooLong}
		}
		var a [32]byte
		copy(a[:], ss[2])
		var b [256]byte
		copy(b[:], ss[3])
		row := Row{
			ID:    int32(id),
			Name:  a,
			Email: b,
		}
		return &Statement{
			StatementType: StatementInsert,
			Row:           row,
		}, nil
	case "SELECT":
		return &Statement{
			StatementType: StatementSelect,
		}, nil
	}
	return nil, DBError{InvalidStatement}
}

func ExecuteStatement(table *Table, s Statement) error {
	switch s.StatementType {
	case StatementSelect:
		rows, err := table.SelectAll()
		if err != nil {
			return err
		}
		for _, row := range rows {
			fmt.Printf("(%d, %s, %s)\n", row.ID, row.Name, row.Email)
		}
	case StatementInsert:
		return table.InsertRow(s.Row)
	default:
		return DBError{InvalidStatement}
	}
	return nil
}

