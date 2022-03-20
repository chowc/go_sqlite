package main

import "fmt"

type DBError struct {
	Code DBCode
}

func (d DBError) Error() string {
	var msg string
	switch d.Code {
	case InvalidStatement:
		msg = "Syntax error. Could not parse statement."
	case NameTooLong:
		msg = "Syntax error. Name too long."
	case EmailTooLong:
		msg = "Syntax error. Email too long."
	case PageFull:
		msg = "Page is full now"
	case DBFileError:
		msg = "DB open file fail"
	case RowNotFound:
		msg = "Row not found"
	case DBWriteFileError:
		msg = "Write to db file fail"
	}
	return fmt.Sprintf("DB error: (%d), %s", d.Code, msg)
}

type DBCode int32

const (
	InvalidStatement DBCode = iota
	NameTooLong
	EmailTooLong
	PageFull
	DBFileError
	RowNotFound
	DBWriteFileError
)