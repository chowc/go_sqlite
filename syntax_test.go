package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

// TODO: name too long test

func TestColumnTooLong(t *testing.T) {
	sql := "insert 1 LoremipsumdolorsitametLoremipsumdolorsitamet mock@email.com"
	_, err := PrepareStatement(sql)
	assert.EqualValues(t, DBError{NameTooLong}, err)

	sql = "insert 1 john LoremipsumdolorsitametLoremipsumdolorsitametLoremipsumdolorsitametLoremipsumdolorsitametLoremipsumdolorsitametLoremipsumdolorsitametLoremipsumdolorsitametLoremipsumdolorsitametLoremipsumdolorsitametLoremipsumdolorsitametLoremipsumdolorsitametLoremipsumdolorsitamet@email.com"
	_, err = PrepareStatement(sql)
	assert.EqualValues(t, DBError{EmailTooLong}, err)
}

func TestColumnNumNotMatch(t *testing.T) {
	sql := "insert 1"
	_, err := PrepareStatement(sql)
	assert.EqualValues(t, DBError{InvalidStatement}, err)
}