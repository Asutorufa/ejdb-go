package ejdb

import (
	"errors"
	"fmt"
)

type ErrorCode string

const (
	CodeClosed             ErrorCode = "EJDB_CLOSED"
	CodeNotFound           ErrorCode = "EJDB_NOT_FOUND"
	CodeCollectionExists   ErrorCode = "EJDB_COLLECTION_EXISTS"
	CodeCollectionAbsent   ErrorCode = "EJDB_COLLECTION_ABSENT"
	CodeIndexNotFound      ErrorCode = "EJDB_INDEX_NOT_FOUND"
	CodeUniqueConstraint   ErrorCode = "EJDB_UNIQUE_CONSTRAINT"
	CodeReadOnlyTx         ErrorCode = "EJDB_READONLY_TX"
	CodeInvalidQuery       ErrorCode = "EJDB_INVALID_QUERY"
	CodeInvalidPlaceholder ErrorCode = "JQL_INVALID_PLACEHOLDER"
	CodeUnsetPlaceholder   ErrorCode = "JQL_UNSET_PLACEHOLDER"
	CodeNoCollection       ErrorCode = "JQL_NO_COLLECTION"
)

type Error struct {
	Code ErrorCode
	Msg  string
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Msg == "" {
		return string(e.Code)
	}
	return e.Msg
}

func (e *Error) Is(target error) bool {
	te, ok := target.(*Error)
	if !ok {
		return false
	}
	return e.Code == te.Code
}

func withCode(code ErrorCode, msg string) error {
	return &Error{Code: code, Msg: msg}
}

func withCodef(code ErrorCode, format string, args ...any) error {
	return &Error{Code: code, Msg: fmt.Sprintf(format, args...)}
}

func CodeOf(err error) (ErrorCode, bool) {
	var e *Error
	if errors.As(err, &e) {
		return e.Code, true
	}
	return "", false
}

var (
	ErrNotFound         = &Error{Code: CodeNotFound, Msg: "ejdb: document not found"}
	ErrCollectionExists = &Error{Code: CodeCollectionExists, Msg: "ejdb: collection already exists"}
	ErrCollectionAbsent = &Error{Code: CodeCollectionAbsent, Msg: "ejdb: collection does not exist"}
	ErrIndexNotFound    = &Error{Code: CodeIndexNotFound, Msg: "ejdb: index not found"}
	ErrUniqueConstraint = &Error{Code: CodeUniqueConstraint, Msg: "ejdb: unique index constraint violated"}
	ErrClosed           = &Error{Code: CodeClosed, Msg: "ejdb: database is closed"}
	ErrReadOnlyTx       = &Error{Code: CodeReadOnlyTx, Msg: "ejdb: transaction is read-only"}
	ErrInvalidQuery     = &Error{Code: CodeInvalidQuery, Msg: "ejdb: invalid query"}
)
