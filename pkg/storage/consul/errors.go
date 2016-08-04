package consul

import (
	"fmt"
	"net/url"

	"k8s.io/kubernetes/pkg/storage"
)

const (
	ErrCodeKeyNotFound int = iota + 1
	ErrCodeKeyExists
	ErrCodeResourceVersionConflicts
	ErrCodeInvalidObj
	ErrCodeUnreachable
)

var errCodeToMessage = map[int]string{
	ErrCodeKeyNotFound:              "key not found",
	ErrCodeKeyExists:                "key exists",
	ErrCodeResourceVersionConflicts: "resource version conflicts",
	ErrCodeInvalidObj:               "invalid object",
	ErrCodeUnreachable:              "server unreachable",
}

func isURLError(err error) bool {
	if err != nil {
		if _, ok := err.(*url.Error); ok {
			return true
		}
	}
	return false
}

func isNotFoundError(err error) bool {
	if err != nil {
		if consulError, ok := err.(ConsulError); ok {
			return consulError.Code == ErrCodeKeyNotFound
		}
	}
	return false
}

func isInvalidObjectError(err error) bool {
	if err != nil {
		if consulError, ok := err.(ConsulError); ok {
			return consulError.Code == ErrCodeInvalidObj
		}
	}
	return false
}

func NewInvalidObjectError(msg string) ConsulError {
	return ConsulError{
		Code:    ErrCodeInvalidObj,
		Message: msg,
	}
}

func NewNotFoundError() ConsulError {
	return ConsulError{
		Code: storage.ErrCodeKeyNotFound,
	}
}

type ConsulError struct {
	Code    int
	Message string
}

func (e ConsulError) Error() string {
	return fmt.Sprintf("StorageError: %s, Code: %d, Key: %s, ResourceVersion: %d, AdditionalErrorMsg: %s",
		errCodeToMessage[e.Code], e.Code, "", 0, e.Message)
}
