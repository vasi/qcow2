package bio

import (
	"log"

	"github.com/timtadh/data-structures/exc"
)

// BacktraceError is an error that also carries backtrace info
type BacktraceError interface {
	error
	Backtrace() string
}

// BacktraceErr turns an error into an BacktraceError
func BacktraceErr(e error) BacktraceError {
	if e != nil {
		return &btErrImpl{e}
	}
	return nil
}

// BacktraceWrap turns an exception into a BacktraceError
func BacktraceWrap(f func()) error {
	return BacktraceErr(exc.Try(f).Error())
}

// Trace prints an error and exits
func Trace(e error) {
	if b, ok := e.(BacktraceError); ok {
		log.Fatal(b.Backtrace())
	}
	log.Fatal(e)
}

type btErrImpl struct {
	error
}

func (e *btErrImpl) Backtrace() string {
	return e.error.Error()
}

func (e *btErrImpl) Error() string {
	if t, ok := e.error.(exc.Throwable); ok {
		return t.Exc().Errors[0].String()
	}
	return e.error.Error()
}
