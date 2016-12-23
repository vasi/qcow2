package eio

import (
	"log"

	"github.com/timtadh/data-structures/exc"
)

// BacktraceError is an error that carries backtrace information along with it.
//
// In constrast with exc.Exception, it does not include the backtrace in its output
// unless it is specifically asked for.
type BacktraceError interface {
	error
	Backtrace() string
}

// BacktraceErr turns an error into an BacktraceError.
func BacktraceErr(e error) BacktraceError {
	if e != nil {
		return &btErrImpl{e}
	}
	return nil
}

// BacktraceWrap turns an exception into a BacktraceError.
func BacktraceWrap(f func()) BacktraceError {
	return BacktraceErr(exc.Try(f).Error())
}

// Trace checks if an error occurred, and if so prints a backtrace and exits.
func Trace(e error) {
	if e == nil {
		return
	}
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
