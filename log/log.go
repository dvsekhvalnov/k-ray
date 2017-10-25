package log

import (
	"io/ioutil"
	"log"
)

var Log StdLogger = log.New(ioutil.Discard, "[K-RAY] [DEBUG]", log.LstdFlags)

// Logger is used for library logging
type StdLogger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}
