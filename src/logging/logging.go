// A simple logging module that mimics the behavior of Python's logging module.
// All it does basically is wrap Go's logger with nice multi-level logging calls, and
// allows you to set the logging level of your app in runtime.
// Logging is done just like calling fmt.Sprintf: logging.Info("This object is %s and that is %s", obj, that)
//
package logging

import (
	"log"
	"fmt"
	"runtime/debug"
	"path"
	"runtime"
)


const (
	DEBUG = 1
	INFO = 2
	WARNING = 4
	WARN = 4
	ERROR = 8
	CRITICAL  = 16
	ALL = 255
	NOTHING = 0
)

//default logging level is ALL
var level int = ALL

// Set the logging level.
// Contrary to Python that specifies a minimal level, this logger is set with a bit mask
// of active levels.
// e.g. for INFO and ERROR use SetLevel(logging.INFO | logging.ERROR)
// For everything but debug and info use SetLevel(logging.ALL &^ (logging.INFO | logging.DEBUG))
func SetLevel(l int) {
	level = l

}

func getContext() (file string, line int) {

	_, file, line, _ = runtime.Caller(3)
	file = path.Base(file)

	return
}

//Output debug logging messages
func Debug(msg string, args ...interface{}) {
	if level & DEBUG != 0 {
		log.Printf(fmt.Sprintf("DEBUG: %s",  msg), args...)
	}
}

func writeMessage(level string, msg string, args ...interface {} ) {
	f, l := getContext()
	log.Printf(fmt.Sprintf("%s @ %s:%d: %s", level, f, l, msg), args...)
}
//output INFO level messages
func Info(msg string, args ...interface{}) {

	if level & INFO != 0 {

		writeMessage("INFO", msg, args...)
	}
}

//output WARNING level messages
func Warning(msg string, args ...interface{}) {
	if level & WARN != 0 {
		writeMessage("WARNING", msg, args...)
	}
}

//output ERROR level messages
func Error(msg string, args ...interface{}) {
	if level & ERROR != 0 {
		writeMessage("ERROR", msg, args...)
	}
}

//Output a CRITICAL level message while showing a stack trace
func Critical(msg string, args ...interface{}) {
	if level & CRITICAL != 0 {
		writeMessage("CRITICAL", msg, args...)
		log.Println(debug.Stack())
	}
}

// Raise a PANIC while writing the stack trace to the log
func Panic(msg string, args ...interface{}) {
	log.Println(debug.Stack())
	log.Panicf(msg, args...)

}
