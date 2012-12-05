/**
 * Created with IntelliJ IDEA.
 * User: dvirsky
 * Date: 12/5/12
 * Time: 12:36 PM
 * To change this template use File | Settings | File Templates.
 */
package logging

import (
	"log"
	"fmt"
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

var level int = ALL

func SetLevel(l int) {
	level = l

}

func Debug(msg string, args ...interface{}) {
	if level & DEBUG != 0 {
		log.Printf(fmt.Sprintf("DEBUG: %s",  msg), args...)
	}
}

func Info(msg string, args ...interface{}) {
	
	if level & INFO != 0 {
		log.Printf(fmt.Sprintf("INFO: %s",  msg), args...)
	}
}

func Warning(msg string, args ...interface{}) {
	if level & WARN != 0 {
		log.Printf(fmt.Sprintf("WARN: %s",  msg), args...)
	}
}

func Error(msg string, args ...interface{}) {
	if level & ERROR != 0 {
		log.Printf(fmt.Sprintf("ERROR: %s",  msg), args...)
	}
}

func Critical(msg string, args ...interface{}) {
	if level & CRITICAL != 0 {
		log.Printf(fmt.Sprintf("CRITICAL: %s",  msg), args...)
	}
}

func Panic(msg string, args ...interface{}) {
	log.Panicf(msg, args...)
}
