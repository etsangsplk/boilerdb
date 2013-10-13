//
// Various utility functions used across the database
//
package util

import (
	"crypto/sha1"
	"fmt"
	"os"
	"sync/atomic"
)

// Wrapper for atomic C.a.S boolean flags
type AtomicFlag struct {
	value int32
}

// Return True if a flag is set
func (af *AtomicFlag) IsSet() bool {
	return atomic.LoadInt32(&af.value) != 0
}

// Atomically set the value of the flag, without checking the current state of it
func (af *AtomicFlag) Set(value bool) {
	if value {
		atomic.StoreInt32(&(af.value), 1)
	} else {
		atomic.StoreInt32(&(af.value), 0)
	}
}

//set the value and return the old value
func (af *AtomicFlag) GetSet(value bool) bool {

	//func CompareAndSwapInt32(val *int32, old, new int32) (swapped bool

	var newVal int32 = 1
	var oldVal int32 = 0
	if !value {
		newVal = 0
		oldVal = 1
	}

	swapped := atomic.CompareAndSwapInt32(&(af.value), oldVal, newVal)

	//if we set true and we swapped - return false
	// if we set true and not swapped -return true
	// if we set false and swapped -return true
	// if we set false and not swapped - return false
	if value {
		return !swapped
	}
	return swapped

}

// generate a unique id by running SHA1 on /dev/urandom
func UniqId() string {

	f, _ := os.Open("/dev/urandom")
	b := make([]byte, 16)
	f.Read(b)
	f.Close()

	s := sha1.New()
	return fmt.Sprintf("%x", s.Sum(b)[:16])
}
