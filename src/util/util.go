/**
 * Created with IntelliJ IDEA.
 * User: dvirsky
 * Date: 12/1/12
 * Time: 3:03 PM
 * To change this template use File | Settings | File Templates.
 */
package util

import (
	"sync/atomic"
	"fmt"
	)

type AtomicFlag struct {
	value int32
}

func (af *AtomicFlag)IsSet() bool {
	return atomic.LoadInt32(&af.value) != 0
}

func (af *AtomicFlag)Set(value bool) {

	fmt.Println(value)
	if value {
		atomic.StoreInt32(&(af.value), 1)
	} else {
		atomic.StoreInt32(&(af.value), 0)
	}
}

//set the value and return the old value
func (af *AtomicFlag)GetSet(value bool) bool {

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

