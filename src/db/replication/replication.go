/**
 * Created with IntelliJ IDEA.
 * User: dvirsky
 * Date: 11/23/12
 * Time: 6:16 PM
 * To change this template use File | Settings | File Templates.
 */
package replication

import (
	"container/list"
)


const (
	STATE_OFFLINE = 0
	STATE_INSYNC = 1
	STATE_LIVE = 2

)
type Slave struct {
	State int
	Id string
	LastCommandId uint64
	buffer list.List
	Channel chan []byte
}


