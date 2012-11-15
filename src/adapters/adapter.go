/**
 * Created with IntelliJ IDEA.
 * User: daniel
 * Date: 11/15/12
 * Time: 9:57 PM
 * To change this template use File | Settings | File Templates.
 */
package adapters

import (
	"net"
	"db"
)

type Adapter interface {
	Init(d *db.DataBase)
	Listen(addr net.Addr) error
	Start() error
	Stop() error
	Name() string
}
