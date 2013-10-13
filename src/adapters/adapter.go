package adapters

import (
	"db"
	"net"
)

// Adapter defines the interface for things that listen on boilerdb's port
type Adapter interface {
	Init(d *db.DataBase)
	Listen(addr net.Addr) error
	Start() error
	Stop() error
	Name() string
}
