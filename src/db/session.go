
package db

import (
	"logging"
	"runtime/debug"
	"net"
	"config"
	"fmt"
	"sync"
)
type Session struct {
	InChan    chan *Command
	OutChan   chan *Result
	db        *DataBase
	Addr      net.Addr
	IsRunning bool
	lock sync.Mutex
}

func (s *Session) Id() string {
	return s.Addr.String()
}
//create a new session
func (db *DataBase) NewSession(addr net.Addr) *Session {

	db.Stats.ActiveSessions++
	db.Stats.TotalSessions++

	ret := &Session{
		InChan:    make(chan *Command, config.IN_CHAN_BUFSIZE),
		OutChan:   make(chan *Result, config.OUT_CHAN_BUFSIZE),
		db:        db,
		Addr:      addr,
		IsRunning: true,
	}

	return ret

}

func (s *Session) Run() {

	defer func() {
		e := recover()
		if e != nil {
			logging.Info("Error running session: %s", e)
			debug.PrintStack()

		}
	}()
	for s.IsRunning {
		cmd := <- s.InChan

		if cmd != nil {

			func() {
				defer func() {
					e := recover()
					if e != nil {
						logging.Error("Runtime erro in plugin: %s", e)
						s.OutChan <- NewResult(NewPluginError("",  fmt.Sprintf("%s", e)))
					}
				}()
				ret, _ := s.db.HandleCommand(cmd, s)
				if s.OutChan != nil {
					s.OutChan <- ret
				}
			}()


		}

	}

	logging.Info("Stopped Session %s....\n", s.Addr)
}

//stop a session on end
func (s *Session) Stop() {

	s.lock.Lock()
	defer s.lock.Unlock()

	if s.IsRunning {
		logging.Info("Stopping Session %s....\n", s.Addr)
		s.IsRunning = false
		s.db.RemoveSink(s.Id())
		s.db.Stats.ActiveSessions--
		//close(s.InChan)
		//close(s.OutChan )
	}
}


