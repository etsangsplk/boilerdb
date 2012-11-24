/**
 * Created with IntelliJ IDEA.
 * User: dvirsky
 * Date: 11/24/12
 * Time: 8:16 PM
 * To change this template use File | Settings | File Templates.
 */
package db

import (
	"log"
	"runtime/debug"
	"net"
	"config"
)
type Session struct {
	InChan    chan *Command
	OutChan   chan *Result
	db        *DataBase
	Addr      net.Addr
	IsRunning bool
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
			log.Printf("Error running session: %s", e)
			debug.PrintStack()

		}
	}()
	for s.IsRunning {
		cmd := <- s.InChan
		ret, _ := s.db.HandleCommand(cmd, s)
		s.OutChan <- ret

	}
	log.Printf("Stopped Session %s....\n", s.Addr)
}

//stop a session on end
func (s *Session) Stop() {
	if s.IsRunning {
		log.Printf("Stopping Session %s....\n", s.Addr)
		s.IsRunning = false
		s.db.RemoveSink(s.Id())
		s.db.Stats.ActiveSessions--
	}
}


