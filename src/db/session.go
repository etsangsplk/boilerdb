package db

import (
	"config"
	"fmt"
	"net"
	"runtime/debug"
	"sync"

	log "github.com/llimllib/loglevel"
)

type Session struct {
	InChan    chan *Command
	outChan   chan *Result
	db        *DataBase
	Addr      net.Addr
	IsRunning bool
	lock      sync.Mutex
}

func (s *Session) Id() string {
	return s.Addr.String()
}

func (s *Session) String() string {
	return s.Id()
}

//create a new session
func (db *DataBase) NewSession(addr net.Addr) *Session {

	db.Stats.ActiveSessions++
	db.Stats.TotalSessions++

	ret := &Session{
		InChan:    make(chan *Command, config.InChanBufsize),
		outChan:   make(chan *Result, config.OutChanBufsize),
		db:        db,
		Addr:      addr,
		IsRunning: true,
	}

	return ret

}

//Run a session while it is open. Read from the input channel and push to the output channel
func (s *Session) Run() {

	defer func() {
		e := recover()
		if e != nil {
			log.Infof("Error running session: %s", e)
			debug.PrintStack()

		}
	}()
	for s.IsRunning {
		cmd := <-s.InChan

		if cmd != nil {

			//we put another function here to sandbox the errors that may arise from handling the command itself
			func() {
				defer func() {
					e := recover()
					if e != nil {
						log.Errorf("Runtime erro in plugin: %s. Stack: %s", e, debug.Stack())

						s.outChan <- NewResult(NewPluginError("", fmt.Sprintf("%s", e)))
					}
				}()
				ret, _ := s.db.HandleCommand(cmd, s)
				if s.outChan != nil {
					s.outChan <- ret
				}
			}()

		}

	}

	log.Infof("Stopped Session %s....\n", s.Addr)
}

// Send a result down a sessions out channel in a safe way
func (s *Session) Send(res *Result) {

	s.lock.Lock()
	defer s.lock.Unlock()
	if s.IsRunning {
		if s.outChan != nil {
			s.outChan <- res
			return
		}

	}
	log.Warnf("Sending a command down a dead/invalid session %s", s)

}

//Receive a message from the session's channel in a safe way
func (s *Session) Receive() *Result {

	if s.IsRunning {
		if s.outChan != nil {
			ret := <-s.outChan
			return ret
		}

	}
	return nil
}

//stop a session on end
func (s *Session) Stop() {

	s.lock.Lock()
	defer s.lock.Unlock()

	if s.IsRunning {
		log.Infof("Stopping Session %s....\n", s.Addr)
		s.IsRunning = false
		s.db.RemoveSink(s.Id())
		s.db.Stats.ActiveSessions--
		//close(s.InChan)
		//close(s.OutChan )
	}
}
