/**
 * Created with IntelliJ IDEA.
 * User: dvirsky
 * Date: 2/16/13
 * Time: 4:35 PM
 * To change this template use File | Settings | File Templates.
 */
package replication

import (
	"db"
	"container/list"
	"config"
//	"net"
	"logging"
	"errors"
//	"strconv"
	"fmt"
//	"strings"
//	"bytes"
//	"bufio"

)




//This represents the status of a slave in  the master
type Slave struct {
	State int
	session *db.Session
	LastCommandId uint64
	buffer *list.List
	Channel chan []byte
	sink *db.CommandSink
}

func (s *Slave) String() string {
	if s.session != nil {
		return s.session.Id()
	}
	return "n/a"

}
var slaves map[string]*Slave = make(map[string]*Slave)

func NewSlave(session *db.Session) *Slave {

	ret := &Slave {
		State: STATE_PENDING_SYNC,
		session: session,
		LastCommandId: 0,
		buffer: list.New(),
		Channel: make(chan []byte),
	}
	logging.Info("Created new slave for session %s", *session)

	ret.sink = db.DB.AddSink(
		db.CMD_WRITER,
		ret.session.Id())


	return ret

}



// The slave implements the io.writer interface so it can be sent directly to the database for dumping a complete SYNC
func (s *Slave)Send(se *db.SerializedEntry) error {

	//we assume what we get here is a gobbed object.
	//We don't verify currently that this is indeed the case

	cmd := &db.Command{
		Command: "LOAD",
		Key: se.Key,
		Args: [][]byte{[]byte(se.Type), []byte(fmt.Sprintf("%d", se.Len)), se.Bytes},
	}


	s.session.OutChan <- db.NewResult(cmd)

	return nil


}

// The Sync Algorithm
//0. while bgsave or another sync is going on - wait a bit
//1. put the client in "sync in progress" state
//2. let the db do a "bgsave" to a writer that sends gob objects to the client's connection
//3. send the gobbed objects to the client
//5. while this is going on, all commands go to the slave's command buffer
//6. put the slave in ONLINE mode and release the sync lock

func (s *Slave) Sync() error {

	logging.Info("Starting sync for slave %s", s)
	numRetries := 0



	for numRetries < config.MAX_SYNC_RETRIES {
		ch := make(chan *db.SerializedEntry)
		go func() {
			defer func() {
				e := recover()
				if e != nil {
					logging.Warning("Error while dumping: %s", e)
				}
			}()

			var se *db.SerializedEntry
			for {

				//read a serialized entry
				se = <- ch

				logging.Debug("Read entry %s", se.Key)
				if se != nil {
					//encode it

					e := s.Send(se)
					if e != nil {
						logging.Warning("Error serializing enty: %s", e)
					}
				} else {
					break
				}
			}

			logging.Info("Finished writing to slave")
		}()

		err := db.DB.DumpEntries(ch, true)
		close(ch)
		if err != nil {
			logging.Warning("Failed to sync slave: %s", err)
			numRetries++
		}
		break

	}

	s.State = STATE_LIVE
	return nil
}

func (s *Slave) StartReplication() error {


	go func() {

		defer func(){
			e := recover()
			if e != nil {
				logging.Info("Could not send command to session outchan: %s", e)
				removeSlave(s)
			}

		}()

		err := s.Sync()
		if err != nil {
			logging.Warning("Could not sync slave %s! %s", s, err)
			removeSlave(s)
			return
		}

		for s.session.IsRunning {

			cmd := <- s.sink.Channel

			if cmd != nil {

				if s.session.OutChan != nil {
					s.session.OutChan <- db.NewResult(cmd)
				}
			}

		}
		logging.Info("finishing session for slave %s", s.session.Id())
		removeSlave(s)
	}()

	return nil
}

func (s *Slave) Disconnect() error {

	if s.State != STATE_OFFLINE {
		s.State = STATE_OFFLINE
		logging.Info("Disconnecting slave %s", *s)
		s.session.Stop()
		s.sink.Close()
		return nil
	}

	return nil
}

func addSlave(session *db.Session) error {

	if session==nil {
		return errors.New("Could not add nil session")
	}
	if slaves[session.Id()] != nil {
		return errors.New("Cannot add an existing slave!")
	}

	slave := NewSlave(session)
	slaves[session.Id()] = slave
	logging.Info("Added slave for session %s, currently replicating to %d slaves", session.Id(), len(slaves))


	err := slave.StartReplication()
	if err != nil {

		logging.Warning("Could not replicate to slave %s! %s", slave.session.Id(), err)
		removeSlave(slave)
		return err
	}

	return nil

}

func removeSlave(s *Slave) error {

	delete(slaves, s.session.Id())
	s.Disconnect()
	return nil

}



func HandleSYNC(cmd *db.Command, entry *db.Entry, session *db.Session) *db.Result {



	err := addSlave(session)

	if err != nil {
		logging.Warning("Aborting SYNC: %s", err)
		return db.NewResult(db.NewPluginError("REPLICATION", "Could not sync slave"))
	}

	return db.NewResult(db.NewStatus("OK"))
}

