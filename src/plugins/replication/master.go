package replication

import (
	"config"
	"container/list"
	"db"
	"errors"
	"fmt"
	"sync"

	log "github.com/llimllib/loglevel"
)

//This represents the status of a slave in  the master
type Slave struct {
	State         int
	session       *db.Session
	LastCommandId uint64
	buffer        *list.List
	Channel       chan []byte
	sink          *db.CommandSink
	lock          sync.Mutex
}

var replicationLock sync.Mutex

func (s *Slave) String() string {
	if s.session != nil {
		return s.session.Id()
	}
	return "n/a"

}

var slaves map[string]*Slave = make(map[string]*Slave)

func NewSlave(session *db.Session) *Slave {

	ret := &Slave{
		State:         STATE_PENDING_SYNC,
		session:       session,
		LastCommandId: 0,
		buffer:        list.New(),
		Channel:       make(chan []byte, 1000),
	}
	log.Infof("Created new slave for session %s", *session)

	ret.sink = db.DB.AddSink(
		db.CMD_WRITER,
		ret.session.Id())

	return ret

}

// The slave implements the io.writer interface so it can be sent directly to the database for dumping a complete SYNC
func (s *Slave) Send(se *db.SerializedEntry) error {

	//we assume what we get here is a gobbed object.
	//We don't verify currently that this is indeed the case

	cmd := &db.Command{
		Command: "LOAD",
		Key:     se.Key,
		Args:    [][]byte{[]byte(se.Type), []byte(fmt.Sprintf("%d", se.Len)), se.Bytes},
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	s.session.Send(db.NewResult(cmd))

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

	log.Infof("Starting sync for slave %s", s)
	numRetries := 0

	for numRetries < config.MAX_SYNC_RETRIES {
		ch := make(chan *db.SerializedEntry)
		go func() {
			//			defer func() {
			//				e := recover()
			//				if e != nil {
			//					logging.Warning("Error while dumping: %s", e)
			//				}
			//			}()

			var se *db.SerializedEntry
			for {

				//read a serialized entry
				se = <-ch

				if se != nil {
					//encode it
					log.Debugf("Read entry %s", se.Key)

					e := s.Send(se)
					if e != nil {
						log.Warnf("Error serializing enty: %s", e)
					}
				} else {
					break
				}
			}

			log.Infof("Finished writing database to slave.")
		}()

		err := db.DB.DumpEntries(ch, true)
		close(ch)
		if err != nil {
			log.Warnf("Failed to sync slave: %s", err)
			numRetries++
		}
		break

	}
	//now send the pending slave buffer if needed

	log.Infof("Slave backlog buffer now %d entries, sending them first...", s.buffer.Len())
	for s.buffer.Len() > 0 {

		elem := s.buffer.Front()

		cmd, ok := elem.Value.(*db.Command)
		if ok {

			s.session.Send(db.NewResult(cmd))
		} else {
			log.Warnf("Could not pop entry from slave %s buffer", s)
		}

	}
	s.lock.Lock()
	s.State = STATE_LIVE
	s.lock.Unlock()
	log.Infof("Finished sending slave backlog buffer")

	//send the "sync ok" message to signal the sync state has ended
	s.session.Send(db.NewResult(SYNC_OK_MESSAGE))

	return nil
}

const MAX_BUFFER int = 100000

func (s *Slave) bufferCommand(cmd *db.Command) error {

	s.lock.Lock()
	defer s.lock.Unlock()

	//if the buffer is not full...
	if s.buffer.Len() < MAX_BUFFER {
		s.buffer.PushBack(cmd)
		log.Infof("Buffered command %s to slave, buffer now %d", cmd, s.buffer.Len())
		return nil
	}

	return fmt.Errorf("Buffer full on slave %s. Aborting replication", s)
}

func (s *Slave) StartReplication() error {

	//Replication loop
	go func() {

		//recovery...
		//		defer func(){
		//			e := recover()
		//			if e != nil {
		//				logging.Info("Could not send command to session outchan: %s", e)
		//				removeSlave(s)
		//			}
		//
		//		}()

		//sync the slave in a separate goroutine
		err := s.Sync()
		if err != nil {
			log.Warnf("Could not sync slave %s! %s", s, err)
			removeSlave(s)
			return
		}

		// read commands from the sink and put them where they belong
		for s.State != STATE_OFFLINE && s.session.IsRunning {

			cmd := <-s.sink.Channel

			if cmd != nil {

				switch s.State {
				//for live slaves - we simply replicate
				case STATE_LIVE:
					s.session.Send(db.NewResult(cmd))

				//for offline - we raise a cry!
				case STATE_OFFLINE:
					log.Errorf("Trying to send a command to an offline slave! Aborting replication")
					panic("Pushing commands to an offline slave")

				//buffer the command for pending sync slaves
				default:
					err := s.bufferCommand(cmd)
					if err != nil {
						log.Errorf("Aborting replication - buffer full!")
						panic(err)
					}

				}

			}

		}
		log.Infof("finishing session for slave %s", s.session.Id())
		removeSlave(s)
	}()

	return nil
}

func (s *Slave) Disconnect() error {

	s.lock.Lock()
	defer s.lock.Unlock()

	if s.State != STATE_OFFLINE {
		s.State = STATE_OFFLINE
		log.Infof("Disconnecting slave %s", *s)
		s.session.Stop()
		s.sink.Close()
		return nil
	}

	return nil
}

func addSlave(session *db.Session) error {

	if session == nil {
		return errors.New("Could not add nil session")
	}
	replicationLock.Lock()

	if slaves[session.Id()] != nil {
		replicationLock.Unlock()
		return errors.New("Cannot add an existing slave!")

	}

	slave := NewSlave(session)
	slaves[session.Id()] = slave
	log.Infof("Added slave for session %s, currently replicating to %d slaves", session.Id(), len(slaves))

	replicationLock.Unlock()
	err := slave.StartReplication()
	if err != nil {

		log.Warnf("Could not replicate to slave %s! %s", slave.session.Id(), err)
		removeSlave(slave)
		return err
	}

	return nil

}

func removeSlave(s *Slave) error {

	replicationLock.Lock()
	delete(slaves, s.session.Id())
	replicationLock.Unlock()
	s.Disconnect()
	return nil

}

func HandleSYNC(cmd *db.Command, entry *db.Entry, session *db.Session) *db.Result {

	err := addSlave(session)

	if err != nil {
		log.Warnf("Aborting SYNC: %s", err)
		return db.NewResult(db.NewPluginError("REPLICATION", "Could not sync slave"))
	}

	return db.NewResult(db.NewStatus("OK"))
}
