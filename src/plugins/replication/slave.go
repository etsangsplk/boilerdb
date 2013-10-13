//
package replication

import (
	"adapters/redis"
	"bufio"
	"bytes"
	"db"
	"encoding/gob"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	log "github.com/llimllib/loglevel"
)

const (
	STATE_OFFLINE          = 0
	STATE_PENDING_SYNC     = 1
	STATE_SYNC_IN_PROGRESS = 2
	STATE_LIVE             = 3
)

const RECONNECT_INTERVAL = 5

var stateMap map[int]string = map[int]string{
	STATE_OFFLINE:          "Offline",
	STATE_PENDING_SYNC:     "Pending Sync",
	STATE_SYNC_IN_PROGRESS: "Sync in progress",
	STATE_LIVE:             "Live",
}

//this represents the status of a master in a slave
type Master struct {
	State   int
	Host    string
	Port    int
	Conn    net.Conn
	decoder *gob.Decoder
	reader  *bufio.ReadWriter
}

func (m *Master) String() string {

	return fmt.Sprintf("Master(%s:%d, %s)", m.Host, m.Port, stateMap[m.State])
}

// Connect to a master
func (m *Master) Connect() error {

	//make sure we don't connect without disconnecting
	if m.State != STATE_OFFLINE {
		log.Warnf("Could not connect to a connected master!")
		return fmt.Errorf("Trying to connect to an already connected master")
	}

	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", m.Host, m.Port))
	if err != nil {
		log.Errorf("Could not connect to master %s: %s", m, err)
		return err
	}
	m.Conn = conn
	//set the state to "pending sync"
	m.State = STATE_PENDING_SYNC
	log.Infof("Successfuly connected to master %s", m)
	return nil
}

// Disconnect from a master
func (m *Master) Disconnect() error {

	m.State = STATE_OFFLINE
	err := m.Conn.Close()
	if err != nil {
		log.Warnf("Could not close conection to master: %s", err)
		return err
	}

	log.Infof("Disconnected master %s", m)

	return nil
}

const SYNC_OK_MESSAGE = "+SYNC_OK"

//run the replication loop of the master
func (m *Master) RunReplication() {

	defer func() {
		err := recover()
		if err != nil {
			log.Errorf("Errro running replication loop! %s", err)
			disconnectMaster()

		}
	}()

	reader := bufio.NewReaderSize(m.Conn, 32768)
	writer := bufio.NewWriterSize(m.Conn, 32768)

	redis.SerializeResponse(&db.Command{"SYNC", "", make([][]byte, 0)}, writer)
	writer.Flush()
	mockSession := db.DB.NewSession(currentMaster.Conn.LocalAddr())
	mockSession.InChan = nil
	for m.State != STATE_OFFLINE {
		//read an parse the request
		cmd, _ := redis.ReadRequest(reader)

		//no command - WAT?
		if len(cmd.Command) == 0 {
			continue
		}

		//got a status - could be OK and could be
		if cmd.Command[0] == '+' {
			if cmd.Command == SYNC_OK_MESSAGE {
				log.Infof("Received sync ok message!")
				currentMaster.State = STATE_LIVE

			}
			continue
		} else if cmd.Command[0] == '-' {
			log.Warnf("Got error message as command: %s", cmd.Command)
			continue
		} else {
			_, er := db.DB.HandleCommand(cmd, mockSession)
			if er != nil {
				log.Warnf("Error handling command: %s", er)
			}
		}

	}

	//stop the session
	mockSession.Stop()

	m.Disconnect()

}

var currentMaster *Master = nil

// Disconnect the slave from the current master
func disconnectMaster() {

	defer func() {
		e := recover()
		if e != nil {
			log.Errorf("Could not disconnect master: %s", e)
			currentMaster = nil
		}

	}()
	if currentMaster != nil {

		err := currentMaster.Disconnect()
		if err != nil {
			log.Warnf("Could not close conection to master: %s", err)
		}
		currentMaster = nil
		log.Infof("Disconnected master")
	}

}

// Connect to a new master
func connectToMaster(host string, port int) error {

	log.Infof("Connecting to master %s:%d", host, port)
	//check to see if we already have a master
	if currentMaster != nil {
		if currentMaster.Host == host && currentMaster.Port == port {
			log.Warnf("Cannot connect to the same master")
			return fmt.Errorf("Trying to reconnect to the current master %s", currentMaster)
		}

		//disconnect from the current one first
		log.Infof("Disconnecting from current master first...")
		disconnectMaster()

	}

	m := &Master{
		Host:    host,
		Port:    port,
		State:   STATE_OFFLINE,
		decoder: nil,
	}

	err := m.Connect()
	if err == nil {
		log.Infof("Setting new master to %s", m)
		currentMaster = m
	} else {
		return err
	}

	go currentMaster.RunReplication()

	return nil
}

func (m *Master) ReadValue(buf []byte) {

	if m.decoder == nil {

		m.decoder = gob.NewDecoder(m.reader)
	}

	m.reader.Writer.Write(buf)

	var se db.SerializedEntry
	err := m.decoder.Decode(&se)
	if err == nil {

		fmt.Println(se)
		db.DB.LoadSerializedEntry(&se)

	}

}

func HandleLOAD(cmd *db.Command, entry *db.Entry, session *db.Session) *db.Result {

	if currentMaster == nil {
		log.Errorf("Got load while not connected to a master!")
		return db.NewResult(db.NewError(db.E_PLUGIN_ERROR))
	}

	l, e := strconv.Atoi(string(cmd.Args[1]))
	if e != nil {
		log.Errorf("Could not read entry len: %s", e)
		return nil
	}

	var se db.SerializedEntry = db.SerializedEntry{
		Key:   cmd.Key,
		Type:  string(cmd.Args[0]),
		Len:   uint64(l),
		Bytes: cmd.Args[2],
	}

	err := db.DB.LoadSerializedEntry(&se)
	if err != nil {

		log.Errorf("Error loading entry: %s", e)
	}

	return nil
}

//this is a coroutine that checks the current master state and restarts it if it has failed
func runMasterWatchdogLoop() {

	log.Infof("Running replication watchdog loop!")
	for {
		if currentMaster != nil {

			log.Infof("Checking current master: %s", currentMaster)
			if currentMaster.State == STATE_OFFLINE {
				err := currentMaster.Connect()
				if err == nil {
					log.Infof("Reconnected current master %s", *currentMaster)
					go currentMaster.RunReplication()
				} else {
					log.Warnf("Could not connect to current master %s: %s", *currentMaster, err)
				}

			}
		}

		time.Sleep(RECONNECT_INTERVAL * time.Second)
	}

}

//drop the current master and stop trying...
func leaveCurrentMaster() {

	log.Infof("Disconnecting from current master %s", currentMaster)

	disconnectMaster()

}

func HandleSLAVEOF(cmd *db.Command, entry *db.Entry, session *db.Session) *db.Result {
	address := cmd.Key
	log.Infof("Got slaveof to %s %s", cmd.Key, cmd.Args[0])

	if strings.ToUpper(cmd.Key) == "NO" && bytes.Equal(bytes.ToUpper(cmd.Args[0]), []byte("ONE")) {
		leaveCurrentMaster()
		return db.ResultOK()
	}
	port, err := strconv.Atoi(string(cmd.Args[0]))
	//valiate port number
	if err != nil || port < 1 || port > 65535 {

		return db.NewResult(db.NewPluginError("REPLICATION", "Invalid port number"))
	}

	err = connectToMaster(address, port)

	if err != nil {
		log.Warnf("Aborting master connection: %s", err)
		return db.NewResult(db.NewPluginError("REPLICATION", fmt.Sprintf("Could not connect to master: %s", err)))
	}

	return db.ResultOK()
}
