//
package replication

import (
	"db"
	"adapters/redis"
	"net"
	"logging"
	"strconv"
	"fmt"
	"strings"
	"bytes"
	"bufio"

)

const (
	STATE_OFFLINE = 0
	STATE_SYNCING = 1
	STATE_LIVE = 2
)



//this represents the status of a master in a slave
type Master struct {
	State int
	Host string
	Port int
	Conn net.Conn

}

func (m *Master) String() string {

	return fmt.Sprintf("Master(%s:%d)", m.Host, m.Port)
}

// Connect to a master
func (m *Master)Connect() error {

	//make sure we don't connect without disconnecting
	if m.State != STATE_OFFLINE {
		logging.Warning("Could not connect to a connected master!")
		return fmt.Errorf("Trying to connect to an already connected master")
	}

	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", m.Host, m.Port))
	if err != nil {
		logging.Error("Could not connect to master %s: %s", m, err)
		return err
	}
	m.Conn = conn
	//set the state to "pending sync"
	m.State = STATE_SYNCING
	logging.Info("Successfuly connected to master %s", m)
	return nil
}



// Disconnect from a master
func (m *Master)Disconnect() error {


	m.State = STATE_OFFLINE
	err := m.Conn.Close()
	if err != nil {
		logging.Warning("Could not close conection to master: %s", err)
		return err
	}

	logging.Info("Disconnected master %s", m)

	return nil
}

func (m *Master)RunReplication()  {


	reader := bufio.NewReaderSize(m.Conn, 32768)
	writer := bufio.NewWriterSize(m.Conn, 32768)

	redis.SerializeResponse(&db.Command{"SYNC", "", make([][]byte, 0)}, writer)
	writer.Flush()
	mockSession :=  db.DB.NewSession(currentMaster.Conn.LocalAddr())
	mockSession.InChan = nil
	for m.State != STATE_OFFLINE {
		//read an parse the request
		cmd, _ := redis.ReadRequest(reader)
		if cmd.Command != "OK" {

			fmt.Println(cmd.ToString())

			_, _ = db.DB.HandleCommand(cmd, mockSession)
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
			logging.Error("Could not disconnect master: %s", e)
			currentMaster = nil
		}

	}()
	if currentMaster != nil {

		err := currentMaster.Disconnect()
		if err != nil {
			logging.Warning("Could not close conection to master: %s", err)
		}
		currentMaster = nil
		logging.Info("Disconnected master")
	}


}

// Connect to a new master
func connectToMaster(host string, port int) error {


	//check to see if we already have a master
	if currentMaster != nil {
		if currentMaster.Host == host && currentMaster.Port == port {
			logging.Warning("Cannot connect to the same master")
			return fmt.Errorf("Trying to reconnect to the current master %s", currentMaster)
		}

		//disconnect from the current one first
		logging.Info("Disconnecting from current master first...")
		disconnectMaster()

	}


	m := &Master{
		Host: host,
		Port: port,
		State: STATE_OFFLINE,
	}

	err := m.Connect()
	if err == nil {
		logging.Info("Setting new master to %s", m)
		currentMaster = m
	} else {
		return err
	}

	go currentMaster.RunReplication()


	return nil
}

func HandleSLAVEOF(cmd *db.Command, entry *db.Entry, session *db.Session) *db.Result {
	address := cmd.Key
	logging.Info("Got slaveof to %s %s", cmd.Key, cmd.Args[0])

	if strings.ToUpper(cmd.Key) == "NO" && bytes.Equal(bytes.ToUpper(cmd.Args[0]), []byte("ONE")) {
		logging.Info("Disconnecting master")

		disconnectMaster()

		return db.ResultOK()
	}
	port, err := strconv.Atoi(string(cmd.Args[0]))
	//valiate port number
	if err !=nil || port < 1 || port > 65535 {

		return db.NewResult(db.NewPluginError("REPLICATION", "Invalid port number"))

	}


	remote :=    fmt.Sprintf("%s:%d", address, port)
	logging.Info("Connecting to master %s", remote)

	err = connectToMaster(address, port)

	if err != nil {
		logging.Warning("Aborting master connection: %s", err)
		return db.NewResult(db.NewPluginError("REPLICATION", fmt.Sprintf("Could not connect to master: %s", err)))
	}

	return db.ResultOK()
}

