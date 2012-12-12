
package slave

import (
	"db"
	"adapters/redis"
	"net"
	"strconv"
	"fmt"
	"logging"
	"bufio"

)

func HandleSLAEOF(cmd *db.Command, entry *db.Entry, session *db.Session) *db.Result {
	address := cmd.Key
	fmt.Println(address)
	port, err := strconv.Atoi(string(cmd.Args[0]))
	//valiate port number
	if err !=nil || port < 0 || port > 65535 {

		return db.NewResult(db.NewPluginError("SLAVE", "Invalid port number"))

	}

	remote :=    fmt.Sprintf("%s:%d", address, port)
	fmt.Println(remote)
	conn, err := net.Dial("tcp", remote )
	if err != nil {
		logging.Error("Could not connect to master: %s", err)
		return db.NewResult(db.NewPluginError("SLAVE", fmt.Sprintf("Could not connect to master: %s", err)))
	}

	go HandleSlave(conn)

	return db.ResultOK()
}

func HandleSlave(c net.Conn) error {

	var err error = nil


	reader := bufio.NewReaderSize(c, 32768)
	writer := bufio.NewWriterSize(c, 32768)

	redis.SerializeResponse(&db.Command{"SYNC", "", make([][]byte, 0)}, writer)
	writer.Flush()
	mockSession :=  db.DB.NewSession(c.LocalAddr())
	mockSession.InChan = nil
	for {
		//read an parse the request
		cmd, _ := redis.ReadRequest(reader)
		if cmd.Command != "OK" {

			fmt.Println(cmd.ToString())

			_, _ = db.DB.HandleCommand(cmd, mockSession)
		}


	}

	//stop the session
	mockSession.Stop()

	c.Close()
	return err
}


type SlavePlugin struct {
}

func (p *SlavePlugin)CreateObject(commandName string) (*db.Entry, string) {
	return nil, ""
}

//deserialize and create a db entry
func (p *SlavePlugin)LoadObject(buf []byte, typeName string) *db.Entry {
	return nil
}

func (p *SlavePlugin)String() string {
	return "SLAVE"
}

// Get the plugin manifest for the simple plugin
func (p *SlavePlugin)GetManifest() db.PluginManifest {

	return db.PluginManifest {

		Name: "SLAVE",
		Types: []string{},
		Commands:  []db.CommandDescriptor {
			db.CommandDescriptor{
				CommandName: "SLAVEOF",
				MinArgs: 1,	MaxArgs: 1,
				Handler: HandleSLAEOF,
				CommandType: db.CMD_SYSTEM,
			},
		},
	}
}

