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
//	"net"
	"logging"
//	"strconv"
//	"fmt"
//	"strings"
//	"bytes"
//	"bufio"

)

//This represents the status of a slave in  the master
type Slave struct {
	State int
	Id string
	LastCommandId uint64
	buffer list.List
	Channel chan []byte
}


func HandleSYNC(cmd *db.Command, entry *db.Entry, session *db.Session) *db.Result {


	sink := db.DB.AddSink(
		db.CMD_WRITER,
		session.Id())

	go func() {

		defer func(){
			e := recover()
			if e != nil {
				logging.Info("Could not send command to session outchan: %s", e)
				sink.Close()
			}

		}()

		for session.IsRunning {

			cmd := <- sink.Channel

			if cmd != nil {

				if session.OutChan != nil {
					session.OutChan <- db.NewResult(cmd)
				}
			}

		}
		logging.Info("finishing session for slave %s", session.Id())
		//sink.Close()
	}()
	return db.NewResult(db.NewStatus("OK"))
}

