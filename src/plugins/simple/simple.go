/**
 * Created with IntelliJ IDEA.
 * User: daniel
 * Date: 11/15/12
 * Time: 12:58 AM
 * To change this template use File | Settings | File Templates.
 */
package simple

import (
	//	"fmt"
	"db"
//	"io"
	gob "encoding/gob"
	"log"
	"bytes"
)


type SimpleStruct struct {
	Val string
}

func (ht *SimpleStruct)Serialize(g *gob.Encoder) error {

	err := g.Encode(ht)

	return err
}



type SimplePlugin struct {


}

func HandleSET(cmd *db.Command, entry *db.Entry, session *db.Session) *db.Result {

	obj := entry.Value.(*SimpleStruct)
	obj.Val = string(cmd.Args[0])

	return db.NewResult(db.NewStatus("OK"))

}
func HandleGET(cmd *db.Command, entry *db.Entry, session *db.Session) *db.Result {

	if entry != nil {
		obj := entry.Value.(*SimpleStruct)
		r := db.NewResult(obj.Val)
		return r
	}

	return nil

}

func HandleEXISTS(cmd *db.Command, entry *db.Entry, session *db.Session) *db.Result {


	return db.NewResult(entry != nil)
}

func HandlePING(cmd *db.Command, entry *db.Entry, session *db.Session) *db.Result {
	return db.NewResult("PONG")
}

func (p *SimplePlugin)CreateObject() *db.Entry {

	ret := &db.Entry{ Value: &SimpleStruct{},
		Type: db.T_STRING,
	}
	//fmt.Println("Created new hash table ", ret)
	return ret
}

//deserialize and create a db entry
func (p *SimplePlugin)LoadObject(buf []byte, t uint32) *db.Entry {

	if t == db.T_STRING {
		var s SimpleStruct
		buffer := bytes.NewBuffer(buf)
		dec := gob.NewDecoder(buffer)
		err := dec.Decode(&s)
		if err != nil {
			log.Printf("Could not deserialize oject: %s", err)
			return nil
		}

		return &db.Entry{
			Value: &s,
			Type: db.T_STRING,
		}
	} else {
		log.Printf("Could not load value, invalid type %d", t)
	}
	return nil

}


func (p *SimplePlugin)GetCommands() []db.CommandDescriptor {


	return []db.CommandDescriptor {
		db.CommandDescriptor{"SET",1, HandleSET, p, 0, db.CMD_WRITER},
		db.CommandDescriptor{"GET", 0, HandleGET, p, 0, db.CMD_READER},
		db.CommandDescriptor{"PING",0, HandlePING, p, 0, db.CMD_READER},
		db.CommandDescriptor{"EXISTS", 0, HandleEXISTS, p, 0, db.CMD_READER},

	}
}


func (p* SimplePlugin) GetTypes() []uint32 {
	return []uint32{db.T_STRING,}
}
