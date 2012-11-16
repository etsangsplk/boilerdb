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
)


type SimpleStruct struct {
	val string
}

func (ht *SimpleStruct)Serialize(g *gob.Encoder) error {

	err := g.Encode(ht)
	return err
}



type SimplePlugin struct {


}

func HandleSET(cmd *db.Command, entry *db.Entry) *db.Result {

	obj := entry.Value.(*SimpleStruct)
	obj.val = string(cmd.Args[0])

	return db.NewResult(db.NewStatus("OK"))

}
func HandleGET(cmd *db.Command, entry *db.Entry) *db.Result {
	obj := entry.Value.(*SimpleStruct)
	r := db.NewResult(obj.val)
	return r
}

func HandlePING(cmd *db.Command, entry *db.Entry) *db.Result {
	return db.NewResult("PONG")
}

func (p *SimplePlugin)CreateObject() *db.Entry {

	ret := &db.Entry{ Value: &SimpleStruct{},
		Type: db.T_STRING,
	}
	//fmt.Println("Created new hash table ", ret)
	return ret
}

func (p *SimplePlugin)LoadObject(buf []byte, t uint32) *db.Entry {
return nil
}


func (p *SimplePlugin)GetCommands() []db.CommandDescriptor {


	return []db.CommandDescriptor {
		db.CommandDescriptor{"SET", "subkey:string value:string", HandleSET, p, 0, db.CMD_WRITER},
		db.CommandDescriptor{"GET", "subkey:string", HandleGET, p, 0, db.CMD_READER},
		db.CommandDescriptor{"PING", "subkey:string", HandlePING, p, 0, db.CMD_READER},

	}
}


func (p* SimplePlugin) GetTypes() []uint32 {
	return []uint32{db.T_STRING,}
}
