/**
 * Created with IntelliJ IDEA.
 * User: dvirsky
 * Date: 11/21/12
 * Time: 12:00 AM
 * To change this template use File | Settings | File Templates.
 */
package system
import (
	gob "encoding/gob"
	"db"
//	"syscall"
)

type SystemPlugin struct {}

type  SystemDataStruct struct { }

func (p *SystemDataStruct)Serialize(g *gob.Encoder) error {
	return nil
}


func (p *SystemPlugin)LoadObject(buf []byte, t uint32) *db.Entry {
	return nil
}


func (p *SystemPlugin)CreateObject() *db.Entry {
	return nil
}




func HandleSAVE(cmd *db.Command, entry *db.Entry) *db.Result {


	db.DB.Lockdown()
	defer func() { db.DB.UNLockdown() }()

	_, err := db.DB.Dump()

	if err == nil {
		return db.NewResult(db.NewStatus("OK"))
	}

	return db.NewResult(db.NewError(db.E_UNKNOWN_ERROR))
}


func (p *SystemPlugin)GetCommands() []db.CommandDescriptor {


	return []db.CommandDescriptor {
		//db.CommandDescriptor{"INFO", 0, HandleINTO, p, 0, db.CMD_READER},
		db.CommandDescriptor{"SAVE", 0, HandleSAVE, p, 0, db.CMD_WRITER},
		//db.CommandDescriptor{"BGSAVE", 0, HandleBGSAVE, p, 0, db.CMD_WRITER},

	}
}


func (p* SystemPlugin) GetTypes() []uint32 {
	return []uint32{}
}
