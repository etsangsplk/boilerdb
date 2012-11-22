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

//	"runtime"
	"log"
	"fmt"
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


// perform BGSAVE - save the DB returning immediately
func HandleBGSAVE(cmd *db.Command, entry *db.Entry) *db.Result {



	//don't let another BGSAVE run
	if db.DB.BGsaveInProgress {
		return db.NewResult(db.NewError(db.E_BGSAVE_IN_PROGRESS))
	}

	//perform bgsave in background
	go func() {
		fmt.Printf("Starting bgsave...")
		//now dump the db
		_, _ = db.DB.Dump()
		fmt.Println("Finished saving!")
	}()
	return db.NewResult(db.NewStatus("OK"))
}

//perform blocking save
func HandleSAVE(cmd *db.Command, entry *db.Entry) *db.Result {


	db.DB.Lockdown()
	defer func() { db.DB.UNLockdown() }()

	_, err := db.DB.Dump()

	if err == nil {
		return db.NewResult(db.NewStatus("OK"))
	}

	return db.NewResult(db.NewError(db.E_UNKNOWN_ERROR))
}

func HandleINFO(cmd *db.Command, entry *db.Entry) *db.Result {

	format := `#BoilerDB
version: 0.1

#Plugins
registered_plugins: %d
registered_commands: %d

#Database
active_sessions: %d
total_sessions: %d
total_commands_processed: %d
bgsave_in_progress: %t

#Data
keys: %d

`

	ret := fmt.Sprintf(format,
		db.DB.Stats.RegisteredPlugins,
		db.DB.Stats.RegisteredCommands,
		db.DB.Stats.ActiveSessions,
		db.DB.Stats.TotalSessions,
		db.DB.Stats.CommandsProcessed,
		db.DB.BGsaveInProgress,
		db.DB.Size(),
	)
	log.Printf("Server stats: %s", ret)
	return db.NewResult(ret)


}

func (p *SystemPlugin)GetCommands() []db.CommandDescriptor {


	return []db.CommandDescriptor {
		db.CommandDescriptor{"INFO", 0, HandleINFO, p, 0, db.CMD_READER},
		db.CommandDescriptor{"SAVE", 0, HandleSAVE, p, 0, db.CMD_WRITER},
		db.CommandDescriptor{"BGSAVE", 0, HandleBGSAVE, p, 0, db.CMD_READER},

	}
}


func (p* SystemPlugin) GetTypes() []uint32 {
	return []uint32{}
}
