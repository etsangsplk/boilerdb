package system
import (
	gob "encoding/gob"
	"db"

//	"runtime"
	"logging"
	"fmt"
	"time"
	"strconv"
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

func HandleMONITOR(cmd *db.Command, entry *db.Entry, session *db.Session) *db.Result {


	sink := db.DB.AddSink(
				db.CMD_READER | db.CMD_WRITER | db.CMD_SYSTEM,
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

				now := time.Now()

				if session.OutChan != nil {
					session.OutChan <- db.NewResult(fmt.Sprintf("[%d.%d %s] %s", now.Unix(), now.Nanosecond(), session.Addr, cmd.ToString()))
				}
			}

		}
		//sink.Close()
	}()
	return db.NewResult(db.NewStatus("OK"))
}

// perform BGSAVE - save the DB returning immediately
func HandleBGSAVE(cmd *db.Command, entry *db.Entry, session *db.Session) *db.Result {



	//don't let another BGSAVE run
	if db.DB.BGsaveInProgress.IsSet() {
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
func HandleSAVE(cmd *db.Command, entry *db.Entry, session *db.Session) *db.Result {


	db.DB.Lockdown()
	defer func() { db.DB.UNLockdown() }()

	_, err := db.DB.Dump()

	if err == nil {
		return db.NewResult(db.NewStatus("OK"))
	}

	logging.Info("Error saving db: %s", err)


	return db.NewResult(db.NewError(db.E_UNKNOWN_ERROR))
}

//perform blocking save
func HandleEXPIRE(cmd *db.Command, entry *db.Entry, session *db.Session) *db.Result {


	secs, err := strconv.Atoi(string(cmd.Args[0]))

	//make sure we have a good number of seconds
	if err != nil || secs <= 0 {
		return db.NewResult(db.NewError(db.E_INVALID_PARAMS))

	}


	when := time.Now().Add(time.Duration(secs) * time.Second)

	_ = db.DB.SetExpire(cmd.Key, entry, when)

	return db.NewResult(db.NewStatus("OK"))
}

func HandleINFO(cmd *db.Command, entry *db.Entry, session *db.Session) *db.Result {

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
	logging.Info("Server stats: %s", ret)
	return db.NewResult(ret)


}

func (p *SystemPlugin)GetCommands() []db.CommandDescriptor {


	return []db.CommandDescriptor {
		db.CommandDescriptor{"INFO", 0, HandleINFO, p, 0, db.CMD_SYSTEM},
		db.CommandDescriptor{"SAVE", 0, HandleSAVE, p, 0, db.CMD_WRITER},
		db.CommandDescriptor{"EXPIRE", 1, HandleEXPIRE, p, 0, db.CMD_WRITER},
		db.CommandDescriptor{"BGSAVE", 0, HandleBGSAVE, p, 0, db.CMD_READER},
		db.CommandDescriptor{"MONITOR", 0, HandleMONITOR, p, 0, db.CMD_SYSTEM},


	}
}


func (p* SystemPlugin) GetTypes() []uint32 {
	return []uint32{}
}

 func (p* SystemPlugin) String() string {
	 return "STRING"
 }
