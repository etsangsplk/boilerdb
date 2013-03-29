package builtin

import (
	gob "encoding/gob"
	"db"

//	"runtime"
	"logging"
	"fmt"
	"time"
	"strconv"
)

const BUILTIN = "BUILTIN"


type BuiltinPlugin struct {}

type  BuiltinDataStruct struct { }

func (p *BuiltinDataStruct)Serialize(g *gob.Encoder) error {
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

				if session.IsRunning {
					session.Send(db.NewResult(fmt.Sprintf("[%d.%d %s] %s", now.Unix(), now.Nanosecond(), session.Addr, cmd.ToString())) )
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
		_ = db.DB.Dump()
		fmt.Println("Finished saving!")
	}()
	return db.NewResult(db.NewStatus("OK"))
}

//perform blocking save
func HandleSAVE(cmd *db.Command, entry *db.Entry, session *db.Session) *db.Result {


	db.DB.Lockdown()
	defer func() { db.DB.UNLockdown() }()

	err := db.DB.Dump()

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




func (p *BuiltinPlugin)LoadObject(buf []byte, t string) *db.Entry {
	return nil
}


func (p *BuiltinPlugin)CreateObject(cmd string) (*db.Entry, string) {
	return nil, ""
}

func (p *BuiltinPlugin)GetManifest() db.PluginManifest {

	return db.PluginManifest {

		Name: BUILTIN,

		Types: make([]string, 0),

		Commands:  []db.CommandDescriptor {
			db.CommandDescriptor{
				CommandName: "INFO",
				MinArgs: 0,	MaxArgs: 0,
				Handler: HandleINFO,
				CommandType: db.CMD_SYSTEM,
			},
			db.CommandDescriptor{
				CommandName: "SAVE",
				MinArgs: 0,	MaxArgs: 0,
				Handler: HandleSAVE,
				CommandType: db.CMD_WRITER,
			},
			db.CommandDescriptor{
				CommandName: "BGSAVE",
				MinArgs: 0,	MaxArgs: 0,
				Handler: HandleBGSAVE,
				CommandType: db.CMD_READER,
			},
			db.CommandDescriptor{
				CommandName: "EXPIRE",
				MinArgs: 1,	MaxArgs: 1,
				Handler: HandleEXPIRE,
				CommandType: db.CMD_WRITER,
			},
			db.CommandDescriptor{
				CommandName: "MONITOR",
				MinArgs: 0,	MaxArgs: 0,
				Handler: HandleMONITOR,
				CommandType: db.CMD_SYSTEM,
			},
//			db.CommandDescriptor{
//				CommandName: "SYNC",
//				MinArgs: 0,	MaxArgs: 0,
//				Handler: HandleSYNC,
//				CommandType: db.CMD_SYSTEM,
//			},
		},
	}
}


 func (p* BuiltinPlugin) String() string {
	 return "BUILTIN"
 }
//init function
func (p* BuiltinPlugin) Init() error {

	return nil
}

//shutdown function
func (p* BuiltinPlugin) Shutdown() { }
