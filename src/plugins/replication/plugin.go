// Replication comand registration etc. This is not done yet
package replication

import (
	"db"
)

type ReplicationPlugin struct {
}

func (p *ReplicationPlugin) CreateObject(commandName string) (*db.Entry, string) {
	return nil, ""
}

//deserialize and create a db entry
func (p *ReplicationPlugin) LoadObject(buf []byte, typeName string) *db.Entry {
	return nil
}

func (p *ReplicationPlugin) String() string {
	return "REPLICATION"
}

//init function
func (p *ReplicationPlugin) Init() error {

	go runMasterWatchdogLoop()

	return nil
}

//shutdown function
func (p *ReplicationPlugin) Shutdown() {}

// Get the plugin manifest for the simple plugin
func (p *ReplicationPlugin) GetManifest() db.PluginManifest {

	return db.PluginManifest{

		Name:  "REPLICATION",
		Types: []string{},
		Commands: []db.CommandDescriptor{
			db.CommandDescriptor{
				CommandName: "SLAVEOF",
				MinArgs:     1, MaxArgs: 1,
				Handler:     HandleSLAVEOF,
				CommandType: db.CMD_SYSTEM,
			},
			db.CommandDescriptor{
				CommandName: "SYNC",
				MinArgs:     0, MaxArgs: 0,
				Handler:     HandleSYNC,
				CommandType: db.CMD_SYSTEM,
			},
			db.CommandDescriptor{
				CommandName: "LOAD",
				MinArgs:     3, MaxArgs: 3,
				Handler:     HandleLOAD,
				CommandType: db.CMD_SYSTEM,
			},
		},
	}
}
